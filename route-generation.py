"""
route-generation.py
-------------------
Local development script for route optimization using Google OR-Tools.

Usage:
    python route-generation.py

The INPUT variable at the bottom of the file contains the test payload.
Modify it to test different scenarios.

Team types:
    A - volunteers only         → visits Type A properties (onsite water)
    B - hydrant-trained         → visits Type A, B properties
    C - 500-gal truck           → visits Type A, B, C properties + hydrant refills
    D - 2000-gal truck          → visits Type A, B, C properties + hydrant refills

Property types:
    A - onsite water available
    B - hydrant nearby
    C - truck required

Hydrant stops:
    Hydrants have unlimited water (city supply). Truck teams (C/D) start with a
    full tank and visit the nearest hydrant whenever their water would run out.
    Hydrant insertion happens as post-processing after OR-Tools optimizes the
    property visit order — hydrants do not appear as nodes in the VRP.
"""

import argparse
import json
import math
import os
from datetime import datetime, timedelta, timezone
from ortools.constraint_solver import routing_enums_pb2
from ortools.constraint_solver import pywrapcp
import requests

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

TEAM_ELIGIBLE_PROPERTY_TYPES = {
    "A": ["A"],
    "B": ["A", "B"],
    "C": ["A", "B", "C"],
    "D": ["A", "B", "C"],
}

TEAM_WATER_CAPACITY_GALLONS = {
    "A": None,   # no truck
    "B": None,   # no truck
    "C": 500,
    "D": 2000,
}

DEFAULT_PROPERTY_SERVICE_TIME_MINUTES = 10
DEFAULT_PROPERTY_WATER_DEMAND_GALLONS = 500
DEFAULT_PROPERTY_PRIORITY_SCORE = 1000
HYDRANT_REFILL_DURATION_MINUTES = 5

ORS_MATRIX_URL = "https://api.openrouteservice.org/v2/matrix/driving-car"
ORS_MAX_LOCATIONS = 3500  # free tier limit

# Average driving speed used to estimate travel times from haversine distances
AVG_SPEED_KMH = 25

# OR-Tools solver time limit per optimization call
SOLVER_TIME_LIMIT_SECONDS = 120


# ---------------------------------------------------------------------------
# Database loading
# ---------------------------------------------------------------------------

def _load_env_file():
    """Best-effort .env loading for local CLI usage."""
    try:
        from dotenv import load_dotenv
        load_dotenv()
        return
    except ImportError:
        # Fall back to a simple local parser to avoid extra dependencies.
        env_path = os.path.join(os.getcwd(), ".env")
        if not os.path.exists(env_path):
            return

        with open(env_path, "r", encoding="utf-8") as env_file:
            for line in env_file:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip().strip('"').strip("'")
                if key and key not in os.environ:
                    os.environ[key] = value

def _get_supabase_client():
    """
    Creates and returns a Supabase client from environment variables.
    """
    url = os.getenv("SUPABASE_URL") or os.getenv("NEXT_PUBLIC_SUPABASE_URL")
    key = os.getenv("SUPABASE_ANON_KEY") or os.getenv("SUPABASE_KEY") or os.getenv("NEXT_PUBLIC_SUPABASE_ANON_KEY")

    if not url or not key:
        raise RuntimeError(
            "Missing Supabase credentials. Set SUPABASE_URL (or NEXT_PUBLIC_SUPABASE_URL) "
            "and SUPABASE_ANON_KEY/SUPABASE_KEY (or NEXT_PUBLIC_SUPABASE_ANON_KEY)."
        )

    try:
        from supabase import create_client
    except ImportError as exc:
        raise RuntimeError(
            "The supabase package is not installed. Install with: pip install supabase"
        ) from exc

    return create_client(url, key)


def _normalize_property_type(raw_value):
    property_type = (raw_value or "A").strip().upper()
    if property_type in ("A", "B", "C"):
        return property_type
    return "A"


def _fetch_all_rows(client, table_name, columns, batch_size=1000):
    """Fetches all rows from a table via range-based pagination."""
    rows = []
    start = 0
    while True:
        end = start + batch_size - 1
        response = client.table(table_name).select(columns).range(start, end).execute()
        batch = response.data or []
        rows.extend(batch)
        if len(batch) < batch_size:
            break
        start += batch_size
    return rows


def fetch_all_properties(client):
    """Fetches properties not watered in the last 30 days, sorted by priority."""
    properties = []
    cutoff_date = (datetime.now(tz=timezone.utc).date() - timedelta(days=30)).isoformat()
    response = (
        client
        .table("Property")
        .select("id,address,latitude,longitude,property_type,priority_score,num_trees")
        .not_.is_("latitude", "null")
        .not_.is_("longitude", "null")
        .or_(f"prev_watered.is.null,prev_watered.lte.{cutoff_date}")
        .order("priority_score", desc=True, nullsfirst=False)
        .execute()
    )
    rows = response.data or []

    for row in rows:
        property_type = _normalize_property_type(row.get("property_type"))
        priority_score = row.get("priority_score")
        if priority_score is None:
            priority_score = DEFAULT_PROPERTY_PRIORITY_SCORE

        num_trees = row.get("num_trees") or 0
        service_time_minutes = 5 * (num_trees + 1)
        water_demand_gallons = 250 * num_trees if num_trees > 0 else DEFAULT_PROPERTY_WATER_DEMAND_GALLONS

        properties.append({
            "id": str(row.get("id")),
            "address": row.get("address") or "Address unknown",
            "lat": row.get("latitude"),
            "lng": row.get("longitude"),
            "property_type": property_type,
            "service_time_minutes": service_time_minutes,
            "water_demand_gallons": water_demand_gallons,
            "priority_score": priority_score,
        })

    return properties


def select_properties_for_tiered_calls(all_properties, vehicles):
        """
        Selects candidate properties for each tiered call:
            1) x * 15 Type A properties, where x = number of Team A vehicles
            2) y * 15 Type B properties, where y = number of Team B vehicles
            3) z * 15 Type C properties, where z = number of Team C/D vehicles

        Note: Remaining (dropped) properties between calls are handled by
        run_tiered_optimization.
        """
        num_a = sum(1 for v in vehicles if v.get("team_type") == "A")
        num_b = sum(1 for v in vehicles if v.get("team_type") == "B")
        num_cd = sum(1 for v in vehicles if v.get("team_type") in ("C", "D"))

        type_a = [p for p in all_properties if p.get("property_type") == "A"]
        type_b = [p for p in all_properties if p.get("property_type") == "B"]
        type_c = [p for p in all_properties if p.get("property_type") == "C"]

        selected_a = type_a[: num_a * 15]
        selected_b = type_b[: num_b * 15]
        selected_c = type_c[: num_cd * 15]

        selected_properties = selected_a + selected_b + selected_c

        print(
                "Info: Tier pools selected by team counts - "
                f"A: {len(selected_a)}/{num_a * 15}, "
                f"B: {len(selected_b)}/{num_b * 15}, "
                f"C: {len(selected_c)}/{num_cd * 15}."
        )

        return selected_properties


def fetch_all_hydrants(client):
    """Fetches all hydrants from Hydrants table."""
    hydrants = []
    rows = _fetch_all_rows(
        client,
        "Hydrants",
        "id,hydrant_address,latitude,longitude",
        batch_size=1000,
    )

    for row in rows:
        if row.get("latitude") is None or row.get("longitude") is None:
            continue
        hydrants.append({
            "id": str(row.get("id")),
            "address": row.get("hydrant_address") or "Hydrant",
            "lat": row.get("latitude"),
            "lng": row.get("longitude"),
        })

    return hydrants


def load_runtime_data(hub, vehicles):
    """
    Loads runtime properties/hydrants from Supabase.
    No hardcoded fallback is used.
    """
    if not vehicles:
        raise ValueError("At least one vehicle is required.")

    client = _get_supabase_client()
    all_properties = fetch_all_properties(client)
    properties = select_properties_for_tiered_calls(all_properties, vehicles)
    hydrants = fetch_all_hydrants(client)

    if not properties:
        raise RuntimeError(
            "No properties available after applying team-based tier limits. "
            "Check team mix and Property data."
        )
    if not hydrants:
        raise RuntimeError("No hydrants found in Hydrants table with valid latitude/longitude.")

    print(
        f"Info: Loaded {len(all_properties)} eligible properties not watered in the last 30 days; "
        f"selected {len(properties)} for tiered calls, "
        f"and {len(hydrants)} hydrants from Supabase."
    )
    return hub, vehicles, properties, hydrants


def parse_team_spec(team_spec):
    """Parses TEAM_TYPE:TIME_MIN format (e.g., C:600)."""
    if ":" not in team_spec:
        raise argparse.ArgumentTypeError(
            f"Invalid --team '{team_spec}'. Use format TEAM_TYPE:TIME_MIN (example: B:120)."
        )

    team_type_raw, time_raw = team_spec.split(":", 1)
    team_type = team_type_raw.strip().upper()

    if team_type not in TEAM_ELIGIBLE_PROPERTY_TYPES:
        raise argparse.ArgumentTypeError(
            f"Invalid team type '{team_type}'. Must be one of A, B, C, D."
        )

    try:
        time_budget = int(time_raw)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"Invalid time budget in '{team_spec}'. TIME_MIN must be an integer."
        ) from exc

    if time_budget <= 0:
        raise argparse.ArgumentTypeError("TEAM time budget must be > 0 minutes.")

    return team_type, time_budget


def build_vehicles(team_specs):
    vehicles = []
    for idx, (team_type, time_budget) in enumerate(team_specs, start=1):
        vehicles.append({
            "id": f"V-{idx}",
            "team_type": team_type,
            "team_time_budget_minutes": time_budget,
        })
    return vehicles


def parse_args():
    parser = argparse.ArgumentParser(
        description="Generate optimized routes from Supabase properties/hydrants using OR-Tools.",
    )
    parser.add_argument(
        "--hub-lat",
        type=float,
        default=34.18724207952961,
        help="Hub latitude (default: 34.18724)",
    )
    parser.add_argument(
        "--hub-lng",
        type=float,
        default=-118.1500512948987,
        help="Hub longitude (default: -118.15005)",
    )
    parser.add_argument(
        "--num-teams",
        type=int,
        required=True,
        help="Number of teams/vehicles (must match count of --team flags)",
    )
    parser.add_argument(
        "--team",
        action="append",
        type=parse_team_spec,
        required=True,
        help="Team spec in TEAM_TYPE:TIME_MIN format. Repeat once per team. Example: --team A:90 --team C:600",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help=(
            "Optional output file path for JSON results. "
            "Default: route-results-<timestamp>.json in the current directory"
        ),
    )

    args = parser.parse_args()

    if args.num_teams <= 0:
        parser.error("--num-teams must be greater than 0.")
    if len(args.team) != args.num_teams:
        parser.error("--num-teams must match the number of --team entries.")

    return args


def write_results_to_file(result, output_path, now):
    """Writes route results JSON to disk and returns the final path."""
    final_path = output_path or f"route-results-{now.strftime('%Y%m%dT%H%M%SZ')}.json"
    output_dir = os.path.dirname(final_path)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    with open(final_path, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2)

    return final_path


# ---------------------------------------------------------------------------
# Haversine distance
# ---------------------------------------------------------------------------

def haversine_km(lat1, lng1, lat2, lng2):
    """Returns great-circle distance in kilometres between two coordinates."""
    R = 6371.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lng2 - lng1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return R * 2 * math.asin(math.sqrt(a))


def travel_time_minutes(lat1, lng1, lat2, lng2):
    """Estimates driving time in minutes using haversine distance and AVG_SPEED_KMH."""
    dist_km = haversine_km(lat1, lng1, lat2, lng2)
    return (dist_km / AVG_SPEED_KMH) * 60


def nearest_hydrant(lat, lng, hydrants):
    """Returns the hydrant closest to the given coordinates."""
    return min(hydrants, key=lambda h: haversine_km(lat, lng, h["lat"], h["lng"]))


# ---------------------------------------------------------------------------
# Google Maps URL
# ---------------------------------------------------------------------------

def generate_maps_url(stops, hub):
    if not stops:
        return None
    waypoints = "|".join(f"{s['lat']},{s['lng']}" for s in stops)
    return (
        f"https://www.google.com/maps/dir/?api=1&"
        f"origin={hub['lat']},{hub['lng']}&"
        f"destination={hub['lat']},{hub['lng']}&"
        f"waypoints={waypoints}&"
        f"travelmode=driving"
    )


# ---------------------------------------------------------------------------
# Hydrant post-processing
# ---------------------------------------------------------------------------

def insert_hydrant_stops(property_stops, vehicle, hydrants, hub, now):
    """
    Takes an ordered list of property stops for a truck vehicle and inserts
    hydrant refill stops wherever the truck would run out of water.

    The truck starts empty at the hub. Whenever the next property
    would exceed the remaining water, we insert a stop at the nearest hydrant
    (relative to the current position) before continuing.

    Also recomputes arrival times for every stop, accounting for travel time
    to any inserted hydrant stops.

    Returns a list of stops (mix of property and hydrant_refill dicts).
    """
    tank_capacity = TEAM_WATER_CAPACITY_GALLONS.get(vehicle.get("team_type"))

    # Non-truck vehicles: return property stops unchanged with arrival times
    if tank_capacity is None or not hydrants:
        return _compute_arrival_times(property_stops, hub, now)

    # Truck teams start empty and must visit a hydrant before first watering.
    water_remaining = 0
    result = []
    current_lat = hub["lat"]
    current_lng = hub["lng"]
    current_time = 0.0  # minutes since start

    for prop_stop in property_stops:
        demand = prop_stop.get("water_demand_gallons",
                               DEFAULT_PROPERTY_WATER_DEMAND_GALLONS)

        # If this property would exceed remaining water, refill first
        if demand > water_remaining:
            h = nearest_hydrant(current_lat, current_lng, hydrants)
            travel_to_hydrant = travel_time_minutes(
                current_lat, current_lng, h["lat"], h["lng"]
            )
            current_time += travel_to_hydrant
            arrival_dt = datetime.fromtimestamp(
                now.timestamp() + current_time * 60, tz=timezone.utc
            )
            # Refill duration depends on truck type: 2 min for C, 5 min for D
            refill_duration = 2 if vehicle.get("team_type") == "C" else 5
            result.append({
                "type": "hydrant_refill",
                "hydrant_id": h["id"],
                "address": h.get("address", "Hydrant"),
                "lat": h["lat"],
                "lng": h["lng"],
                "duration_min": refill_duration,
                "arrival_time": arrival_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
            })
            current_time += refill_duration
            water_remaining = tank_capacity  # full tank
            current_lat, current_lng = h["lat"], h["lng"]

        # Travel to the property
        travel_to_prop = travel_time_minutes(
            current_lat, current_lng, prop_stop["lat"], prop_stop["lng"]
        )
        current_time += travel_to_prop
        arrival_dt = datetime.fromtimestamp(
            now.timestamp() + current_time * 60, tz=timezone.utc
        )
        result.append({
            **prop_stop,
            "arrival_time": arrival_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
        })
        current_time += prop_stop["service_time_min"]
        water_remaining -= demand
        current_lat, current_lng = prop_stop["lat"], prop_stop["lng"]

    return result


def _compute_arrival_times(property_stops, hub, now):
    """
    Recomputes arrival times for a list of property stops based on
    travel time from hub → stop 1 → stop 2 → ... using haversine.
    Used for non-truck vehicles where no hydrant insertion is needed.
    """
    result = []
    current_lat = hub["lat"]
    current_lng = hub["lng"]
    current_time = 0.0

    for stop in property_stops:
        travel = travel_time_minutes(
            current_lat, current_lng, stop["lat"], stop["lng"]
        )
        current_time += travel
        arrival_dt = datetime.fromtimestamp(
            now.timestamp() + current_time * 60, tz=timezone.utc
        )
        result.append({
            **stop,
            "arrival_time": arrival_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
        })
        current_time += stop["service_time_min"]
        current_lat, current_lng = stop["lat"], stop["lng"]

    return result


# ---------------------------------------------------------------------------
# Node list and time matrix (properties only — no hydrant nodes)
# ---------------------------------------------------------------------------

def build_node_list(hub, properties):
    """
    Builds the flat list of nodes for OR-Tools: depot (hub) + properties only.
    Hydrants are no longer VRP nodes — they are inserted in post-processing.
    """
    nodes = []
    nodes.append({
        "type": "hub",
        "lat": hub["lat"],
        "lng": hub["lng"],
        "service_time_minutes": 0,
    })
    for p in properties:
        nodes.append({
            "type": "property",
            "id": p["id"],
            "lat": p["lat"],
            "lng": p["lng"],
            "address": p.get("address", "Address unknown"),
            "property_type": p.get("property_type", "A"),
            "service_time_minutes": p.get("service_time_minutes", DEFAULT_PROPERTY_SERVICE_TIME_MINUTES),
            "water_demand_gallons": p.get("water_demand_gallons", DEFAULT_PROPERTY_WATER_DEMAND_GALLONS),
            "priority_score": p.get("priority_score", DEFAULT_PROPERTY_PRIORITY_SCORE),
        })
    return nodes

def _ors_matrix_single(nodes):
    """Single ORS matrix request for small node counts (≤ ORS_CHUNK_SIZE)."""
    ors_api_key = os.getenv("ORS_API_KEY")
    coords = [[n["lng"], n["lat"]] for n in nodes]
    try:
        response = requests.post(
            ORS_MATRIX_URL,
            json={
                "locations": coords,
                "metrics": ["duration"],
                "units": "m",
            },
            headers={
                "Authorization": ors_api_key,
                "Content-Type": "application/json",
            },
            timeout=60,
        )
        response.raise_for_status()
        raw = response.json()["durations"]
        matrix = []
        for i, row in enumerate(raw):
            matrix_row = []
            for j, cell in enumerate(row):
                if cell is not None:
                    matrix_row.append(max(1, round(cell / 60)) if i != j else 0)
                else:
                    matrix_row.append(int(round(travel_time_minutes(
                        nodes[i]["lat"], nodes[i]["lng"],
                        nodes[j]["lat"], nodes[j]["lng"],
                    ))))
            matrix.append(matrix_row)
        print(f"Info: ORS matrix built for {len(nodes)} nodes.")
        return matrix
    except requests.exceptions.RequestException as e:
        try:
            print(f"ORS error response: {e.response.json()}")
        except Exception:
            pass
        print(f"Warning: ORS request failed ({e}), falling back to haversine.")
        return build_time_matrix_haversine(nodes)

ORS_CHUNK_SIZE = 58  # keeps any chunk under 3500 cells (58*58 = 3364 worst case)

def build_time_matrix_ors(nodes):
    n = len(nodes)

    ors_api_key = os.getenv("ORS_API_KEY")

    if not ors_api_key:
        print("Warning: ors_api_key not set, falling back to haversine.")
        return build_time_matrix_haversine(nodes)

    if n <= ORS_CHUNK_SIZE:
        return _ors_matrix_single(nodes)

    print(f"Info: {n} nodes requires chunked ORS matrix ({n}x{n}={n*n} cells).")
    matrix = [[0] * n for _ in range(n)]
    all_coords = [[nd["lng"], nd["lat"]] for nd in nodes]

    for i_start in range(0, n, ORS_CHUNK_SIZE):
        i_end = min(i_start + ORS_CHUNK_SIZE, n)
        for j_start in range(0, n, ORS_CHUNK_SIZE):
            j_end = min(j_start + ORS_CHUNK_SIZE, n)

            src_idx = list(range(i_start, i_end))
            tgt_idx = list(range(j_start, j_end))

            # Only send the coords ORS needs, remapped to 0-based indices
            needed_idx = sorted(set(src_idx + tgt_idx))
            coord_remap = {orig: new for new, orig in enumerate(needed_idx)}
            chunk_coords = [all_coords[k] for k in needed_idx]
            chunk_src = [coord_remap[k] for k in src_idx]
            chunk_tgt = [coord_remap[k] for k in tgt_idx]

            try:
                response = requests.post(
                    ORS_MATRIX_URL,
                    json={
                        "locations": chunk_coords,
                        "sources": chunk_src,
                        "destinations": chunk_tgt,
                        "metrics": ["duration"],
                    },
                    headers={
                        "Authorization": ors_api_key,
                        "Content-Type": "application/json",
                    },
                    timeout=60,
                )
                response.raise_for_status()
                raw = response.json()["durations"]

                for i_local, row in enumerate(raw):
                    for j_local, cell in enumerate(row):
                        i_global = i_start + i_local
                        j_global = j_start + j_local
                        if cell is not None:
                            matrix[i_global][j_global] = max(1, round(cell / 60)) if i_global != j_global else 0
                        else:
                            matrix[i_global][j_global] = int(round(travel_time_minutes(
                                nodes[i_global]["lat"], nodes[i_global]["lng"],
                                nodes[j_global]["lat"], nodes[j_global]["lng"],
                            )))

            except requests.exceptions.RequestException as e:
                try:
                    print(f"ORS chunk error: {e.response.json()}")
                except Exception:
                    pass
                print(f"Warning: ORS chunk [{i_start}:{i_end}, {j_start}:{j_end}] failed, using haversine.")
                for i_local in range(i_end - i_start):
                    for j_local in range(j_end - j_start):
                        i_global = i_start + i_local
                        j_global = j_start + j_local
                        matrix[i_global][j_global] = int(round(travel_time_minutes(
                            nodes[i_global]["lat"], nodes[i_global]["lng"],
                            nodes[j_global]["lat"], nodes[j_global]["lng"],
                        )))

    print(f"Info: ORS chunked matrix built for {n} nodes.")
    return matrix


def build_time_matrix_haversine(nodes):
    """Original haversine-based matrix, kept as fallback."""
    n = len(nodes)
    matrix = []
    for i in range(n):
        row = []
        for j in range(n):
            if i == j:
                row.append(0)
            else:
                t = travel_time_minutes(
                    nodes[i]["lat"], nodes[i]["lng"],
                    nodes[j]["lat"], nodes[j]["lng"],
                )
                row.append(int(round(t)))
        matrix.append(row)
    return matrix



# ---------------------------------------------------------------------------
# Single optimization call
# ---------------------------------------------------------------------------

def optimize_vehicles(hub, properties, vehicles, hydrants, now):
    """
    Runs OR-Tools VRP for the given set of vehicles and properties.
    Hydrant stops are inserted into the final routes as post-processing.

    Returns:
        routes  - list of route dicts (vehicle_id, stops, totals, maps_url)
        dropped - list of {property_id, reason}
    """
    if not properties or not vehicles:
        return {
            "routes": [],
            "dropped": [{"property_id": p["id"], "reason": "No eligible vehicle"} for p in properties],
        }

    nodes = build_node_list(hub, properties)
    time_matrix = build_time_matrix_ors(nodes)

    num_nodes = len(nodes)
    num_vehicles = len(vehicles)
    depot = 0

    manager = pywrapcp.RoutingIndexManager(num_nodes, num_vehicles, depot)
    routing = pywrapcp.RoutingModel(manager)

    # ------------------------------------------------------------------
    # Travel time callback (travel + service time at the from-node)
    # ------------------------------------------------------------------
    
    def travel_callback(from_index, to_index):
        from_node = manager.IndexToNode(from_index)
        to_node = manager.IndexToNode(to_index)
        return time_matrix[from_node][to_node]

    travel_callback_index = routing.RegisterTransitCallback(travel_callback)
    routing.SetArcCostEvaluatorOfAllVehicles(travel_callback_index)
    
    def time_callback(from_index, to_index):
        from_node = manager.IndexToNode(from_index)
        to_node = manager.IndexToNode(to_index)
        return time_matrix[from_node][to_node] + int(nodes[from_node]["service_time_minutes"])

    transit_callback_index = routing.RegisterTransitCallback(time_callback)

    # ------------------------------------------------------------------
    # Time dimension — enforces per-vehicle time budget
    # ------------------------------------------------------------------
    max_budget = max(v["team_time_budget_minutes"] for v in vehicles)
    routing.AddDimension(
        transit_callback_index,
        0,           # no waiting time slack
        max_budget,  # global upper bound (tightened per-vehicle below)
        True,        # start cumul at zero
        "Time",
    )
    time_dimension = routing.GetDimensionOrDie("Time")

    for v_idx, vehicle in enumerate(vehicles):
        time_dimension.CumulVar(routing.End(v_idx)).SetMax(
            vehicle["team_time_budget_minutes"]
        )

    # ------------------------------------------------------------------
    # Property disjunctions — each stop is optional with a skip penalty
    # ------------------------------------------------------------------
    prop_start_node = 1  # node 0 is depot
    prop_end_node = prop_start_node + len(properties) - 1

    for node_idx in range(prop_start_node, prop_end_node + 1):
        raw_penalty = nodes[node_idx].get("priority_score", DEFAULT_PROPERTY_PRIORITY_SCORE)
        penalty = int(round(raw_penalty)) if raw_penalty is not None else DEFAULT_PROPERTY_PRIORITY_SCORE
        if penalty < 0:
            penalty = 0
        routing.AddDisjunction([manager.NodeToIndex(node_idx)], penalty)

    # ------------------------------------------------------------------
    # Vehicle-to-property eligibility via VehicleVar
    # ------------------------------------------------------------------
    for node_idx in range(prop_start_node, prop_end_node + 1):
        prop_type = nodes[node_idx].get("property_type", "A")
        allowed = [
            int(v_idx) for v_idx, v in enumerate(vehicles)
            if prop_type in TEAM_ELIGIBLE_PROPERTY_TYPES.get(v.get("team_type", "A"), [])
        ]
        if len(allowed) < num_vehicles:
            routing.VehicleVar(manager.NodeToIndex(node_idx)).SetValues(allowed)

    # ------------------------------------------------------------------
    # Solver parameters
    # ------------------------------------------------------------------
    search_params = pywrapcp.DefaultRoutingSearchParameters()
    search_params.first_solution_strategy = (
        routing_enums_pb2.FirstSolutionStrategy.PATH_CHEAPEST_ARC
    )
    search_params.local_search_metaheuristic = (
        routing_enums_pb2.LocalSearchMetaheuristic.GUIDED_LOCAL_SEARCH
    )
    search_params.guided_local_search_lambda_coefficient = 1.0
    search_params.time_limit.seconds = SOLVER_TIME_LIMIT_SECONDS

    # ------------------------------------------------------------------
    # Solve
    # ------------------------------------------------------------------
    solution = routing.SolveWithParameters(search_params)

    if not solution:
        return {
            "routes": [],
            "dropped": [{"property_id": p["id"], "reason": "No solution found"} for p in properties],
        }

    # ------------------------------------------------------------------
    # Extract routes and insert hydrant stops
    # ------------------------------------------------------------------
    routes = []
    visited_property_ids = set()

    for v_idx, vehicle in enumerate(vehicles):
        # Collect ordered property stops from the solution (no arrival times yet)
        raw_property_stops = []
        index = routing.Start(v_idx)

        while not routing.IsEnd(index):
            node_idx = manager.IndexToNode(index)
            node = nodes[node_idx]

            if node["type"] == "property":
                raw_property_stops.append({
                    "type": "property",
                    "property_id": node["id"],
                    "address": node["address"],
                    "lat": node["lat"],
                    "lng": node["lng"],
                    "service_time_min": node["service_time_minutes"],
                    "water_demand_gallons": node.get("water_demand_gallons", DEFAULT_PROPERTY_WATER_DEMAND_GALLONS),
                    "arrival_time": None,  # filled in below
                })
                visited_property_ids.add(node["id"])

            index = solution.Value(routing.NextVar(index))

        if not raw_property_stops:
            continue

        # Insert hydrant stops and compute all arrival times
        stops = insert_hydrant_stops(raw_property_stops, vehicle, hydrants, hub, now)

        property_stops = [s for s in stops if s["type"] == "property"]
        hydrant_stops = [s for s in stops if s["type"] == "hydrant_refill"]

        total_service_min = sum(s["service_time_min"] for s in property_stops)
        total_refill_min = sum(s["duration_min"] for s in hydrant_stops)

        # Travel time = total route time minus all service and refill time
        route_end_time = solution.Min(time_dimension.CumulVar(routing.End(v_idx)))
        total_travel_min = max(0, route_end_time - total_service_min)

        maps_stops = [{"lat": s["lat"], "lng": s["lng"]} for s in stops]

        routes.append({
            "vehicle_id": vehicle["id"],
            "stops": stops,
            "totals": {
                "travel_min": total_travel_min,
                "service_min": total_service_min,
                "refill_min": total_refill_min,
                "total_min": total_travel_min + total_service_min + total_refill_min,
            },
            "maps_url": generate_maps_url(maps_stops, hub),
        })

    dropped = [
        {"property_id": p["id"], "reason": "Could not fit in time budget"}
        for p in properties
        if p["id"] not in visited_property_ids
    ]

    return {"routes": routes, "dropped": dropped}


# ---------------------------------------------------------------------------
# Tiered optimization (A → B → C/D)
# ---------------------------------------------------------------------------

def run_tiered_optimization(hub, all_properties, all_vehicles, hydrants, now):
    """
    Three-pass tiered approach:

    Pass 1: Team A vehicles  + Type A properties
    Pass 2: Team B vehicles  + Type B properties + Pass 1 drops
    Pass 3: Team C/D vehicles + Type C properties + Pass 2 drops
    """
    property_by_id = {p["id"]: p for p in all_properties}

    type_a_props = [p for p in all_properties if p.get("property_type") == "A"]
    type_b_props = [p for p in all_properties if p.get("property_type") == "B"]
    type_c_props = [p for p in all_properties if p.get("property_type") == "C"]

    team_a  = [v for v in all_vehicles if v.get("team_type") == "A"]
    team_b  = [v for v in all_vehicles if v.get("team_type") == "B"]
    team_cd = [v for v in all_vehicles if v.get("team_type") in ("C", "D")]

    all_routes = []
    final_dropped = []

    # Pass 1: Team A + Type A
    if team_a and type_a_props:
        print(f"Pass 1: {len(team_a)} Team A vehicle(s), {len(type_a_props)} Type A properties")
        result1 = optimize_vehicles(hub, type_a_props, team_a, [], now)
        all_routes.extend(result1["routes"])
        pass1_dropped_ids = [d["property_id"] for d in result1["dropped"]]
    else:
        print("Pass 1 skipped — no Team A vehicles or Type A properties")
        pass1_dropped_ids = [p["id"] for p in type_a_props]

    # Pass 2: Team B + Type B + Pass 1 drops
    pass2_props = type_b_props + [property_by_id[pid] for pid in pass1_dropped_ids]
    if team_b and pass2_props:
        print(f"Pass 2: {len(team_b)} Team B vehicle(s), {len(pass2_props)} properties "
              f"({len(type_b_props)} Type B + {len(pass1_dropped_ids)} from Pass 1)")
        result2 = optimize_vehicles(hub, pass2_props, team_b, [], now)
        all_routes.extend(result2["routes"])
        pass2_dropped_ids = [d["property_id"] for d in result2["dropped"]]
    else:
        print("Pass 2 skipped — no Team B vehicles or properties")
        pass2_dropped_ids = [p["id"] for p in pass2_props]

    # Pass 3: Team C/D + Type C + Pass 2 drops
    pass3_props = type_c_props + [property_by_id[pid] for pid in pass2_dropped_ids]
    if team_cd and pass3_props:
        print(f"Pass 3: {len(team_cd)} Team C/D vehicle(s), {len(pass3_props)} properties "
              f"({len(type_c_props)} Type C + {len(pass2_dropped_ids)} from Pass 2)")
        result3 = optimize_vehicles(hub, pass3_props, team_cd, hydrants, now)
        all_routes.extend(result3["routes"])
        final_dropped = result3["dropped"]
    else:
        print("Pass 3 skipped — no Team C/D vehicles or properties")
        final_dropped = [
            {"property_id": p["id"], "reason": "Could not fit in time budget"}
            for p in pass3_props
        ]

    return {
        "routes": all_routes,
        "dropped": final_dropped,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    _load_env_file()
    args = parse_args()
    now = datetime.now(tz=timezone.utc)

    hub = {"lat": args.hub_lat, "lng": args.hub_lng}
    vehicles = build_vehicles(args.team)
    hub, vehicles, properties, hydrants = load_runtime_data(hub, vehicles)

    print(f"\n{'='*60}")
    print(f"Route Optimization — {now.strftime('%Y-%m-%dT%H:%M:%SZ')}")
    print(f"Vehicles: {len(vehicles)} | Properties: {len(properties)} | Hydrants: {len(hydrants)}")
    print(f"{'='*60}\n")

    result = run_tiered_optimization(hub, properties, vehicles, hydrants, now)

    print(f"\n{'='*60}")
    print("RESULT")
    print(f"{'='*60}")
    print(json.dumps(result, indent=2))

    output_file = write_results_to_file(result, args.output, now)
    print(f"\nWrote result JSON to: {output_file}")

    print(f"\nSummary: {len(result['routes'])} route(s), {len(result['dropped'])} dropped property/properties")
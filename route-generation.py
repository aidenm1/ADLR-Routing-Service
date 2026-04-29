"""
route-generation.py
-------------------
Local development script for route optimization using Google OR-Tools.

Usage:
    python route-generation.py --num-teams 3 --team A:90 --team B:120 --team C:600

The script runs three separate OR-Tools VRP calls (tiered):
    Pass 1: Team A vehicles  + Type A properties only
    Pass 2: Team B vehicles  + Type B properties + Pass 1 drops
    Pass 3: Team C/D vehicles + Type C properties + Pass 2 drops

Team types:
    A - volunteers only         → visits Type A properties (onsite water)
    B - hydrant-trained         → visits Type B properties (+ Pass 1 drops)
    C - 500-gal truck           → visits Type C properties (+ Pass 2 drops) + hydrant refills
    D - 2000-gal truck          → visits Type C properties (+ Pass 2 drops) + hydrant refills

Property types:
    A - onsite water available
    B - hydrant nearby
    C - truck required

Hydrant stops:
    Hydrants have unlimited water (city supply). Truck teams (C/D) start with an empty
    tank and visit the nearest hydrant whenever their water would run out.
    Hydrant insertion happens as post-processing after OR-Tools optimises the
    property visit order — hydrants do not appear as nodes in the VRP to minimize size of travel time matrix.
"""

import argparse
import json
import math
import os
from datetime import datetime, timedelta, timezone

from ortools.constraint_solver import pywrapcp, routing_enums_pb2
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

TEAM_WATER_CAPACITY_GALLONS = {
    "A": None,   # no truck
    "B": None,   # no truck
    "C": 500,
    "D": 2000,
}

DEFAULT_PROPERTY_SERVICE_TIME_MINUTES = 10
DEFAULT_PROPERTY_WATER_DEMAND_GALLONS = 500

HYDRANT_REFILL_DURATION_MINUTES = {
    "C": 2,
    "D": 5,
}

ORS_MATRIX_URL = "https://api.openrouteservice.org/v2/matrix/driving-car"
ORS_CHUNK_SIZE = 58   # keeps any chunk under 3 500 cells (58×58 = 3 364)

# Fallback average driving speed when ORS is unavailable
AVG_SPEED_KMH = 25

# OR-Tools solver time limit per optimisation call
SOLVER_TIME_LIMIT_SECONDS = 120

# Num workers to build time matrix
MAX_WORKERS = 8


# ---------------------------------------------------------------------------
# Environment / Supabase helpers
# ---------------------------------------------------------------------------

def _load_env_file():
    """Best-effort .env loading for local CLI usage."""
    try:
        from dotenv import load_dotenv
        load_dotenv()
        return
    except ImportError:
        pass

    env_path = os.path.join(os.getcwd(), ".env")
    if not os.path.exists(env_path):
        return

    with open(env_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = value


def _get_supabase_client():
    url = os.getenv("SUPABASE_URL") or os.getenv("NEXT_PUBLIC_SUPABASE_URL")
    key = (
        os.getenv("SUPABASE_ANON_KEY")
        or os.getenv("SUPABASE_KEY")
        or os.getenv("NEXT_PUBLIC_SUPABASE_ANON_KEY")
    )
    if not url or not key:
        raise RuntimeError(
            "Missing Supabase credentials. Set SUPABASE_URL and "
            "SUPABASE_ANON_KEY (or their NEXT_PUBLIC_ variants)."
        )
    try:
        from supabase import create_client
    except ImportError as exc:
        raise RuntimeError(
            "supabase package not installed. Run: pip install supabase"
        ) from exc
    return create_client(url, key)


def _normalize_property_type(raw):
    v = (raw or "A").strip().upper()
    return v if v in ("A", "B", "C") else "A"


def _fetch_all_rows(client, table, columns, batch_size=1000):
    rows, start = [], 0
    while True:
        batch = (
            client.table(table)
            .select(columns)
            .range(start, start + batch_size - 1)
            .execute()
            .data or []
        )
        rows.extend(batch)
        if len(batch) < batch_size:
            break
        start += batch_size
    return rows


def fetch_all_properties(client):
    cutoff = (datetime.now(tz=timezone.utc).date() - timedelta(days=30)).isoformat()
    rows = (
        client.table("Property")
        .select("id,address,latitude,longitude,property_type,priority_score,num_trees")
        .not_.is_("latitude", "null")
        .not_.is_("longitude", "null")
        .or_(f"prev_watered.is.null,prev_watered.lte.{cutoff}")
        .order("priority_score", desc=True, nullsfirst=False)
        .execute()
        .data or []
    )

    properties = []
    for row in rows:
        num_trees = row.get("num_trees") or 0
        properties.append({
            "id": str(row["id"]),
            "address": row.get("address") or "Address unknown",
            "lat": row["latitude"],
            "lng": row["longitude"],
            "property_type": _normalize_property_type(row.get("property_type")),
            "service_time_minutes": 5 * (num_trees + 1),
            "water_demand_gallons": (
                250 * num_trees if num_trees > 0 else DEFAULT_PROPERTY_WATER_DEMAND_GALLONS
            ),
            "priority_score": row.get("priority_score") or 100,
        })
    return properties


def fetch_all_hydrants(client):
    hydrants = []
    for row in _fetch_all_rows(client, "Hydrants", "id,hydrant_address,latitude,longitude"):
        if row.get("latitude") is None or row.get("longitude") is None:
            continue
        hydrants.append({
            "id": str(row["id"]),
            "address": row.get("hydrant_address") or "Hydrant",
            "lat": row["latitude"],
            "lng": row["longitude"],
        })
    return hydrants


def load_runtime_data(hub, vehicles):
    if not vehicles:
        raise ValueError("At least one vehicle is required.")
    client = _get_supabase_client()
    properties = fetch_all_properties(client)
    hydrants = fetch_all_hydrants(client)
    if not properties:
        raise RuntimeError("No eligible properties found in Supabase.")
    if not hydrants:
        raise RuntimeError("No hydrants with valid coordinates found in Supabase.")
    print(
        f"Info: Loaded {len(properties)} eligible properties "
        f"and {len(hydrants)} hydrants from Supabase."
    )
    return hub, vehicles, properties, hydrants


# ---------------------------------------------------------------------------
# CLI argument parsing
# ---------------------------------------------------------------------------

def parse_team_spec(spec):
    if ":" not in spec:
        raise argparse.ArgumentTypeError(
            f"Invalid --team '{spec}'. Use TEAM_TYPE:TIME_MIN, e.g. B:120."
        )
    team_type_raw, time_raw = spec.split(":", 1)
    team_type = team_type_raw.strip().upper()
    if team_type not in TEAM_WATER_CAPACITY_GALLONS:
        raise argparse.ArgumentTypeError(
            f"Invalid team type '{team_type}'. Must be A, B, C, or D."
        )
    try:
        time_budget = int(time_raw)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"TIME_MIN must be an integer in '{spec}'."
        ) from exc
    if time_budget <= 0:
        raise argparse.ArgumentTypeError("TIME_MIN must be > 0.")
    return team_type, time_budget


def build_vehicles(team_specs):
    return [
        {
            "id": f"V-{idx}",
            "team_type": team_type,
            "team_time_budget_minutes": time_budget,
        }
        for idx, (team_type, time_budget) in enumerate(team_specs, start=1)
    ]


def parse_args():
    parser = argparse.ArgumentParser(
        description="Generate optimised routes from Supabase data using OR-Tools."
    )
    parser.add_argument("--hub-lat", type=float, default=34.18724207952961)
    parser.add_argument("--hub-lng", type=float, default=-118.1500512948987)
    parser.add_argument("--num-teams", type=int, required=True)
    parser.add_argument(
        "--team",
        action="append",
        type=parse_team_spec,
        required=True,
        help="TEAM_TYPE:TIME_MIN, e.g. --team A:90 --team C:600",
    )
    parser.add_argument("--output", type=str, default=None)

    args = parser.parse_args()
    if args.num_teams <= 0:
        parser.error("--num-teams must be > 0.")
    if len(args.team) != args.num_teams:
        parser.error("--num-teams must equal the number of --team entries.")
    return args


def write_results(result, output_path, now):
    path = output_path or f"route-results-{now.strftime('%Y%m%dT%H%M%SZ')}.json"
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2)
    return path


# ---------------------------------------------------------------------------
# Geometry helpers
# ---------------------------------------------------------------------------

def haversine_km(lat1, lng1, lat2, lng2):
    R = 6371.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lng2 - lng1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return R * 2 * math.asin(math.sqrt(a))


def travel_time_minutes(lat1, lng1, lat2, lng2):
    return (haversine_km(lat1, lng1, lat2, lng2) / AVG_SPEED_KMH) * 60


def nearest_hydrant(lat, lng, hydrants):
    return min(hydrants, key=lambda h: haversine_km(lat, lng, h["lat"], h["lng"]))


# ---------------------------------------------------------------------------
# Google Maps URL
# ---------------------------------------------------------------------------

def generate_maps_url(stops, hub=None):
    if not stops and not hub:
        return None

    def fmt(p):
        return f"{p['lat']},{p['lng']}" if isinstance(p, dict) else f"{p[0]},{p[1]}"

    base = "https://www.google.com/maps/dir/"

    if hub:
        hub_str = fmt(hub)
        path = "/".join([hub_str] + [fmt(s) for s in stops] + [hub_str])
    else:
        coords = [fmt(s) for s in stops]
        path = "/".join(coords)

    return base + path


# ---------------------------------------------------------------------------
# Travel time matrix (ORS with haversine fallback)
# ---------------------------------------------------------------------------

def build_time_matrix_haversine(nodes):
    """Full N×N haversine matrix in seconds."""
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
                row.append(max(1, int(round(t * 60))))
        matrix.append(row)
    return matrix


def _ors_chunk(coords, src_idx, tgt_idx, all_nodes):
    """POST one ORS matrix chunk; returns a dict {(i,j): seconds}."""
    ors_api_key = os.getenv("ORS_API_KEY", "")
    needed = sorted(set(src_idx + tgt_idx))
    remap = {orig: new for new, orig in enumerate(needed)}
    chunk_coords = [coords[k] for k in needed]
    chunk_src = [remap[k] for k in src_idx]
    chunk_tgt = [remap[k] for k in tgt_idx]

    try:
        resp = requests.post(
            ORS_MATRIX_URL,
            json={
                "locations": chunk_coords,
                "sources": chunk_src,
                "destinations": chunk_tgt,
                "metrics": ["duration"],
            },
            headers={"Authorization": ors_api_key, "Content-Type": "application/json"},
            timeout=60,
        )
        resp.raise_for_status()
        raw = resp.json()["durations"]
        result = {}
        for i_local, row in enumerate(raw):
            for j_local, cell in enumerate(row):
                i_global = src_idx[i_local]
                j_global = tgt_idx[j_local]
                if cell is not None:
                    result[(i_global, j_global)] = (
                        0 if i_global == j_global else max(1, int(round(cell)))
                    )
                else:
                    # ORS returned null — fall back to haversine for this pair
                    t = travel_time_minutes(
                        all_nodes[i_global]["lat"], all_nodes[i_global]["lng"],
                        all_nodes[j_global]["lat"], all_nodes[j_global]["lng"],
                    )
                    result[(i_global, j_global)] = (
                        0 if i_global == j_global else max(1, int(round(t * 60)))
                    )
        return result
    except requests.exceptions.RequestException as exc:
        try:
            print(f"ORS chunk error response: {exc.response.json()}")
        except Exception:
            pass
        print(f"Warning: ORS chunk failed ({exc}), using haversine for this chunk.")
        result = {}
        for i_global in src_idx:
            for j_global in tgt_idx:
                t = travel_time_minutes(
                    all_nodes[i_global]["lat"], all_nodes[i_global]["lng"],
                    all_nodes[j_global]["lat"], all_nodes[j_global]["lng"],
                )
                result[(i_global, j_global)] = (
                    0 if i_global == j_global else max(1, int(round(t * 60)))
                )
        return result


def build_time_matrix_ors(nodes):
    """
    Parallel version: builds N×N matrix using ORS with chunking.
    """
    ors_api_key = os.getenv("ORS_API_KEY", "")
    if not ors_api_key:
        print("Warning: ORS_API_KEY not set — using haversine matrix.")
        return build_time_matrix_haversine(nodes)

    n = len(nodes)
    coords = [[nd["lng"], nd["lat"]] for nd in nodes]

    matrix = [[0] * n for _ in range(n)]

    tasks = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for i_start in range(0, n, ORS_CHUNK_SIZE):
            i_end = min(i_start + ORS_CHUNK_SIZE, n)

            for j_start in range(0, n, ORS_CHUNK_SIZE):
                j_end = min(j_start + ORS_CHUNK_SIZE, n)

                src_idx = list(range(i_start, i_end))
                tgt_idx = list(range(j_start, j_end))

                future = executor.submit(
                    _ors_chunk, coords, src_idx, tgt_idx, nodes
                )
                tasks.append(future)

        for future in as_completed(tasks):
            chunk = future.result()
            for (i, j), val in chunk.items():
                matrix[i][j] = val

    print(f"Info: Travel-time matrix built for {n} nodes.")
    return matrix


# ---------------------------------------------------------------------------
# Node list builder
# ---------------------------------------------------------------------------

def build_node_list(hub, properties):
    """
    Returns a flat list: [depot, prop_0, prop_1, …]
    Node 0 is always the depot (hub).
    """
    nodes = [{
        "type": "hub",
        "lat": hub["lat"],
        "lng": hub["lng"],
        "service_time_minutes": 0,
        "water_demand_gallons": 0,
    }]
    for p in properties:
        nodes.append({
            "type": "property",
            "id": p["id"],
            "lat": p["lat"],
            "lng": p["lng"],
            "address": p.get("address", "Address unknown"),
            "property_type": p.get("property_type", "A"),
            "service_time_minutes": p.get(
                "service_time_minutes", DEFAULT_PROPERTY_SERVICE_TIME_MINUTES
            ),
            "water_demand_gallons": p.get(
                "water_demand_gallons", DEFAULT_PROPERTY_WATER_DEMAND_GALLONS
            ),
            "priority_score": p.get("priority_score", 100),
        })
    return nodes


# ---------------------------------------------------------------------------
# Hydrant post-processing
# ---------------------------------------------------------------------------

def insert_hydrant_stops(property_stops, vehicle, hydrants, hub, now):
    """
    For truck vehicles (C/D): inserts hydrant refill stops wherever the
    truck would run out of water before reaching the next property.
    Truck starts with an empty tank.

    For non-truck vehicles: passes through unchanged.

    Computes arrival_time for every stop based on haversine travel times
    from hub → stop_0 → stop_1 → …
    """
    tank_capacity = TEAM_WATER_CAPACITY_GALLONS.get(vehicle.get("team_type"))

    if tank_capacity is None or not hydrants:
        # Non-truck: just compute arrival times
        return _assign_arrival_times(property_stops, hub, now)

    refill_duration = HYDRANT_REFILL_DURATION_MINUTES.get(
        vehicle.get("team_type"), 5
    )

    water_remaining = 0
    result = []
    cur_lat, cur_lng = hub["lat"], hub["lng"]
    cur_time = 0.0  # minutes elapsed since route start

    for prop in property_stops:
        demand = prop.get("water_demand_gallons", DEFAULT_PROPERTY_WATER_DEMAND_GALLONS)

        # Refill before this property if needed
        if demand > water_remaining:
            h = nearest_hydrant(cur_lat, cur_lng, hydrants)
            cur_time += travel_time_minutes(cur_lat, cur_lng, h["lat"], h["lng"])
            result.append({
                "type": "hydrant_refill",
                "hydrant_id": h["id"],
                "address": h.get("address", "Hydrant"),
                "lat": h["lat"],
                "lng": h["lng"],
                "duration_min": refill_duration,
                "arrival_time": _fmt_time(now, cur_time),
            })
            cur_time += refill_duration
            water_remaining = tank_capacity
            cur_lat, cur_lng = h["lat"], h["lng"]

        # Travel to property
        cur_time += travel_time_minutes(cur_lat, cur_lng, prop["lat"], prop["lng"])
        result.append({
            **prop,
            "arrival_time": _fmt_time(now, cur_time),
        })
        cur_time += prop["service_time_min"]
        water_remaining -= demand
        cur_lat, cur_lng = prop["lat"], prop["lng"]

    return result


def _assign_arrival_times(property_stops, hub, now):
    """Compute arrival times for non-truck vehicles."""
    result = []
    cur_lat, cur_lng = hub["lat"], hub["lng"]
    cur_time = 0.0

    for stop in property_stops:
        cur_time += travel_time_minutes(cur_lat, cur_lng, stop["lat"], stop["lng"])
        result.append({**stop, "arrival_time": _fmt_time(now, cur_time)})
        cur_time += stop["service_time_min"]
        cur_lat, cur_lng = stop["lat"], stop["lng"]

    return result


def _fmt_time(now, minutes_offset):
    dt = datetime.fromtimestamp(
        now.timestamp() + minutes_offset * 60, tz=timezone.utc
    )
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


# ---------------------------------------------------------------------------
# Core OR-Tools optimiser
# ---------------------------------------------------------------------------

def optimize_vehicles(hub, properties, vehicles, hydrants, now):
    """
    Runs a single OR-Tools VRP for the given vehicles and properties.

    Key design decisions (based on OR-Tools documentation):

    1. A single transit callback (travel + service time) is used for BOTH
       the arc cost evaluator AND the Time dimension. This means the solver
       minimises the same quantity it uses to enforce the time budget —
       they are on the same scale.

    2. Per-vehicle time budgets are enforced via AddDimensionWithVehicleCapacity,
       which accepts one capacity value per vehicle. This is more correct than
       AddDimension (single global cap) for mixed-budget fleets.

    3. Every property node is wrapped in an AddDisjunction so the solver can
       drop it (make it optional) if it cannot fit within any vehicle's budget.
       The penalty is set high enough that the solver will always prefer to
       visit a node over dropping it (penalty >> any realistic single-arc cost).

    4. VehicleVar eligibility (which vehicle types can visit which property
       types) is enforced via VehicleVar(node).SetValues(allowed_vehicle_indices).
       Guard against empty allowed list to prevent model infeasibility.

    Returns {"routes": [...], "dropped": [...]}
    """
    if not properties or not vehicles:
        return {
            "routes": [],
            "dropped": [
                {"property_id": p["id"], "reason": "No eligible vehicle"}
                for p in properties
            ],
        }

    nodes = build_node_list(hub, properties)
    n_nodes = len(nodes)
    n_vehicles = len(vehicles)
    depot = 0

    # ------------------------------------------------------------------
    # Travel-time matrix (seconds). Used directly by the transit callback.
    # Includes service time at the FROM node so the solver accounts for
    # the full time cost of visiting a node before moving to the next.
    # ------------------------------------------------------------------
    travel_matrix = build_time_matrix_ors(nodes)

    # ------------------------------------------------------------------
    # Routing model
    # ------------------------------------------------------------------
    manager = pywrapcp.RoutingIndexManager(n_nodes, n_vehicles, depot)
    routing = pywrapcp.RoutingModel(manager)

    # ------------------------------------------------------------------
    # Cost callback: travel time only (what the solver minimizes).
    # Time callback: travel + service time (used by the Time dimension).
    # Separating these keeps the objective focused on travel while the
    # Time dimension enforces realistic budgets including service.
    # ------------------------------------------------------------------
    def cost_callback(from_index, to_index):
        from_node = manager.IndexToNode(from_index)
        to_node = manager.IndexToNode(to_index)
        return travel_matrix[from_node][to_node]

    cost_idx = routing.RegisterTransitCallback(cost_callback)
    routing.SetArcCostEvaluatorOfAllVehicles(cost_idx)

    def time_callback(from_index, to_index):
        from_node = manager.IndexToNode(from_index)
        to_node = manager.IndexToNode(to_index)
        service_sec = int(round(nodes[from_node]["service_time_minutes"] * 60))
        return travel_matrix[from_node][to_node] + service_sec

    time_idx = routing.RegisterTransitCallback(time_callback)

    # ------------------------------------------------------------------
    # Time dimension with per-vehicle budgets.
    #
    # AddDimensionWithVehicleCapacity takes a list of per-vehicle maximums,
    # which is the correct approach for mixed-budget fleets (vs AddDimension
    # which uses a single global cap shared by all vehicles).
    # ------------------------------------------------------------------
    vehicle_budgets_sec = [
        v["team_time_budget_minutes"] * 60 for v in vehicles
    ]

    routing.AddDimensionWithVehicleCapacity(
        time_idx,
        0,                    # no waiting time (slack)
        vehicle_budgets_sec,  # per-vehicle time budget in seconds
        True,                 # cumul starts at zero for every vehicle
        "Time",
    )

    # ------------------------------------------------------------------
    # Optional nodes: every property node can be dropped.
    #
    # Penalty is set to max_budget_seconds so the solver always prefers
    # visiting a node over dropping it (a dropped node costs as much as
    # an entire route). With no priority weighting at this stage, all
    # properties are treated equally.
    # ------------------------------------------------------------------
    max_budget_sec = max(vehicle_budgets_sec)

    for node_idx in range(1, n_nodes):  # skip depot (node 0)
        routing.AddDisjunction(
            [manager.NodeToIndex(node_idx)],
            max_budget_sec,
        )

    # ------------------------------------------------------------------
    # Vehicle eligibility: restrict which vehicle indices can visit each
    # property node. Guard against empty allowed list (which would make
    # the model immediately infeasible).
    # ------------------------------------------------------------------
    # For a tiered call, all vehicles in `vehicles` are of the same tier
    # and can visit all properties passed in. However we still apply the
    # guard defensively in case this function is called with mixed types.
    ELIGIBLE_TYPES = {
        "A": {"A"},
        "B": {"A", "B"},
        "C": {"A", "B", "C"},
        "D": {"A", "B", "C"},
    }

    for node_idx in range(1, n_nodes):
        prop_type = nodes[node_idx]["property_type"]
        allowed = [
            v_idx for v_idx, v in enumerate(vehicles)
            if prop_type in ELIGIBLE_TYPES.get(v["team_type"], set())
        ]
        # Only restrict if at least one vehicle is eligible but not all —
        # if no vehicle is eligible, the disjunction penalty handles dropping.
        if 0 < len(allowed) < n_vehicles:
            routing.VehicleVar(
                manager.NodeToIndex(node_idx)
            ).SetValues(allowed)

    # ------------------------------------------------------------------
    # Search parameters
    #
    # PARALLEL_CHEAPEST_INSERTION builds all routes simultaneously as the
    # initial solution, distributing nodes across vehicles from the start.
    # This is better than PATH_CHEAPEST_ARC for multi-vehicle problems with
    # optional nodes, which tends to over-load the first vehicle.
    # ------------------------------------------------------------------
    search_params = pywrapcp.DefaultRoutingSearchParameters()
    search_params.first_solution_strategy = (
        routing_enums_pb2.FirstSolutionStrategy.PARALLEL_CHEAPEST_INSERTION
    )
    search_params.local_search_metaheuristic = (
        routing_enums_pb2.LocalSearchMetaheuristic.GUIDED_LOCAL_SEARCH
    )
    search_params.time_limit.seconds = SOLVER_TIME_LIMIT_SECONDS

    # ------------------------------------------------------------------
    # Solve
    # ------------------------------------------------------------------
    solution = routing.SolveWithParameters(search_params)

    if not solution:
        return {
            "routes": [],
            "dropped": [
                {"property_id": p["id"], "reason": "No solution found"}
                for p in properties
            ],
        }

    # ------------------------------------------------------------------
    # Extract routes
    # ------------------------------------------------------------------
    routes = []
    visited_ids = set()

    for v_idx, vehicle in enumerate(vehicles):
        raw_stops = []
        index = routing.Start(v_idx)

        while not routing.IsEnd(index):
            node_idx = manager.IndexToNode(index)
            node = nodes[node_idx]
            if node["type"] == "property":
                raw_stops.append({
                    "type": "property",
                    "property_id": node["id"],
                    "address": node["address"],
                    "lat": node["lat"],
                    "lng": node["lng"],
                    "service_time_min": node["service_time_minutes"],
                    "water_demand_gallons": node["water_demand_gallons"],
                    "arrival_time": None,  # filled in below
                })
                visited_ids.add(node["id"])
            index = solution.Value(routing.NextVar(index))

        # Always emit a record for every vehicle, even if it has no stops
        if not raw_stops:
            routes.append({
                "vehicle_id": vehicle["id"],
                "team_type": vehicle["team_type"],
                "stops": [],
                "totals": {
                    "properties_visited": 0,
                    "travel_min": 0,
                    "service_min": 0,
                    "refill_min": 0,
                    "total_min": 0,
                },
                "maps_url": None,
            })
            continue

        # Insert hydrant stops and compute all arrival times
        stops = insert_hydrant_stops(raw_stops, vehicle, hydrants, hub, now)

        property_stops = [s for s in stops if s["type"] == "property"]
        hydrant_stops  = [s for s in stops if s["type"] == "hydrant_refill"]

        service_min = sum(s["service_time_min"] for s in property_stops)
        refill_min  = sum(s["duration_min"]     for s in hydrant_stops)

        # Compute actual travel time by walking the stop sequence
        travel_min = 0.0
        prev_lat, prev_lng = hub["lat"], hub["lng"]
        for s in stops:
            travel_min += travel_time_minutes(prev_lat, prev_lng, s["lat"], s["lng"])
            prev_lat, prev_lng = s["lat"], s["lng"]
        travel_min = int(round(travel_min))

        maps_stops = [{"lat": s["lat"], "lng": s["lng"]} for s in stops]

        routes.append({
            "vehicle_id": vehicle["id"],
            "team_type": vehicle["team_type"],
            "stops": stops,
            "totals": {
                "properties_visited": len(property_stops),
                "travel_min": travel_min,
                "service_min": int(round(service_min)),
                "refill_min": int(round(refill_min)),
                "total_min": travel_min + int(round(service_min)) + int(round(refill_min)),
            },
            "maps_url": generate_maps_url(maps_stops, hub),
        })

    dropped = [
        {"property_id": p["id"], "reason": "Could not fit in time budget"}
        for p in properties
        if p["id"] not in visited_ids
    ]

    return {"routes": routes, "dropped": dropped}


# ---------------------------------------------------------------------------
# Tiered optimisation (A → B → C/D)
# ---------------------------------------------------------------------------

def run_tiered_optimization(hub, all_properties, all_vehicles, hydrants, now):
    """
    Three-pass tiered approach so that each team type only visits the
    property type it is trained for, while unserved properties cascade
    to the next capable tier.

    Pass 1: Team A vehicles  + Type A properties
    Pass 2: Team B vehicles  + Type B properties + Pass 1 drops
    Pass 3: Team C/D vehicles + Type C properties + Pass 2 drops
    """
    prop_by_id = {p["id"]: p for p in all_properties}

    type_a = [p for p in all_properties if p["property_type"] == "A"]
    type_b = [p for p in all_properties if p["property_type"] == "B"]
    type_c = [p for p in all_properties if p["property_type"] == "C"]

    team_a  = [v for v in all_vehicles if v["team_type"] == "A"]
    team_b  = [v for v in all_vehicles if v["team_type"] == "B"]
    team_cd = [v for v in all_vehicles if v["team_type"] in ("C", "D")]

    all_routes  = []
    all_dropped = []

    # ------------------------------------------------------------------
    # Pass 1: Team A + Type A properties
    # ------------------------------------------------------------------
    if team_a and type_a:
        print(
            f"\nPass 1: {len(team_a)} Team A vehicle(s), "
            f"{len(type_a)} Type A properties"
        )
        r1 = optimize_vehicles(hub, type_a, team_a, [], now)
        all_routes.extend(r1["routes"])
        pass1_drops = [prop_by_id[d["property_id"]] for d in r1["dropped"]]
    else:
        print("\nPass 1 skipped — no Team A vehicles or Type A properties.")
        pass1_drops = list(type_a)

    # ------------------------------------------------------------------
    # Pass 2: Team B + Type B properties + Pass 1 drops
    # ------------------------------------------------------------------
    pass2_props = type_b + pass1_drops
    if team_b and pass2_props:
        print(
            f"\nPass 2: {len(team_b)} Team B vehicle(s), "
            f"{len(pass2_props)} properties "
            f"({len(type_b)} Type B + {len(pass1_drops)} from Pass 1)"
        )
        r2 = optimize_vehicles(hub, pass2_props, team_b, [], now)
        all_routes.extend(r2["routes"])
        pass2_drops = [prop_by_id[d["property_id"]] for d in r2["dropped"]]
    else:
        print("\nPass 2 skipped — no Team B vehicles or properties.")
        pass2_drops = list(pass2_props)

    # ------------------------------------------------------------------
    # Pass 3: Team C/D + Type C properties + Pass 2 drops
    # ------------------------------------------------------------------
    pass3_props = type_c + pass2_drops
    if team_cd and pass3_props:
        print(
            f"\nPass 3: {len(team_cd)} Team C/D vehicle(s), "
            f"{len(pass3_props)} properties "
            f"({len(type_c)} Type C + {len(pass2_drops)} from Pass 2)"
        )
        r3 = optimize_vehicles(hub, pass3_props, team_cd, hydrants, now)
        all_routes.extend(r3["routes"])
        all_dropped = r3["dropped"]
    else:
        print("\nPass 3 skipped — no Team C/D vehicles or properties.")
        all_dropped = [
            {"property_id": p["id"], "reason": "No eligible vehicle in any pass"}
            for p in pass3_props
        ]

    return {"routes": all_routes, "dropped": all_dropped}


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    _load_env_file()
    args = parse_args()
    now = datetime.now(tz=timezone.utc)

    hub = {"lat": args.hub_lat, "lng": args.hub_lng}
    vehicles = build_vehicles(args.team)
    hub, vehicles, properties, hydrants = load_runtime_data(hub, vehicles)

    print(f"\n{'='*60}")
    print(f"Route Optimisation — {now.strftime('%Y-%m-%dT%H:%M:%SZ')}")
    print(f"Vehicles: {len(vehicles)} | Properties: {len(properties)} | Hydrants: {len(hydrants)}")
    print(f"{'='*60}")

    result = run_tiered_optimization(hub, properties, vehicles, hydrants, now)

    print(f"\n{'='*60}")
    print("RESULT")
    print(f"{'='*60}")
    print(json.dumps(result, indent=2))

    output_file = write_results(result, args.output, now)
    print(f"\nWrote results to: {output_file}")
    print(
        f"\nSummary: {len(result['routes'])} route(s), "
        f"{len(result['dropped'])} dropped propert{'y' if len(result['dropped']) == 1 else 'ies'}"
    )
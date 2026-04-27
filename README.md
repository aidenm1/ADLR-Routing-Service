# Route Generation OR-Tools

This project runs route optimization against Supabase data using Google OR-Tools.

## Requirements

- Python 3.12 or newer
- A Supabase project with `Property` and `Hydrants` tables
- Optional: OpenRouteService API key for improved routing distances

## Create a virtual environment

From the project root:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
```

If you already have a local environment and want to reuse it, activate that instead.

## Install dependencies

Install the Python packages used by the script:

```bash
pip install ortools requests supabase python-dotenv
```

Notes:

- `python-dotenv` is optional, but recommended for loading `.env` files.
- The script also includes a small built-in `.env` fallback parser, so it can still load environment variables even if `python-dotenv` is not installed.

## Environment variables

Create a `.env` file in the project root with the following values:

```bash
NEXT_PUBLIC_SUPABASE_URL=your_supabase_url
NEXT_PUBLIC_SUPABASE_ANON_KEY=your_supabase_anon_key
ORS_API_KEY=your_openrouteservice_key
```

The script will also accept these alternate Supabase variable names if you prefer them:

- `SUPABASE_URL`
- `SUPABASE_ANON_KEY`
- `SUPABASE_KEY`

## Run the script

Each team is passed as `TEAM_TYPE:TIME_MIN`.

```bash
python route-generation.py \
  --num-teams 3 \
  --team A:300 \
  --team B:300 \
  --team C:300 \
  --output latest-result.json
```

Arguments:

- `--num-teams` must match the number of `--team` entries.
- `--team` defines the team type and time budget.
- `--output` is optional. If omitted, the script writes a timestamped JSON file in the current directory.
- `--hub-lat` and `--hub-lng` can be used to override the default hub coordinates.

## What the script does

- Loads eligible properties from Supabase
- Keeps only properties not watered in the last 30 days
- Selects top-priority properties by tier and team count
- Loads all hydrants from Supabase
- Builds routes with OR-Tools
- Inserts hydrant refill stops for truck teams
- Writes the final route result to JSON

## Output

The output JSON contains:

- `routes`
- `dropped`

Each route includes the ordered stops, totals, and a Google Maps directions URL.

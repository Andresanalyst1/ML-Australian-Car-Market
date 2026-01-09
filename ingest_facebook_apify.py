from apify_client import ApifyClient
import pandas as pd
import os

# ==============================
# CONFIG
# ==============================
APIFY_TOKEN = os.getenv("APIFY_TOKEN")
ACTOR_ID = "Y0QGH7cuqgKtNbEgt"

OUTPUT_CSV = "data/raw_data_facebook_scraper.csv"
LAST_RUN_FILE = "state/last_processed_run.txt"

# ==============================
# SETUP
# ==============================
client = ApifyClient(APIFY_TOKEN)
os.makedirs("state", exist_ok=True)
os.makedirs("data", exist_ok=True)

# ==============================
# LOAD LAST PROCESSED RUN ID
# ==============================
last_processed_run_id = None

if os.path.exists(LAST_RUN_FILE):
    with open(LAST_RUN_FILE, "r") as f:
        last_processed_run_id = f.read().strip()

# ==============================
# FETCH RECENT SUCCESSFUL RUNS
# ==============================
runs = client.actor(ACTOR_ID).runs().list(
    status="SUCCEEDED",
    desc=False,   # oldest → newest (IMPORTANT)
    limit=1000    # enough for a day of hourly runs
).items

if not runs:
    print("No successful runs found.")
    exit()

# ==============================
# FILTER NEW RUNS
# ==============================
new_runs = []
start_collecting = last_processed_run_id is None

for run in runs:
    if start_collecting:
        new_runs.append(run)

    if run["id"] == last_processed_run_id:
        start_collecting = True
        new_runs = []  # reset, only take runs AFTER this one

if not new_runs:
    print("No new runs to process.")
    exit()

print(f"Processing {len(new_runs)} new runs.")

# ==============================
# LOAD EXISTING DATA
# ==============================
if os.path.exists(OUTPUT_CSV):
    df_existing = pd.read_csv(OUTPUT_CSV)
else:
    df_existing = pd.DataFrame()

all_new_data = []

# ==============================
# PROCESS EACH RUN
# ==============================
for run in new_runs:
    dataset_id = run["defaultDatasetId"]
    items = client.dataset(dataset_id).list_items().items

    if not items:
        continue

    df_run = pd.DataFrame(items)
    all_new_data.append(df_run)

# ==============================
# MERGE & DEDUPLICATE
# ==============================
if not all_new_data:
    print("No new data found in runs.")
    exit()

df_new = pd.concat(all_new_data, ignore_index=True)

df_combined = pd.concat(
    [df_existing, df_new],
    ignore_index=True
)

# Strict deduplication
if "id" not in df_combined.columns:
    raise RuntimeError("Expected unique column 'id' not found.")

df_combined["id"] = df_combined["id"].astype(str)

before = len(df_combined)
df_combined = df_combined.drop_duplicates(subset="id")
after = len(df_combined)

print(f"Deduplicated using 'id': {before} → {after}")

# ==============================
# SAVE OUTPUT
# ==============================
df_combined.to_csv(OUTPUT_CSV, index=False)

# ==============================
# UPDATE LAST PROCESSED RUN
# ==============================
last_run_id = new_runs[-1]["id"]

with open(LAST_RUN_FILE, "w") as f:
    f.write(last_run_id)

print("Daily batch ingestion completed successfully.")
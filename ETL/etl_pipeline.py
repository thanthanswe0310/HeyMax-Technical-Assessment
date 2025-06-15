import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

# Connection parameters
conn = psycopg2.connect(
    dbname="DemoHeyMax",
    user="postgres",
    password="1234",
    host="localhost",
    port="5432"
)
cur = conn.cursor()

# Load the CSV file
df = pd.read_csv(
    r"\..\..\event_stream.csv",
    parse_dates=["event_time"]
)

# Build dim_users
dim_users = (
    df.sort_values("event_time")
    .groupby("user_id")
    .agg({
        "event_time": "first",
        "platform": "first",
        "utm_source": "first",
        "country": "first"
    })
    .rename(columns={"event_time": "first_seen"})
    .reset_index()
)

dim_users["first_seen"] = dim_users["first_seen"].dt.to_pydatetime()
dim_user_values = [tuple(x) for x in dim_users.to_numpy()]

# Insert into HeyMaxSchema.dim_users
execute_values(cur, """
    INSERT INTO dim_users (user_id, first_seen, platform, utm_source, country)
    VALUES %s
    ON CONFLICT (user_id) DO NOTHING
""", dim_user_values)

# Prepare fct_events
fct_events = df[[
    "user_id", "event_time", "event_type", "transaction_category", "miles_amount"
]].copy()

fct_events["event_time"] = fct_events["event_time"].dt.to_pydatetime()
fct_events["miles_amount"] = fct_events["miles_amount"].fillna(0)
fct_event_values = [tuple(x) for x in fct_events.to_numpy()]

# Insert into HeyMaxSchema.fct_events
execute_values(cur, """
    INSERT INTO fct_events (user_id, event_time, event_type, transaction_category, miles_amount)
    VALUES %s
""", fct_event_values)

# Commit and close
conn.commit()
cur.close()
conn.close()

print("ETL load complete.")

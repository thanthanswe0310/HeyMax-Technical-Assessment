-- public schema is a default schema. I have created two tables (a dimension table and a fact table) in the PostgreSQL database.

-- Create dim_users table
CREATE TABLE IF NOT EXISTS "public".dim_users (
    user_id TEXT PRIMARY KEY,
    first_seen TIMESTAMP,
    platform TEXT,
    utm_source TEXT,
    country TEXT
);

-- Create fct_events table

CREATE TABLE IF NOT EXISTS "public".fct_events (
    id SERIAL PRIMARY KEY,
    user_id TEXT,
    event_time TIMESTAMP,
    event_type TEXT,
    transaction_category TEXT,
    miles_amount FLOAT,
    FOREIGN KEY (user_id) REFERENCES "public".dim_users(user_id)
);



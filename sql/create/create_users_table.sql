CREATE TABLE IF NOT EXISTS users (
    user_id INT PRIMARY KEY,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    gender TEXT,
    birth_date DATE,
    city TEXT,
    state TEXT,
    state_code TEXT,
    postal_code TEXT,
    country TEXT,
    age_group TEXT
);
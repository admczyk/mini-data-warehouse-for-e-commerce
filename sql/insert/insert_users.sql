INSERT INTO users (
    user_id, first_name, last_name, gender, birth_date,
    city, state, state_code, postal_code, country, age_group
) VALUES %s
ON CONFLICT (user_id) DO NOTHING;
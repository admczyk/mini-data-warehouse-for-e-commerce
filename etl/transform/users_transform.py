from airflow.decorators import task
import pandas as pd
import datetime as dt
import json

def normalize_users_dtypes(users_df):
    users_df["user_id"] = pd.to_numeric(users_df["user_id"], errors="coerce").astype("Int64")

    users_df["first_name"] = users_df["first_name"].astype("string").str.strip()
    users_df["last_name"] = users_df["last_name"].astype("string").str.strip()
    users_df["gender"] = users_df["gender"].astype("string").str.strip()
    users_df["city"] = users_df["city"].astype("string").str.strip()
    users_df["state"] = users_df["state"].astype("string").str.strip()
    users_df["state_code"] = users_df["state_code"].astype("string").str.strip()
    users_df["postal_code"] = users_df["postal_code"].astype("string").str.strip()
    users_df["country"] = users_df["country"].astype("string").str.strip()
   
    users_df["birth_date"] = pd.to_datetime(users_df["birth_date"], errors="coerce")

    return users_df

def clean_and_validate_users_data(users_df):
    required_cols = ["user_id", "first_name", "last_name"]
    
    missing_mask = users_df[required_cols].isnull().any(axis=1)
    if missing_mask.any():
        print(f"Deleted {missing_mask.sum()} records with missing required values.")
        users_df = users_df[~missing_mask]

    invalid_ids = users_df["user_id"] <= 0
    if invalid_ids.any():
        print(f"Removed {invalid_ids.sum()} invalid IDs.")
        df = df[~invalid_ids]

    dups = users_df.duplicated(subset=["user_id"])
    if dups.any():
        print(f"Removed {dups.sum()} duplicate rows.")
        users_df = users_df[~dups]

    missing_str = users_df["gender"].isnull()
    if missing_str.any():
        print(f"Found {missing_str.sum()} rows with missing strings. Set them as \"none\".")
        users_df["gender"] =  users_df["gender"].fillna("none")

    missing_str = users_df["city"].isnull()
    if missing_str.any():
        print(f"Found {missing_str.sum()} rows with missing strings. Set them as \"none\".")
        users_df["city"] =  users_df["city"].fillna("none")

    missing_str = users_df["state"].isnull()
    if missing_str.any():
        print(f"Found {missing_str.sum()} rows with missing strings. Set them as \"none\".")
        users_df["state"] =  users_df["state"].fillna("none")

    missing_str = users_df["postal_code"].isnull()
    if missing_str.any():
        print(f"Found {missing_str.sum()} rows with missing strings. Set them as \"none\".")
        users_df["postal_code"] =  users_df["postal_code"].fillna("none")

    missing_str = users_df["country"].isnull()
    if missing_str.any():
        print(f"Found {missing_str.sum()} rows with missing strings. Set them as \"none\".")
        users_df["country"] =  users_df["country"].fillna("none")


    return users_df

def add_new_users_values(data):
    data["birth_date"] = pd.to_datetime(data["birth_date"])
    data["age_group"] = pd.cut(
        (pd.Timestamp.today().normalize() - data["birth_date"]).dt.days // 365,
        bins=[17, 24, 34, 44, 54, 64, 120],
        labels=["18-24", "25-34", "35-44", "45-54", "55-64", "65+"]
    )
    return data

@task  
def transform_users_data(data):
    users_df = pd.DataFrame(data)

    users_df = users_df.rename(columns={
        "id": "user_id",
        "firstName": "first_name",
        "lastName": "last_name",
        "birthDate": "birth_date",
        "stateCode": "state_code",
        "postalCode": "postal_code"
    })

    normalized_users_df =  normalize_users_dtypes(users_df)

    cleaned_users_df = clean_and_validate_users_data(normalized_users_df)

    final_users_df = add_new_users_values(cleaned_users_df)

    return final_users_df
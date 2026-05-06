from airflow.decorators import task
import pandas as pd
import datetime as dt

def normalize_reviews_dtypes(reviews_df):
    reviews_df["review_id"] = pd.to_numeric(reviews_df["review_id"], errors="coerce").astype("Int64")
    reviews_df["product_id"] = pd.to_numeric(reviews_df["product_id"], errors="coerce").astype("Int64")
    reviews_df["rating"] = pd.to_numeric(reviews_df["rating"], errors="coerce")

    reviews_df["review_comment"] = reviews_df["review_comment"].astype("string").str.strip()
   
    reviews_df["review_date"] = pd.to_datetime(reviews_df["review_date"], errors="coerce")

    return reviews_df

def clean_and_validate_reviews_data(reviews_df):
    required_cols = ["review_id", "product_id", "rating"]
    
    missing_mask = reviews_df[required_cols].isnull().any(axis=1)
    if missing_mask.any():
        print(f"Deleted {missing_mask.sum()} records with missing reqired values.")
        reviews_df = reviews_df[~missing_mask]

    ids = required_cols[:2]
    invalid_ids = (reviews_df[ids] <= 0).any(axis=1)
    if invalid_ids.any():
        print(f"Deleted {invalid_ids.sum()} records with invalid ids.")
        reviews_df = reviews_df[~invalid_ids]

    reviews_df["rating"] = reviews_df["rating"].clip(0, 5)

    dups = reviews_df.duplicated(subset=["review_id"])
    if dups.any():
        print(f"Removed {dups.sum()} duplicate rows.")
        reviews_df = reviews_df[~dups]

    missing_str = reviews_df["review_comment"].isnull()
    if missing_str.any():
        print(f"Found {missing_str.sum()} rows with missing strings. Filled them with \"no comment\".")
        reviews_df["review_comment"] =  reviews_df["review_comment"].fillna("no comment")

    missing_date = reviews_df["review_date"].isnull()
    if missing_date.any():
       print(f"Deleted {missing_date.sum()} records with missing date.")
       reviews_df = reviews_df[~missing_date]

    return reviews_df

def add_new_reviews_values(data):
    data["review_year"] = data["review_date"].dt.year
    data["review_month"] = data["review_date"].dt.month
    data["review_bucket"] = pd.cut(
        data["rating"],
        bins=[0, 2, 3, 4, 5],
        labels=["bad", "neutral", "good", "excellent"]
    )
    return data

@task
def transform_reviews_data(reviews_df):
    reviews_df = reviews_df.explode("reviews")
    print(reviews_df)
    reviews_df = pd.concat(
            [reviews_df.drop(columns=["reviews"]).reset_index(drop=True),
             pd.json_normalize(reviews_df["reviews"].reset_index(drop=True))],
             axis = 1
        )
    reviews_df = reviews_df.drop(columns=["reviewerName", "reviewerEmail"])
    reviews_df = reviews_df.reset_index()
    reviews_df["index"] = reviews_df["index"] + 1

    reviews_df = reviews_df.rename(columns={
        "index": "review_id",
        "comment": "review_comment",
        "date": "review_date"
        })

    normalized_reviews_df = normalize_reviews_dtypes(reviews_df)

    cleaned_reviews_df = clean_and_validate_reviews_data(normalized_reviews_df)

    final_reviews_df = add_new_reviews_values(cleaned_reviews_df)

    return final_reviews_df
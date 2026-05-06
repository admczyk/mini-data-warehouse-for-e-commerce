from airflow.decorators import task
import pandas as pd
import datetime as dt

def normalize_product_dtypes(df):
    df["product_id"] = pd.to_numeric(df["product_id"], errors="coerce").astype("Int64")
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["discount_percentage"] = pd.to_numeric(df["discount_percentage"], errors="coerce")
    df["overall_rating"] = pd.to_numeric(df["overall_rating"], errors="coerce")
    df["stock"] = pd.to_numeric(df["stock"], errors="coerce").astype("Int64")

    df["title"] = df["title"].astype("string").str.strip()
    df["description"] = df["description"].astype("string").str.strip()
    df["category"] = df["category"].astype("string").str.strip()
    df["brand"] = df["brand"].astype("string").str.strip()
    
    return df

def clean_and_validate_product_data(df):
    required_cols = ["product_id", "title", "price", "category", "stock"]

    missing_mask = df[required_cols].isnull().any(axis=1)
    if missing_mask.any():
        print(f"Deleted {missing_mask.sum()} records with missing reqired values.")
        df = df[~missing_mask]

    num_cols = df[required_cols].select_dtypes(include="number")
    neg_mask = (num_cols < 0).any(axis=1)
    if neg_mask.any():
        print(f"Deleted {neg_mask.sum()} records with negative values.")
        df = df[~neg_mask]

    invalid_ids = df["product_id"] <= 0
    if invalid_ids.any():
        print(f"Removed {invalid_ids.sum()} invalid IDs.")
        df = df[~invalid_ids]

    other_cols = [col for col in df.columns if col not in required_cols]

    str_cols = df[other_cols].select_dtypes(include=["object"])
    missing_mask = str_cols.isnull().any(axis=1)
    if missing_mask.any():
        print(f"Found {missing_mask.sum()} rows with missing strings. Filled them with \"not set\".")
        df[str_cols.columns] = str_cols.fillna("not set")

    num_cols = df[other_cols].select_dtypes(include="number")
    missing_mask = num_cols.isnull().mean()
    for col, ratio in missing_mask.items():
        if ratio == 0:
            continue

        if ratio < 0.05:
            print(f"Deleted records with missing {col} value.")
            df = df.dropna(subset=[col])
        elif ratio < 0.2:
            print(f"Replacing missing {col} with 0.")
            df[col] = df[col].fillna(0)
    
    df["discount_percentage"] = df["discount_percentage"].clip(0, 100)
    df["overall_rating"] = df["overall_rating"].clip(0, 5)

    dups = df.duplicated(subset=["product_id"])
    if dups.any():
        print(f"Removed {dups.sum()} duplicate rows.")
        df = df[~dups]

    return df

def add_new_product_values(data):
    data["final_price"] = data["price"] * (1-data["discount_percentage"])
    data["price_bucket"] = pd.cut(
        data["price"],
        bins=[0, 100, 500, 2000, 10000, float("inf")],
        labels=["very_low", "low", "medium", "high", "premium"]
    )
    data["rating_bucket"] = pd.cut(
        data["overall_rating"],
        bins=[0, 2, 3, 4, 5],
        labels=["bad", "neutral", "good", "excellent"]
    )
    data["value_score"] = round(data["overall_rating"] / data["price"], 3)
    return data

@task
def transform_product_data(data):
    products_df = pd.DataFrame(data)

    products_df = products_df["products"]
    products_df = pd.json_normalize(products_df)

    products_df = products_df.drop(columns=[
        "sku", "weight", "warrantyInformation", "shippingInformation",
        "returnPolicy", "images", "thumbnail", "dimensions.width",
        "dimensions.height", "dimensions.depth", "meta.createdAt",
        "meta.updatedAt", "meta.barcode", "meta.qrCode", 
        "availabilityStatus", "minimumOrderQuantity"
    ])
    
    products_df = products_df.rename(columns={
        "id": "product_id",
        "discountPercentage": "discount_percentage",
        "rating": "overall_rating"
    })

    tags_df = products_df[["product_id", "tags"]]
    products_df = products_df.drop(columns=["tags"])

    reviews_df = products_df[["product_id", "reviews"]]
    products_df = products_df.drop(columns=["reviews"])
    
    products_df = normalize_product_dtypes(products_df)

    products_df = clean_and_validate_product_data(products_df)
    
    products_df = add_new_product_values(products_df)

    return {
        "products": products_df,
        "products_reviews": reviews_df,
        "products_tags": tags_df
    }
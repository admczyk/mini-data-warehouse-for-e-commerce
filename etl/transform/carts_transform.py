from airflow.decorators import task
import pandas as pd

def normalize_carts_dtypes(carts_df):
    carts_df["cart_id"] = pd.to_numeric(carts_df["cart_id"], errors="coerce")
    carts_df["cart_total"] = pd.to_numeric(carts_df["cart_total"], errors="coerce")
    carts_df["cart_total_discounted"] = pd.to_numeric(carts_df["cart_total_discounted"], errors="coerce")
    carts_df["user_id"] = pd.to_numeric(carts_df["user_id"], errors="coerce")
    carts_df["cart_total_quantity"] = pd.to_numeric(carts_df["cart_total_quantity"], errors="coerce")

    return carts_df

def clean_and_validate_carts_data(carts_df):
    required_cols = carts_df.columns

    missing_mask = carts_df[required_cols].isnull().any(axis=1)
    if missing_mask.any():
        print(f"Deleted {missing_mask.sum()} records with missing reqired values.")
        carts_df = carts_df[~missing_mask]

    ids = ["cart_id", "user_id"]
    invalid_ids = (carts_df[ids] <= 0).any(axis=1)
    print(invalid_ids)
    if invalid_ids.any():
        print(f"Removed {invalid_ids.sum()} invalid IDs.")
        carts_df = carts_df[~invalid_ids]

    dups = carts_df.duplicated(subset=["cart_id"])
    if dups.any():
        print(f"Removed {dups.sum()} duplicate rows.")
        carts_df = carts_df[~dups]

    return carts_df

def add_new_cart_values(data):
    data["cart_size_bucket"] = pd.cut(
        data["cart_total_quantity"],
        bins=[0, 3, 6, 12, 20],
        labels=["very_small", "small", "medium", "large"]
    )

    data["cart_value_bucket"] = pd.cut(
        data["cart_total_discounted"],
        bins=[0, 20, 100, 500, 2000, 10000, float("inf")],
        labels=["micro", "very_low", "low", "medium", "large", "enterprise"]
    )

    return data

@task
def transform_carts_data(data):
    carts_df = pd.DataFrame(data)

    carts_df = carts_df["carts"]
    carts_df = pd.json_normalize(carts_df)

    carts_df = carts_df.rename(columns={
        "id": "cart_id",
        "total": "cart_total",
        "userId": "user_id",
        "discountedTotal": "cart_total_discounted",
        "totalQuantity": "cart_total_quantity"})

    cart_products_df = carts_df[["cart_id", "products"]]
    
    carts_df = carts_df.drop(columns=["products", "totalProducts"])

    normalized_carts_df = normalize_carts_dtypes(carts_df)

    cleaned_carts_df = clean_and_validate_carts_data(normalized_carts_df)

    final_carts_df = add_new_cart_values(cleaned_carts_df)

    return {
        "carts": final_carts_df, 
        "carts_contents": cart_products_df
    }


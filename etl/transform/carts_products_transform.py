from airflow.decorators import task
import pandas as pd

def normalize_carts_products_dtypes(carts_df):
    carts_df["cart_id"] = pd.to_numeric(carts_df["cart_id"], errors="coerce")
    carts_df["product_id"] = pd.to_numeric(carts_df["product_id"], errors="coerce")
    carts_df["product_price"] = pd.to_numeric(carts_df["product_price"], errors="coerce")
    carts_df["product_quantity"] = pd.to_numeric(carts_df["product_quantity"], errors="coerce")
    carts_df["product_total"] = pd.to_numeric(carts_df["product_total"], errors="coerce")
    carts_df["product_total_discounted"] = pd.to_numeric(carts_df["product_total_discounted"], errors="coerce")

    return carts_df

def clean_and_validate_carts_products_data(carts_df):
    required_cols = carts_df.columns

    missing_mask = carts_df[required_cols].isnull().any(axis=1)
    if missing_mask.any():
        print(f"Deleted {missing_mask.sum()} records with missing reqired values.")
        carts_df = carts_df[~missing_mask]

    ids = ["cart_id", "product_id"]
    invalid_ids = (carts_df[ids] <= 0).any(axis=1)
    print(invalid_ids)
    if invalid_ids.any():
        print(f"Removed {invalid_ids.sum()} invalid IDs.")
        carts_df = carts_df[~invalid_ids]

    dups = carts_df.duplicated(subset=["cart_id", "product_id"])
    if dups.any():
        print(f"Removed {dups.sum()} duplicate rows.")
        carts_df = carts_df[~dups]

    return carts_df

@task
def transform_carts_products_data(carts_df):
    carts_products_df = carts_df.explode("products").reset_index(drop=True)
    products_df = pd.json_normalize(carts_products_df["products"])

    products_df = products_df.rename(columns={
        "id": "product_id", 
        "total": "product_total",
        "price": "product_price",
        "quantity": "product_quantity",
        "discountedTotal": "product_total_discounted"
    })

    products_df = products_df.drop(columns=["title", "thumbnail", "discountPercentage"])

    carts_products_df = carts_products_df.drop(columns=["products"]).reset_index(drop=True)
    carts_products_df = carts_products_df.join(products_df)

    normalized_cart_products_df = normalize_carts_products_dtypes(carts_products_df)

    final_cart_products_df = clean_and_validate_carts_products_data(normalized_cart_products_df)

    return final_cart_products_df
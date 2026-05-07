from airflow import DAG
from airflow.decorators import task
import etl.extract as ex
import etl.transform.carts_transform as ct
import etl.transform.carts_products_transform as cpt
import utils.files_io as f
import etl.load as ld
from datetime import datetime, timedelta
import pendulum

local_tz = pendulum.timezone("Europe/Warsaw")

default_args = {
    "owner": "admczyk",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
    "start_date": datetime(2026, 1, 1, tzinfo=local_tz),
}

@task
def load(data, category):
    return data[category]

with DAG (
    dag_id = "etl_carts",
    default_args = default_args,
    description = "DAG that initializes etl pipeline for carts",
    schedule = "@hourly",
    catchup = False
) as etl_carts:
    
    categories = ["carts", "carts_contents"]

    extracted_data = ex.get_data(categories[0])
    save_data_to_json = f.save_to_json(extracted_data, categories[0])

    transformed = ct.transform_carts_data(extracted_data)

    carts = load(transformed, categories[0])
    carts_save_data_to_csv = f.save_as_csv(carts, categories[0])
    carts_data_loading = ld.load_data(carts, "carts")

    load_carts_products = load(transformed, categories[1])
    carts_products = cpt.transform_carts_products_data(load_carts_products)
    carts_products_save_data_to_csv = f.save_as_csv(carts_products, categories[1])
    carts_products_data_loading = ld.load_data(carts_products, "cart_contents")


    extracted_data >> save_data_to_json >> transformed >> carts >> carts_save_data_to_csv >> carts_data_loading >> load_carts_products >> carts_products >> carts_products_save_data_to_csv >> carts_products_data_loading
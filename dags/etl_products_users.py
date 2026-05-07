from airflow import DAG
from airflow.decorators import task
import etl.extract as ex
import etl.transform.products_transform as pt
import etl.transform.products_reviews_transform as prt
import etl.transform.products_tags_transform as ptt
import etl.transform.users_transform as ut
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
    dag_id = "etl_products_users",
    default_args = default_args,
    description = "DAG that initializes etl pipeline for products and users",
    schedule = "@daily",
    catchup = False
) as etl_pipeline:
    p_cat = ["products", "products_reviews", "products_tags"]

    p_extracted_data = ex.get_data(p_cat[0])
    p_save_json = f.save_to_json(p_extracted_data, p_cat[0])

    transformed = pt.transform_product_data(p_extracted_data)
    products = load(transformed, p_cat[0])
    reviews = load(transformed, p_cat[1])
    tags = load(transformed, p_cat[2])
    r_trans = prt.transform_reviews_data(reviews)
    t_trans = ptt.transform_tags_data(tags)
    p_save_csv = f.save_as_csv(products, p_cat[0])
    r_save_csv = f.save_as_csv(r_trans, p_cat[1])
    t_save_csv = f.save_as_csv(t_trans, p_cat[2])
    
    p_load = ld.load_data(products, p_cat[0])
    r_load = ld.load_data(r_trans, p_cat[1])
    t_load = ld.load_data(t_trans, p_cat[2])

    u_extracted_data = ex.get_data("users")
    u_save_json = f.save_to_json(u_extracted_data, "users")

    users = ut.transform_users_data(u_extracted_data)
    u_save_csv = f.save_as_csv(users, "users")

    u_load = ld.load_data(users, "users")

    # carts = load(transformed, categories[0])
    # carts_save_data_to_csv = f.save_as_csv(carts, categories[0])
    # carts_data_loading = ld.load_data(carts, "carts")

    # load_carts_products = load(transformed, categories[1])
    # carts_products = cpt.transform_carts_products_data(load_carts_products)
    # carts_products_save_data_to_csv = f.save_as_csv(carts_products, categories[1])
    # carts_products_data_loading = ld.load_data(carts_products, "carts_contents")

    p_extracted_data >> p_save_json >> transformed >> products >> reviews >> tags >> r_trans >> t_trans >> p_save_csv >> r_save_csv >> t_save_csv >> p_load >> r_load >> t_load
    u_extracted_data >> u_save_json >> users >> u_save_csv >> u_load


# with DAG (
#     dag_id = "etl_pipeline",
#     default_args = default_args,
#     description = "DAG that initializes etl pipeline",
#     schedule = "@daily",
#     catchup = False
# ) as etl_pipeline:
#     extracted_data = get_data("products")
#     save_data_to_json = save_to_json(extracted_data, "products")

#     read_data = read_file("products")
#     transformed_data = transform_product_data(read_data)
#     new_transformed_data = products_summary(transformed_data)
#     save_data_to_csv = save_as_csv(new_transformed_data, "products")

#     data_loading = load_data("products")

#     extracted_data >> save_data_to_json >> read_data >> transformed_data >> new_transformed_data >> save_data_to_csv >> data_loading

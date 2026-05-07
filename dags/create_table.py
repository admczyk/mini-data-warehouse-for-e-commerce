from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from airflow import DAG
import pendulum
from datetime import datetime, timedelta

@task
def create_table(category):
    try:
        hook = PostgresHook(postgres_conn_id="postgres_db_etl")
        conn = hook.get_conn()
        cursor = conn.cursor()

        with open(f"/opt/airflow/sql/create/create_{category}_table.sql", "r") as file:
            query = file.read()

        cursor.execute(query)

        conn.commit()
        return True
    except Exception as e:
        raise e
    finally:
        cursor.close()
        conn.close()

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

with DAG (
    dag_id = "create_tables",
    default_args = default_args,
    description = "DAG that executes creating tables in DB",
    schedule = None,
    catchup = False
) as create_products_table:
    create_products_table_task = create_table("products")
    create_users_table_task = create_table("users")
    create_carts_table_task = create_table("carts")
    create_products_reviews_table_task = create_table("products_reviews")
    create_products_tags_table_task = create_table("products_tags")
    create_carts_contents_table_task = create_table("carts_contents")

    create_products_table_task >> create_users_table_task >> create_carts_table_task >> create_products_reviews_table_task >> create_products_tags_table_task >> create_carts_contents_table_task


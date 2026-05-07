from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values
from airflow.decorators import task

@task
def load_data(data, category):
    # data = read_csv(category)
    # print("hi")
    hook = PostgresHook(postgres_conn_id="postgres_db_etl")
    conn = hook.get_conn()
    cursor = conn.cursor()

    try:
        values = data[data.columns].values.tolist()

        with open(f"./sql/insert/insert_{category}.sql", "r") as file:
            query = file.read()
            print(query)
        
        execute_values(cursor, query, values)
        conn.commit()

    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()
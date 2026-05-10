# Mini Data Warehouse For E-commerce with Apache Airflow
[![en](https://img.shields.io/badge/lang-en-green.svg)](https://github.com/admczyk/Mini-Data-Warehouse-for-E-commerce-With-Apache-Airflow/blob/main/README.md)
[![pl](https://img.shields.io/badge/lang-pl-red.svg)](https://github.com/admczyk/Mini-Data-Warehouse-for-E-commerce-With-Apache-Airflow/blob/main/README.pl.md)

This project is a complete ETL (Extract, Transform, Load) pipeline that fetches product, carts and users data from the __dummyjson.com__ website, processes it and loads it into a PostgreSQL database.
The pipeline is orchestrated using Apache Airflow.

## Table of Contents
* [Project Overview](https://github.com/admczyk/Mini-Data-Warehouse-for-E-commerce-With-Apache-Airflow#project-overview)
* [Project Structure](https://github.com/admczyk/Mini-Data-Warehouse-for-E-commerce-With-Apache-Airflow#project-structure)
* [Technologies Used](https://github.com/admczyk/Mini-Data-Warehouse-for-E-commerce-With-Apache-Airflow#technologies-used)
* [Data Pipeline Architecture](https://github.com/admczyk/Mini-Data-Warehouse-for-E-commerce-With-Apache-Airflow#data-pipeline-architecture)
    - [ETL Pipeline](https://github.com/admczyk/Mini-Data-Warehouse-for-E-commerce-With-Apache-Airflow#etl-pipeline)
    - [Database Design](https://github.com/admczyk/Mini-Data-Warehouse-for-E-commerce-With-Apache-Airflow#database-design)
    - [Airflow DAGs](https://github.com/admczyk/Mini-Data-Warehouse-for-E-commerce-With-Apache-Airflow#airflow-dags)
* [Data Analysis and Visualization](https://github.com/admczyk/Mini-Data-Warehouse-for-E-commerce-With-Apache-Airflow#data-analysis-and-visualization)
* [Setup Instructions](https://github.com/admczyk/Mini-Data-Warehouse-for-E-commerce-With-Apache-Airflow#setup-instructions)


## Project Overview
This project implements a mini data warehouse for e-commerce data using Apache Airflow.

The pipeline extracts product data from a public API, transforms it into an analytical format, and loads it into a PostgreSQL database.

The dataset includes:
#### Products
- **product information** (product_id, title, description, category, price, discount_percentage, overall_rating, stock, brand, final_price, price_bucket, rating_bucket, value_score)
- **product reviews** (review_id, product_id, review_comment, review_date, rating, review_year, review_month, review_bucket)
- **product tags** (tag_id, product_id, tag)

#### Carts
- **carts information** (cart_id, cart_total, cart_total_discounted, user_id, cart_total_quantity, cart_size_bucket_cart_value_bucket)
- **carts items** (cart_id, product_id, product_price, product_quantity, product_total, product_total_discounted)

#### Users
- **user information** (user_id, first_name, last_name, gender, birth_date, city, state, state_code, postal_code, country, age_group)

the main goal of the project is to simulate a real-world data engineering workflow, including:
- ETL pipeline design
- data cleaning and validation
- feature engineering for analytics
- orchestration with Apache Airflow

An additional objective is to analyse aquired data in order to extract meaningful insights and support data-driven decision making.

## Project Structure
```
mini_data_warehouse/
|
├── dags/                               # Contains Apache Airflow DAGs responsible for orchestrating the workflow
|   ├── create_table.py                     # Initializes database schema and tables
|   ├── etl_carts.py                        # Defines ETL workflow for carts data
|   └── etl_products_users.py               # Defines ETL workflow for products and users data
├── data/                               # Stores data used across the pipeline
|   ├── raw                                 # Raw data extracted from external API (JSON format)
|   └── transformed                         # Cleaned and processed data ready for loading (CSV format)
├── etl/                                # Implements the core ETL logic
|   ├── transform/                          # Handles main logic for data transformation
|   |   ├── carts_products_transform.py     # Transforms items inside carts data
|   |   ├── carts_transform.py              # Transforms carts data
|   |   ├── products_reviews_transform.py   # Transforms product reviews data
|   |   ├── products_tags_transform.py      # Transforms product tag data
|   |   ├── products_transform.py           # Transforms product data
|   |   └── users_transform.py              # Transforms users data
|   ├── extract.py                      # Handles data retrieval from external sources
|   └── load.py                         # Loads processed data into PostgreSQL database
├── sql/                                # Stores SQL queries
|   ├── create/                             # SQL queries responsible for creating data tables 
|   └── insert/                             # SQL queries responsible for inserting data into tables
├── utils/
|   └── files_io.py                         # Utility functions for reading and writing files
├── .env                    # Environmental variables used for configuration
├── docker-compose.yaml     # Defines the containerized environment, includeing Airflow services and PostgreSQL database
├── key_generator.py        # Generates FERNET_KEY and SECRET_KEY
├── requirements.txt        # Lists Python dependencies required to run the project
└── README.md               # Project documentation
```

## Technologies used
- SQL
- PostgreSQL
- Python (ETL scripts)
- Docker
- Git / Github

## Data Pipeline Architecture
### ETL Pipeline
The ETL process consists of:
#### 1. Extract
- Fetches data using HTTP requests
- Retrieves raw data from external API
- Saves raw data as JSON file for traceability

#### 2. Transform
- Data is read from JSON file using Pandas
- Nested fields are flattened
- Normalizes data values 
- Handles missing values and resolves logical inconsistences
- Additional analytical columns are created
- Saves transformed data as CSV for traceability

#### 3. Load
- Data is read from CSV using Pandas
- Connection is managed via Airflow PostgresHook
- Records are inserted row-by-row into the products table

### Database Design
This subcategory is still under construction...

### Airflow DAGs
The project uses Apache Airflow to orchestrate data workflows through Directed Acyclic Graphs (DAGs). Each DAG defines a sequence of tasks and their dependencies, enabling automated and repeatable data processing.
- __create_table.py__ - this DAG is responsible for initializing the database schema
- __etl_carts.py__ - this is one of the main DAGs that implements the end-to-end ETL pipeline for carts data. It is activated every hour.
- __etl_products_users.py__ - this is one of the main DAGs that implements the end-to-end ETL pipeline for products and users data. It is activated once every day.

## Data Analysis and Visualization
There will be more soon...

## Setup Instructions
#### 1. Clone the Repository
```
git clone https://github.com/admczyk/Mini-Data-Warehouse-for-E-commerce-With-Apache-Airflow.git
cd Mini-Data-Warehouse-for-E-commerce-With-Apache-Airflow
```
#### 2. Create a Virtual Environment
Ensure you have Python installed, then create and activate a virtual environment:
```
python -m venv venv
source venv/bin/activate    # On macOS/Linux
venv\Scripts\activate       # On Windows
```
#### 3. Install Dependencies
Install the required Python packages:
```
pip install -r requirements.txt
```
#### 4. Set Up Environment Variables
Create `.env` file in the root directory and assign values:
```
POSTGRES_USERNAME= 
POSTGRES_PASSWORD=
POSTGRES_HOST=
POSTGRES_PORT=
POSTGRES_DATABASE_NAME=

AIRFLOW_UID=                            # 1000 on macOS/Linux or 50000 on Windows
AIRFLOW_WWW_USER_USERNAME=
AIRFLOW_WWW_USER_PASSWORD=
FERNET_KEY=
SECRET_KEY=

WEBSITE_PATH="https://dummyjson.com/"
```
`FERNET_KEY` and `SECRET_KEY` should be generated. To do this run `key_generator.py` file.

#### 5. Build and Start Services
Run Docker compose to start Airflow and PostgreSQL
```
docker compose up -d
```
This command will start:
- Airflow webserver
- Airflow scheduler
- PostgreSQL database

#### 6. Access Airflow UI
Open browser and go to:
```
http://localhost:8080
```
or _(if above doesn't work)_:
```
http://127.0.0.1:8080
```
Login using username and password set in environment variables.

#### 7. Run the Pipeline
1. Enable the DAG `create_product_table`
2. Trigger it manually to create SQL table
3. Enable the DAG `etl_carts` and `etl_products_users`
4. Trigger them manually or wait for the scheduler

#### 8. Verify Results
- Raw data - `data/raw/`
- Transformed data - `data/transformed/`
- Database table:
    ```
    docker exec -it postgres bash

    psql -U YOUR_POSTGRES_USERNAME -d YOUR_POSTGRES_DATABASE

    SELECT * FROM products LIMIT(10);
    ```
### Stopping the Environment
To stop all services use:
```
docker compose down
```
### Reseting the Environment
To remove all data and start fresh use:
```
docker compose down -v
```
## Future Improvements
- Add logging to improve monitoring and debugging
- Integrate BI tools (Power BI / Tableau)
- Deploy pipeline to cloud

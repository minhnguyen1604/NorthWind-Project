from google.cloud import bigquery
import airflow
from airflow.models.dag import DAG
from datetime import timedelta, datetime
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.operators.bash import BashOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from google.cloud import storage
import os
import pandas as pd

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/opt/airflow/dags/northwind-411015-fd607e874924.json'

default_args = {
    'owner': 'tuanminh',
    'start_date': datetime(2023, 12, 29), # YYYY-MM-DD
    'retries': None,
    'retry_daylsy': timedelta(minutes=2)
}

dag = DAG(
    'sql_example',
    default_args=default_args,
    description='Run BigQuery SQL query',
    schedule_interval=None,
)

def read_sql_file(file_path):
  with open(file_path, 'r') as file:
    return file.read()

DimCustomer_sql_query = read_sql_file('/opt/airflow/dags/SQL_query/Create_dim_customer_table.sql')
DimDate_sql_query = read_sql_file('/opt/airflow/dags/SQL_query/Create_dim_date_table.sql')
DimEmployee_sql_query = read_sql_file('/opt/airflow/dags/SQL_query/Create_dim_employee_table.sql')
DimOrdershipinfo_sql_query = read_sql_file('/opt/airflow/dags/SQL_query/Create_dim_ordershipinfo_table.sql')
DimProduct_sql_query = read_sql_file('/opt/airflow/dags/SQL_query/Create_dim_product_table.sql')
DimSupplier_sql_query = read_sql_file('/opt/airflow/dags/SQL_query/Create_dim_supplier_table.sql')
FactOrder_sql_query = read_sql_file('/opt/airflow/dags/SQL_query/Create_fact_order_table.sql')

create_update_fact_order = BigQueryOperator(
    task_id='create_update_fact_order',
    sql= FactOrder_sql_query,
    use_legacy_sql=False,
    dag=dag,
)
t1 = create_update_fact_order

create_update_dim_date = BigQueryOperator(
    task_id='create_update_dim_date',
    sql= DimDate_sql_query,
    use_legacy_sql=False,
    dag=dag,
)
t2 = create_update_dim_date

create_update_dim_customer = BigQueryOperator(
    task_id='create_update_dim_customer',
    sql= DimCustomer_sql_query,
    use_legacy_sql=False,
    dag=dag,
)
t3 = create_update_dim_customer

create_update_dim_product = BigQueryOperator(
    task_id='create_update_dim_product',
    sql= DimProduct_sql_query,
    use_legacy_sql=False,
    dag=dag,
)
t4 = create_update_dim_product

create_update_dim_employee = BigQueryOperator(
    task_id='create_update_dim_employee',
    sql= DimEmployee_sql_query,
    use_legacy_sql=False,
    dag=dag,
)
t5 = create_update_dim_employee

create_update_dim_ordershipinfo = BigQueryOperator(
    task_id='create_update_dim_ordershipinfo',
    sql= DimOrdershipinfo_sql_query,
    use_legacy_sql=False,
    dag=dag,
)
t6 = create_update_dim_ordershipinfo

create_update_dim_supplier = BigQueryOperator(
    task_id='create_update_dim_supplier',
    sql= DimSupplier_sql_query,
    use_legacy_sql=False,
    dag=dag,
)
t7 = create_update_dim_supplier

TaskDelay = BashOperator(task_id="delay_bash_task",
                         dag=dag,
                         bash_command="sleep 5s")

t1 >> t2 >> TaskDelay >> t3
t3 >> t4 >> t5 >> t6 >> t7
[t1, t2] >> t4

if __name__ == "__main__":
    dag.cli()

from airflow import DAG
from datetime import datetime
from airflow.operators.python_operator import PythonOperator
from common.spark_postgres_to_bronze import load_postgres_to_bronze
from common.spark_bronze_to_silver import load_postgres_bronze_to_silver
from common.spark_bronze_to_silver import load_bronze_dshop_orders_to_silver
from common.spark_silver_to_dwh import *

dag = DAG(
    dag_id='postgres_data_pipeline'
    , start_date=datetime(2021, 11, 15, 9)
    , schedule_interval='@daily'
)

load_to_bronze_table_aisles = PythonOperator(
    task_id=f'load_to_bronze_table_aisles',
    dag=dag,
    python_callable=load_postgres_to_bronze,
    provide_context=True,
    op_kwargs={'table':'aisles', 'postgres_conn_id':'dshop'},
)

load_to_bronze_table_clients = PythonOperator(
    task_id=f'load_to_bronze_table_clients',
    dag=dag,
    python_callable=load_postgres_to_bronze,
    provide_context=True,
    op_kwargs={'table':'clients', 'postgres_conn_id':'dshop'},
)

load_to_bronze_table_departments = PythonOperator(
    task_id=f'load_to_bronze_table_departments',
    dag=dag,
    python_callable=load_postgres_to_bronze,
    provide_context=True,
    op_kwargs={'table':'departments', 'postgres_conn_id':'dshop'},
)

load_to_bronze_table_orders = PythonOperator(
    task_id=f'load_to_bronze_table_orders',
    dag=dag,
    python_callable=load_postgres_to_bronze,
    provide_context=True,
    op_kwargs={'table':'orders', 'postgres_conn_id':'dshop'},
)

load_to_bronze_table_products = PythonOperator(
    task_id=f'load_to_bronze_table_products',
    dag=dag,
    python_callable=load_postgres_to_bronze,
    provide_context=True,
    op_kwargs={'table':'products', 'postgres_conn_id':'dshop'},
)





load_to_silver_table_aisles = PythonOperator(
    task_id=f'load_to_silver_table_aisles',
    dag=dag,
    python_callable=load_postgres_bronze_to_silver,
    provide_context=True,
    op_kwargs={'table':'aisles', 'project':'dshop'},
)

load_to_silver_table_clients = PythonOperator(
    task_id=f'load_to_silver_table_clients',
    dag=dag,
    python_callable=load_postgres_bronze_to_silver,
    provide_context=True,
    op_kwargs={'table':'clients', 'project':'dshop'},
)

load_to_silver_table_departments = PythonOperator(
    task_id=f'load_to_silver_table_departments',
    dag=dag,
    python_callable=load_postgres_bronze_to_silver,
    provide_context=True,
    op_kwargs={'table':'departments', 'project':'dshop'},
)

load_to_silver_table_products = PythonOperator(
    task_id=f'load_to_silver_table_products',
    dag=dag,
    python_callable=load_postgres_bronze_to_silver,
    provide_context=True,
    op_kwargs={'table':'products', 'project':'dshop'},
)

load_to_silver_table_orders = PythonOperator(
    task_id=f'load_to_silver_table_orders',
    dag=dag,
    python_callable=load_bronze_dshop_orders_to_silver,
    provide_context=True,
)


load_to_dwh_dimension_clients = PythonOperator(
    task_id=f'load_to_dwh_dimension_clients',
    dag=dag,
    python_callable=load_dim_clients_to_dwh,
    provide_context=True,
)

load_to_dwh_dimension_products = PythonOperator(
    task_id=f'load_to_dwh_dimension_products',
    dag=dag,
    python_callable=load_dim_products_to_dwh,
    provide_context=True,
)

load_to_dwh_dimension_date = PythonOperator(
    task_id=f'load_to_dwh_dimension_date',
    dag=dag,
    python_callable=load_dim_date_to_dwh,
    provide_context=True,
)

load_to_dwh_fact_orders = PythonOperator(
    task_id=f'load_to_dwh_fact_orders',
    dag=dag,
    python_callable=load_fact_orders_to_dwh,
    provide_context=True,
)

load_to_dwh_fact_out_of_stock = PythonOperator(
    task_id=f'load_to_dwh_fact_out_of_stock',
    dag=dag,
    python_callable=load_fact_out_of_stock_to_dwh,
    provide_context=True,
)

load_to_bronze_table_aisles >> load_to_silver_table_aisles >> load_to_dwh_dimension_clients >> load_to_dwh_fact_orders
load_to_bronze_table_clients >> load_to_silver_table_clients >> load_to_dwh_dimension_clients
load_to_bronze_table_departments >> load_to_silver_table_departments >> load_to_dwh_dimension_clients
load_to_bronze_table_orders >> load_to_silver_table_products >> load_to_dwh_dimension_products

load_to_bronze_table_products >> load_to_silver_table_orders

load_to_dwh_dimension_date >> load_to_dwh_fact_orders
load_to_dwh_dimension_date >> load_to_dwh_fact_out_of_stock
load_to_dwh_dimension_products >> load_to_dwh_fact_orders
load_to_dwh_dimension_products >> load_to_dwh_fact_out_of_stock
from airflow import DAG
from datetime import datetime
from airflow.operators.python_operator import PythonOperator
from common.spark_postgres_to_bronze import load_postgres_to_bronze
from common.spark_bronze_to_silver import load_postgres_bronze_to_silver
from common.spark_bronze_to_silver import load_bronze_dshop_orders_to_silver

dag = DAG(
    dag_id='postgres_data_pipeline'
    , start_date=datetime(2021, 11, 9)
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

load_to_bronze_table_aisles >> load_to_silver_table_aisles
load_to_bronze_table_clients >> load_to_silver_table_clients
load_to_bronze_table_departments >> load_to_silver_table_departments
load_to_bronze_table_orders >> load_to_silver_table_products
load_to_bronze_table_products >> load_to_silver_table_orders
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from airflow.hooks.base_hook import BaseHook
import logging

def load_dim_clients_to_dwh(*args, **kwargs):

    dimension = "dim_client"    

    logging.info(f"Load dimension: {dimension} for data mart: sales")

    logging.info(f"Try initialize spark session")
    spark = SparkSession.builder\
        .config('spark.driver.extraClassPath', '/home/user/shared/postgresql-42.3.1.jar')\
        .master('local')\
        .appName('sales_to_dwh')\
        .getOrCreate()  
    logging.info(f"Successfully initialized spark session: {spark}")  

    logging.info(f"Begin get DWH database connection info from connection config")
    gp_conn = BaseHook.get_connection('DWH')
    gp_url = f"jdbc:postgresql://{gp_conn.host}:{gp_conn.port}/{gp_conn.schema}"
    gp_creds = {"user":gp_conn.login, "password":gp_conn.password}
    logging.info(f"Successfully get DWH database connection info from connection config")

    table = "clients"
    data_path = "/silver/dshop/"
    data_path = data_path + table

    logging.info(f"Begin of read data from table: {table} from {data_path} to spark dataframe")
    df_silver_clients = spark.read.parquet(data_path)
    logging.info(f"End of read data from table: {table} from {data_path} to spark dataframe")
    logging.info(f"Dataframe size is: {df_silver_clients.count()} rows")

    logging.info(f"Begin of prepare data for dimension: {dimension}")
    df_dim_clients = df_silver_clients\
        .select('client_id', F.col('fullname').alias('client_name'))
    logging.info(f"End of prepare data for dimension: {dimension}")
    logging.info(f"Dataframe size is: {df_dim_clients.count()} rows")

    logging.info(f"Begin of write dimension: {dimension}")
    df_dim_clients.write.jdbc(gp_url, table = 'dim_clients', properties = gp_creds, mode='overwrite')
    logging.info(f"End of write dimension: {dimension}")
    logging.info(f"Writed {df_dim_clients.count()} rows")

def load_dim_products_to_dwh(*args, **kwargs):
  
    dimension = "dim_products" 

    logging.info(f"Load dimension: {dimension} for data mart: sales")

    logging.info(f"Try initialize spark session")
    spark = SparkSession.builder\
        .config('spark.driver.extraClassPath', '/home/user/shared/postgresql-42.3.1.jar')\
        .master('local')\
        .appName('sales_to_dwh')\
        .getOrCreate()  
    logging.info(f"Successfully initialized spark session: {spark}") 

    logging.info(f"Begin get DWH database connection info from connection config")
    gp_conn = BaseHook.get_connection('DWH')
    gp_url = f"jdbc:postgresql://{gp_conn.host}:{gp_conn.port}/{gp_conn.schema}"
    gp_creds = {"user":gp_conn.login, "password":gp_conn.password}
    logging.info(f"Successfully get DWH database connection info from connection config")

    table = "aisles"
    data_path = "/silver/dshop/"
    data_path = data_path + table

    logging.info(f"Begin of read data from table: {table} from {data_path} to spark dataframe")
    df_silver_aisles = spark.read.parquet(data_path)
    logging.info(f"End of read data from table: {table} from {data_path} to spark dataframe")
    logging.info(f"Dataframe size is: {df_silver_aisles.count()} rows")    

    table = "departments"
    data_path = "/silver/dshop/"
    data_path = data_path + table

    logging.info(f"Begin of read data from table: {table} from {data_path} to spark dataframe")
    df_silver_departments = spark.read.parquet(data_path)
    logging.info(f"End of read data from table: {table} from {data_path} to spark dataframe")
    logging.info(f"Dataframe size is: {df_silver_departments.count()} rows")   

    table = "products"
    data_path = "/silver/dshop/"
    data_path = data_path + table

    logging.info(f"Begin of read data from table: {table} from {data_path} to spark dataframe")
    df_silver_products = spark.read.parquet(data_path)
    logging.info(f"End of read data from table: {table} from {data_path} to spark dataframe")
    logging.info(f"Dataframe size is: {df_silver_products.count()} rows")       

    logging.info(f"Begin of prepare data for dimension: {dimension}")
    df_dim_products = df_silver_products\
        .join(df_silver_aisles, 'aisle_id', 'left')\
        .join(df_silver_departments, 'department_id', 'left')\
        .select('product_id', 'product_name', 'aisle', 'department')
    logging.info(f"End of prepare data for dimension: {dimension}")
    logging.info(f"Dataframe size is: {df_dim_products.count()} rows")

    logging.info(f"Begin of write dimension: {dimension}")
    df_dim_products.write.jdbc(gp_url, table = 'dim_products', properties = gp_creds, mode='overwrite')
    logging.info(f"End of write dimension: {dimension}")
    logging.info(f"Writed {df_dim_products.count()} rows")

def load_dim_date_to_dwh(*args, **kwargs):
  
    dimension = "dim_date" 

    logging.info(f"Load dimension: {dimension} for data mart: sales")

    logging.info(f"Try initialize spark session")
    spark = SparkSession.builder\
        .config('spark.driver.extraClassPath', '/home/user/shared/postgresql-42.3.1.jar')\
        .master('local')\
        .appName('sales_to_dwh')\
        .getOrCreate()  
    logging.info(f"Successfully initialized spark session: {spark}") 

    logging.info(f"Begin get DWH database connection info from connection config")
    gp_conn = BaseHook.get_connection('DWH')
    gp_url = f"jdbc:postgresql://{gp_conn.host}:{gp_conn.port}/{gp_conn.schema}"
    gp_creds = {"user":gp_conn.login, "password":gp_conn.password}
    logging.info(f"Successfully get DWH database connection info from connection config")

    logging.info(f"Begin of prepare data for dimension: {dimension}")
    df_period = spark.createDataFrame(["2000-01-01"], "string").toDF("start")
    df_period = df_period.withColumn("stop", F.current_date())
    start, stop = df_period.select([F.col(c).cast("timestamp").cast("long") for c in ("start", "stop")])\
        .first()
    df_dim_date = spark.range(start, stop, 24*60*60)\
        .select(F.col("id").cast("timestamp").cast("date").alias("date"))\
        .withColumn("year", F.year("date"))\
        .withColumn("month", F.month("date"))\
        .withColumn("day", F.dayofmonth("date"))\
        .withColumn("day_of_week", F.dayofweek("date"))\
        .withColumn("day_of_year", F.dayofyear("date"))
    logging.info(f"End of prepare data for dimension: {dimension}")
    logging.info(f"Dataframe size is: {df_dim_date.count()} rows")

    logging.info(f"Begin of write dimension: {dimension}")
    df_dim_date.write.jdbc(gp_url, table = 'dim_date', properties = gp_creds, mode='overwrite')
    logging.info(f"End of write dimension: {dimension}")
    logging.info(f"Writed {df_dim_date.count()} rows")

def load_fact_orders_to_dwh(*args, **kwargs):

    fact = "fact_orders"

    logging.info(f"Load fact: {fact} for data mart: sales")

    logging.info(f"Try initialize spark session")
    spark = SparkSession.builder\
        .config('spark.driver.extraClassPath', '/home/user/shared/postgresql-42.3.1.jar')\
        .master('local')\
        .appName('sales_to_dwh')\
        .getOrCreate()  
    logging.info(f"Successfully initialized spark session: {spark}") 

    logging.info(f"Begin get DWH database connection info from connection config")
    gp_conn = BaseHook.get_connection('DWH')
    gp_url = f"jdbc:postgresql://{gp_conn.host}:{gp_conn.port}/{gp_conn.schema}"
    gp_creds = {"user":gp_conn.login, "password":gp_conn.password}
    logging.info(f"Successfully get DWH database connection info from connection config")

    table = "orders"
    data_path = "/silver/dshop/"
    data_path = data_path + table
    logging.info(f"Begin of read data from table: {table} from {data_path} to spark dataframe")
    df_silver_orders = spark.read.parquet(data_path)
    logging.info(f"End of read data from table: {table} from {data_path} to spark dataframe")  
    logging.info(f"Dataframe size is: {df_silver_orders.count()} rows")

    logging.info(f"Begin of prepare data for fact: {fact}")
    df_fact_orders = df_silver_orders\
        .select('order_id', 'product_id', 'client_id', 'order_date', 'quantity')
    logging.info(f"End of prepare data for dimension: {fact}")
    logging.info(f"Dataframe size is: {df_fact_orders.count()} rows") 

    logging.info(f"Begin of write fact: {fact}")
    df_fact_orders.write.jdbc(gp_url, table = 'fact_orders', properties = gp_creds, mode='overwrite')
    logging.info(f"End of write fact: {fact}")
    logging.info(f"Writed {df_fact_orders.count()} rows") 

    #read actual clients from DB
    dimension = "dim_clients"
    logging.info(f"Begin of read data from dimension: {dimension} from DWH")
    df_dim_clients = spark.read.jdbc(gp_url, table = 'dim_clients', properties = gp_creds)
    logging.info(f"End of read data from dimension: {dimension} from DWH")
    logging.info(f"Dataframe size is: {df_dim_clients.count()} rows")

    
    logging.info(f"Begin of found missing values for dimension: {dimension}")
    df_missing_dim_clients = df_fact_orders\
        .join(df_dim_clients, 'client_id', 'left_anti')\
        .groupby('client_id')\
        .count()\
        .withColumn('client_name', F.lit('Unknown'))\
        .select('client_id', 'client_name')
    logging.info(f"End of found missing values for dimension: {dimension}")
    logging.info(f"Dataframe size is: {df_missing_dim_clients.count()} rows")

    if df_missing_dim_clients.count() > 0:
        logging.info(f"Begin of write dimension: {dimension}")
        df_missing_dim_clients.write.jdbc(gp_url, table = 'dim_clients', properties = gp_creds, mode='append')
        logging.info(f"End of write dimension: {dimension}")
        logging.info(f"Writed {df_missing_dim_clients.count()} rows")

    #read actual products from DB
    dimension = "dim_products"    
    logging.info(f"Begin of read data from dimension: {dimension} from DWH")    
    df_dim_products = spark.read.jdbc(gp_url, table = 'dim_products', properties = gp_creds)
    logging.info(f"End of read data from dimension: {dimension} from DWH")
    logging.info(f"Dataframe size is: {df_dim_products.count()} rows")    

    logging.info(f"Begin of found missing values for dimension: {dimension}")
    df_missing_dim_products = df_fact_orders\
        .join(df_dim_products, 'product_id', 'left_anti')\
        .groupby('product_id')\
        .count()\
        .withColumn('product_name', F.lit('Unknown'))\
        .withColumn('aisle', F.lit('Unknown'))\
        .withColumn('department', F.lit('Unknown'))\
        .select('product_id', 'product_name', 'aisle', 'department')
    logging.info(f"End of found missing values for dimension: {dimension}")
    logging.info(f"Dataframe size is: {df_missing_dim_products.count()} rows")

    if df_missing_dim_products.count() > 0:
        logging.info(f"Begin of write dimension: {dimension}")
        df_missing_dim_products.write.jdbc(gp_url, table = 'dim_products', properties = gp_creds, mode='append')
        logging.info(f"End of write dimension: {dimension}")
        logging.info(f"Writed {df_missing_dim_products.count()} rows")

def load_fact_out_of_stock_to_dwh(*args, **kwargs):

    fact = "fact_out_of_stock"

    logging.info(f"Load fact: {fact} for data mart: sales")

    logging.info(f"Try initialize spark session")
    spark = SparkSession.builder\
        .config('spark.driver.extraClassPath', '/home/user/shared/postgresql-42.3.1.jar')\
        .master('local')\
        .appName('sales_to_dwh')\
        .getOrCreate()  
    logging.info(f"Successfully initialized spark session: {spark}")     

    logging.info(f"Begin get DWH database connection info from connection config")
    gp_conn = BaseHook.get_connection('DWH')
    gp_url = f"jdbc:postgresql://{gp_conn.host}:{gp_conn.port}/{gp_conn.schema}"
    gp_creds = {"user":gp_conn.login, "password":gp_conn.password}
    logging.info(f"Successfully get DWH database connection info from connection config")  

    table = "out_of_stock_app"
    data_path = "/silver/"
    data_path = data_path + table
    logging.info(f"Begin of read data from table: {table} from {data_path} to spark dataframe")
    df_silver_out_of_stock_app = spark.read.parquet(data_path)
    logging.info(f"End of read data from table: {table} from {data_path} to spark dataframe") 
    logging.info(f"Dataframe size is: {df_silver_out_of_stock_app.count()} rows")     

    logging.info(f"Begin of prepare data for fact: {fact}")
    df_fact_out_of_stock = df_silver_out_of_stock_app
    logging.info(f"End of prepare data for dimension: {fact}")
    logging.info(f"Dataframe size is: {df_fact_out_of_stock.count()} rows")    

    logging.info(f"Begin of write fact: {fact}")
    df_fact_out_of_stock.write.jdbc(gp_url, table = 'fact_out_of_stock', properties = gp_creds, mode='overwrite')
    logging.info(f"End of write fact: {fact}")
    logging.info(f"Writed {df_fact_out_of_stock.count()} rows")    

    #read actual products from DB
    dimension = "dim_products"
    logging.info(f"Begin of read data from dimension: {dimension} from DWH") 
    df_dim_products = spark.read.jdbc(gp_url, table = 'dim_products', properties = gp_creds)
    logging.info(f"End of read data from dimension: {dimension} from DWH")
    logging.info(f"Dataframe size is: {df_dim_products.count()} rows")    

    logging.info(f"Begin of found missing values for dimension: {dimension}")
    df_missing_dim_products = df_fact_out_of_stock\
        .join(df_dim_products, 'product_id', 'left_anti')\
        .groupby('product_id')\
        .count()\
        .withColumn('product_name', F.lit('Unknown'))\
        .withColumn('aisle', F.lit('Unknown'))\
        .withColumn('department', F.lit('Unknown'))\
        .select('product_id', 'product_name', 'aisle', 'department')
    logging.info(f"End of found missing values for dimension: {dimension}")
    logging.info(f"Dataframe size is: {df_missing_dim_products.count()} rows")

    if df_missing_dim_products.count() > 0:
        logging.info(f"Begin of write dimension: {dimension}")
        df_missing_dim_products.write.jdbc(gp_url, table = 'dim_products', properties = gp_creds, mode='append')
        logging.info(f"End of write dimension: {dimension}")
        logging.info(f"Writed {df_missing_dim_products.count()} rows")
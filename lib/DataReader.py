from lib import ConfigReader

#defining customer schema
def get_customer_schema():
    schema="customer_id int,customer_fname string,customer_lname string,username string,password string,address string,city string,state string,pincode string"
    return schema 

#creating customer dataframe
def read_customers(spark,env):
    conf = ConfigReader.get_app_config(env)
    customers_file_path = conf["customers.file.path"]
    return spark.read \
    .format("csv") \
    .option("header","true") \
    .schema(get_customer_schema()) \
    .load(customers_file_path)

#defining order schema
def get_order_schema():
    schema = "order_id int,order_date string,customer_id int,order_status string"
    return schema

def read_orders(spark,env):
    conf = ConfigReader.get_app_config(env)
    orders_file_path = conf["orders.file.path"]
    return spark.read \
    .format("csv") \
    .option("header","true") \
    .schema(get_order_schema()) \
    .load(orders_file_path)
import sys
from lib import Utils, DataReader, DataManipulation
from pyspark.sql.functions import *
from lib.Logger import Log4j

if __name__ == '__main__':

    if len(sys.argv)<2:
        print("Please enter the env")
        sys.exit(-1)

    job_run_env = sys.argv[1]
    print(job_run_env)

    print("create spark session")

    spark = Utils.get_spark_session(job_run_env)
    #sc = spark.sparkContext
    #sc.setLogLevel("INFO")
    logger = Log4j(spark)

    logger.info("spark session created")

    customers_df = DataReader.read_customers(spark,job_run_env)

    orders_df = DataReader.read_orders(spark,job_run_env)

    filterd_order_df = DataManipulation.filter_closed_orders(orders_df)

    joined_df = DataManipulation.join_orders_customers(filterd_order_df,customers_df)

    agg_df = DataManipulation.count_orders_state(joined_df)

    print(agg_df.show())

    logger.info("end of main")
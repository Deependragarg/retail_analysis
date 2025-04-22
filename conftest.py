import pytest
from lib.Utils import get_spark_session

@pytest.fixture
def spark():
    "create spark session"
    spark_session = get_spark_session("LOCAL")
    yield spark_session
    spark_session.stop()

@pytest.fixture
def expected_result(spark):
    ex_schema = "state string, count int"
    csv_file_path = r"E:\Learnings\The Ultimate Big Data\Projects\RetailAnalysis\data\test_results\state_aggregate.csv"
    return spark.read \
    .format("csv") \
    .option("header","true") \
    .schema(ex_schema) \
    .load(csv_file_path)
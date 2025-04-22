import pytest
from lib.DataReader import read_customers, read_orders
from lib.DataManipulation import filter_closed_orders, count_orders_state, filter_order_generic
from lib.ConfigReader import get_app_config

@pytest.mark.skip("work in process")
def test_read_customers_df(spark):
    customers_count = read_customers(spark,"LOCAL").count()
    assert customers_count == 12435

@pytest.mark.skip("work in process")
def test_read_orders_df(spark):
    orders_count = read_orders(spark,"LOCAL").count()
    assert orders_count == 68884

@pytest.mark.skip("work in process")
def test_filter_closed_orders(spark):
    orders_df = read_orders(spark,"LOCAL")
    fitlered_count = filter_closed_orders(orders_df).count()
    assert fitlered_count == 7556

@pytest.mark.skip("work in process")
def test_read_app_config(spark):
    config = get_app_config('LOCAL')
    assert config['orders.file.path'] == "E:\Learnings\The Ultimate Big Data\Projects\RetailAnalysis\data\orders.csv"

@pytest.mark.skip("work in process")
def test_read_order_state(spark,expected_result):
    customers_df = read_customers(spark,"LOCAL")
    customer_state = count_orders_state(customers_df)
    assert customer_state.collect() == expected_result.collect()

@pytest.mark.skip("work in process")
def test_filter_order_generic(spark):
    orders_df = read_orders(spark,"LOCAL")
    fitlered_count = filter_order_generic(orders_df,'CLOSED').count()
    assert fitlered_count == 7556

@pytest.mark.parametrize(
        "status,count",
        [("COMPLETE",22900),
         ("CLOSED",7556),
         ("PENDING_PAYMENT",15030)]
)

@pytest.mark.latest()
def test_filter_count_status(spark,status,count):
    orders_df = read_orders(spark,"LOCAL")
    fitlered_count = filter_order_generic(orders_df,status).count()
    assert fitlered_count == count
    
from python.libraries import snowpark as sf
from snowflake.snowpark.window import Window
from snowflake.snowpark.types import StringType
from snowflake.snowpark.functions import udf, sum, col, count, row_number, year, dense_rank

utils = sf.utils()

# Total amount bought from each Manufacturer
def manufacturer_sales():
    window = Window.orderBy(col('TOTAL_TRANSACTION'))
    src_parts_fact = utils.getTable('CVS_CRUSADERS.RETAIL_PROD.PART')
    rpt_mfg_sales = src_parts_fact \
        .groupBy(col('P_MFGR')) \
        .agg(sum(col('P_RETAILPRICE')).alias('TOTAL_TRANSACTION'), row_number().over(window).alias('RANK')) \
        .select(col('P_MFGR').alias('MANUFACTURER'), col('TOTAL_TRANSACTION'), col('RANK'))
    rpt_mfg_sales.show()
    '''select P_MFGR, SUM(P_RETAILPRICE) as TOTAL , ROW_NUMBER() OVER ( ORDER BY TOTAL DESC)FROM "CVS_CRUSADERS"."RETAIL_PROD"."PART" group by P_MFGR;'''
    rpt_mfg_sales.write.mode("overwrite").save_as_table("manufacturer_sales")

# Customer spendings every year Customer#077922590
def customer_spendings():
    src_orders_fact = utils.getTable('CVS_CRUSADERS.RETAIL_PROD.ORDERS')
    cust_dim_query = 'SELECT C_CUSTKEY, C_NAME FROM CVS_CRUSADERS.RETAIL_PROD.CUSTOMER WHERE C_CUSTKEY = 077922590'
    src_cust_dim = utils.getTable(sql=cust_dim_query, isSql=True)
    stg_cust_spend = src_orders_fact \
        .groupBy(col('O_CUSTKEY'), year(col('O_ORDERDATE'))) \
        .agg(sum(col('O_TOTALPRICE')))
    rpt_cust_spend = stg_cust_spend.join(src_cust_dim, stg_cust_spend.col("O_CUSTKEY") == src_cust_dim.col("C_CUSTKEY")) \
        .select(col('C_NAME').alias('NAME'),
                col('YEAR(O_ORDERDATE)').alias('YEAR'),
                col('SUM(O_TOTALPRICE)').alias('SPENDINGS'))
    rpt_cust_spend.show()
    '''select O_CUSTKEY, YEAR(O_ORDERDATE), SUM(O_TOTALPRICE) from "CVS_CRUSADERS"."RETAIL_PROD"."ORDERS" where O_CUSTKEY=130998796 '''
    rpt_cust_spend.write.mode("overwrite").save_as_table("customer_spendings")

# Nation specific customer count
def customer_density():
    src_cust_dim = utils.getTable('CVS_CRUSADERS.RETAIL_PROD.CUSTOMER')
    # df = spark.table(tablename)
    src_nation_dim = utils.getTable('CVS_CRUSADERS.RETAIL_PROD.NATION')

    stg_cust_dens = src_cust_dim \
        .groupBy(col('C_NATIONKEY')) \
        .agg(count(col('C_NATIONKEY')))

    rpt_cust_dens = stg_cust_dens.join(src_nation_dim,
                                       stg_cust_dens.col('C_NATIONKEY') == src_nation_dim.col('N_NATIONKEY')) \
        .select(col('N_NAME').alias('COUNTRY'), col('COUNT(C_NATIONKEY)').alias('COUNT'))
    rpt_cust_dens.show()
    '''select C_NATIONKEY, COUNT(C_NATIONKEY) from "CVS_CRUSADERS"."RETAIL_PROD"."CUSTOMER" group by C_NATIONKEY'''
    rpt_cust_dens.write.mode("overwrite").save_as_table("customer_density")

# Top supplier of every nation
def top_supplier():
    session = utils.getSession()
    src_nation_dim = session.table('CVS_CRUSADERS.RETAIL_PROD.NATION')
    supplier_dim_query = 'SELECT S_SUPPKEY, S_NATIONKEY, S_NAME, S_ACCTBAL FROM CVS_CRUSADERS.RETAIL_PROD.SUPPLIER'
    src_supplier_dim = session.sql(supplier_dim_query)

    @udf(name="custom_udf", input_types=[StringType()], return_type=StringType(), is_permanent=False, replace=True)
    def cleaner(input) -> str:
        return input.lstrip('Supplier#')

    window = Window.partitionBy(col('S_NATIONKEY')).orderBy(col('S_ACCTBAL'))
    stg_supplier_dim = src_supplier_dim \
        .select(col('S_NATIONKEY'),
                col('S_SUPPKEY'),
                col('S_NAME'),
                col('S_ACCTBAL'),
                dense_rank().over(window).alias('RANK')) \
        .filter(col('RANK') == 1)
    rpt_top_supplier = stg_supplier_dim.join(src_nation_dim,
                                             stg_supplier_dim.col('S_NATIONKEY') == src_nation_dim.col(
                                                 'N_NATIONKEY')) \
        .select(col('N_NAME').alias('COUNTRY'),
                col('S_NAME').alias('TOP_SUPPLIER')).withColumn('new', cleaner(col('TOP_SUPPLIER')))
    rpt_top_supplier.show()

    rpt_top_supplier.write.mode("overwrite").save_as_table("top_supplier")

    '''select * from (select S_NATIONKEY,S_SUPPKEY,DENSE_RANK() OVER (PARTITION BY S_NATIONKEY ORDER BY S_ACCTBAL) as S_RANK from  
    "CVS_CRUSADERS"."RETAIL_PROD"."SUPPLIER" ) where S_RANK=1
'''


if __name__ == '__main__': customer_spendings()

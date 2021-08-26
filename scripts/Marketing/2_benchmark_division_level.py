'''
Benchmark (Marketing) Division Level
'''
from __future__ import print_function

from datetime import datetime
import argparse

from pyspark.sql import SparkSession, Window
from pyspark.sql import types as T
from pyspark.sql import functions as F
from pyspark.sql.functions import col, when, lit, count, percent_rank, sum as fsum, pandas_udf, PandasUDFType
from scipy import stats
from slackclient import SlackClient

PATH_PUBLISH = 's3://dsci-eol-data-bucket/prod/publish/'
PROJECTNAME = 'Benchmark'
SCRIPTNAME = 'Marketing_2_division_lvl'

# PARSE PARAM
PARSER = argparse.ArgumentParser("EDA")
PARSER.add_argument("--bucket", '-b', metavar="bucket",
                    type=str, default='dsci-eol-data-bucket',
                    help="s3 bucket for benchmark")
CONF = PARSER.parse_args()
BENCHMARK_BUCKET = CONF.bucket

########################
# SLACK
SLACK_TOKEN = "xoxp-28674668403-481692716498-596758164998-dc313510003e460288a85a43e805c69a"
SLACK_CLIENT = SlackClient(SLACK_TOKEN)
def send_to_slack(message, target_channel="GHQKCTSNN"):
    '''
    Send slack message to #cig-lago-log
    '''
    try:
        message = str(message)
        print(message)
        SLACK_CLIENT.api_call(
            "chat.postMessage",
            channel=target_channel,
            text="%s: %s- %s"%(PROJECTNAME, SCRIPTNAME, message))
    except Exception as general_exception:
        print("cannot send on slack")
        print(general_exception)
########################

def calculate_union_of_kpis(spark):
    '''
    Calculate KPI per Division Category
    '''
    # Initiate timeline we are interested in
    start_month = "2012-01"
    end_month = datetime.now().strftime('%Y-%m')

    # Load BenchmarkRaw
    input_dir = "s3://{}/prod/publish/BenchmarkRaw/"
    benchmark_raw = spark.read\
        .parquet(input_dir.format(BENCHMARK_BUCKET))\
        .filter("NOT type LIKE '%310'")\
        .dropDuplicates()

    # Revenue KPI
    aggregate_key = ['WOmz', 'WOvb']
    kpi_df_revenue = benchmark_raw\
        .toDF(*[c.lower() for c in benchmark_raw.columns])\
        .filter(col('aggregate').isin(*aggregate_key))\
        .groupBy('entryperiod', 'division').agg(fsum('value').alias('value'))\
        .withColumn('value', -1 * col('value'))\
        .withColumn('aggregate', lit("Revenue"))\
        .select('division', 'entryperiod', 'value', 'aggregate')\
        .filter("value IS NOT NULL")

    # Cost KPI
    aggregate_key = ['WPer', 'WBed', 'WKpr', 'WAfs', 'WFbe']
    kpi_df_cost = benchmark_raw\
        .filter(col('aggregate').isin(*aggregate_key))\
        .groupBy('entryperiod', 'division').agg(fsum('value').alias('value'))\
        .withColumn('aggregate', lit("Cost"))\
        .select('division', 'entryperiod', 'value', 'aggregate')\
        .filter("value IS NOT NULL")

    # Profitability KPI
    kpi_df_profitability = kpi_df_cost.withColumnRenamed('value', 'cost').alias('a')\
        .join(kpi_df_revenue.withColumnRenamed('value', 'revenue').alias('b'), ['division', 'entryperiod'], 'inner')\
        .select("a.*", "b.revenue")\
        .withColumn('value', (col('revenue')-col("Cost"))/col('revenue'))\
        .withColumn('aggregate', lit('Profitability'))\
        .select('division', 'entryperiod', 'value', 'aggregate')\
        .filter("value IS NOT NULL")

    # Cash Flow Position KPI
    window = Window\
        .partitionBy(['division', 'aggregate']) \
        .orderBy('entryperiod')\
        .rowsBetween(-10000, 0)
    aggregate_key = ['BLim']
    kpi_df_cash_flow_position = benchmark_raw\
        .filter(col('aggregate').isin(*aggregate_key))\
        .withColumn('aggregate', lit("CashFlowPosition"))\
        .select('division', 'entryperiod', fsum("Value").over(window).alias("Value"), 'aggregate')\
        .filter("value IS NOT NULL")

    # Monthly Cash Flow KPI
    aggregate_key = ['BLim']
    kpi_df_cash_flow_monthly = benchmark_raw\
        .filter(col('aggregate').isin(*aggregate_key))\
        .groupBy('entryperiod', 'division').agg(fsum('value').alias('value'))\
        .withColumn('aggregate', lit('CashFlowMonthly'))\
        .select('division', 'entryperiod', 'value', 'aggregate')\
        .filter("value IS NOT NULL")

    # D2C KPIs
    aggregate_key = ['D2C_30d']
    kpi_df_d2c_30 = benchmark_raw\
        .filter(col('aggregate').isin(*aggregate_key))\
        .withColumn('aggregate', lit('D2C_30d'))\
        .filter("value IS NOT NULL")\
        .select('division', 'entryperiod', 'value', 'aggregate')

    aggregate_key = ['D2C_14d']
    kpi_df_d2c_14 = benchmark_raw\
        .filter(col('aggregate').isin(*aggregate_key))\
        .withColumn('aggregate', lit('D2C_14d'))\
        .filter("value IS NOT NULL")\
        .select('division', 'entryperiod', 'value', 'aggregate')

    aggregate_key = ['D2C_all']
    kpi_df_d2c_all = benchmark_raw\
        .filter(col('aggregate').isin(*aggregate_key))\
        .withColumn('aggregate', lit('D2C_all'))\
        .filter("value IS NOT NULL")\
        .select('division', 'entryperiod', 'value', 'aggregate')

    # UNION all KPIs
    union_of_all_kpis = kpi_df_revenue\
        .union(kpi_df_cost)\
        .union(kpi_df_profitability)\
        .union(kpi_df_cash_flow_position)\
        .union(kpi_df_cash_flow_monthly)\
        .union(kpi_df_d2c_30)\
        .union(kpi_df_d2c_14)\
        .union(kpi_df_d2c_all)\
        .filter((col('entryperiod') >= start_month) & (col('entryperiod') < end_month))

    return union_of_all_kpis

def apply_outlier_detection(spark, all_kpis_df):
    '''
    Apply outlier detection
    '''
    # Join with DivisionLookUp
    div_lookup = spark\
        .read\
        .parquet(*[PATH_PUBLISH + 'DivisionLookUp'])\
        .filter("EuroCurrency = '1'")\
        .filter(col('country').contains('NL'))\
        .withColumn('division', col('division').cast(T.StringType()))\

    all_kpis_df = div_lookup\
        .join(all_kpis_df, ['division'], how='inner')

    to_append = [T.StructField("value_t", T.DoubleType(), True),
                 T.StructField("lambda", T.DoubleType(), True),
                 T.StructField("length", T.DoubleType(), True)]
    schema = T.StructType(all_kpis_df.schema.fields + to_append)

    @pandas_udf(schema, PandasUDFType.GROUPED_MAP)
    def yeojohnson_transform(pdf):
        feat_x = pdf.value
        pdf['value_t'] = stats.yeojohnson(feat_x)[0]
        pdf['lambda'] = stats.yeojohnson(feat_x)[1]
        pdf['length'] = pdf.shape[0]
        return pdf

    condition_1p5iqr = ((col('size_of_group_per_period') < 75) & (col('aggregate').substr(1, 3) != 'D2C')) |\
        (col('sort_value') > (col('q3') + 1.5 * col('iqr'))) |\
        (col('sort_value') < (col('q1') - 1.5 * col('iqr')))

    condition_3p0iqr = ((col('size_of_group_per_period') < 75) & (col('aggregate').substr(1, 3) != 'D2C')) |\
        (col('sort_value') > (col('q3') + 3.0 * col('iqr'))) |\
        (col('sort_value') < (col('q1') - 3.0 * col('iqr')))

    w_partition_group = Window.partitionBy('entryperiod', 'group_id', 'aggregate')
    w_partition_sector = Window.partitionBy('entryperiod', 'sectorcode', 'aggregate')
    is_d2c = (col('aggregate').substr(1, 3) == 'D2C')

    q_1 = when(is_d2c, F.max(when(col('percentile') >= 0.25, None).otherwise(col('sort_value'))).over(w_partition_sector))\
        .otherwise(F.max(when(col('percentile') >= 0.25, None).otherwise(col('sort_value'))).over(w_partition_group))
    q_3 = when(is_d2c, F.min(when(col('percentile') <= 0.75, None).otherwise(col('sort_value'))).over(w_partition_sector))\
        .otherwise(F.min(when(col('percentile') <= 0.75, None).otherwise(col('sort_value'))).over(w_partition_group))

    division_df = all_kpis_df.groupBy('entryperiod', 'sectorcode', 'aggregate').apply(yeojohnson_transform)\
        .withColumn('size_of_group_per_period', count('division').over(w_partition_group))\
        .withColumn('sort_value', when(is_d2c, col('value_t')).otherwise(col('value')))\
        .withColumn('percentile',
                    when(is_d2c, percent_rank().over(w_partition_sector.orderBy('sort_value')))\
                        .otherwise(percent_rank().over(w_partition_group.orderBy('sort_value'))))\
        .withColumn('q1', q_1)\
        .withColumn('q3', q_3)\
        .withColumn('iqr', F.coalesce(col('q3') - col('q1'), lit(0)))\
        .withColumn('is_1point5_iqr_outlier', when(condition_1p5iqr, 'Y').otherwise('N'))\
        .withColumn('is_3point0_iqr_outlier', when(condition_3p0iqr, 'Y').otherwise('N'))

    print("total divisions available:", division_df.select('division').distinct().count())

    return division_df

def main():
    '''
    Main application
    '''
    spark_session = SparkSession\
        .builder\
        .appName("ETL for Benchmark KPI")\
        .getOrCreate()
    try:
        union_of_all_kpis = calculate_union_of_kpis(spark_session).cache()
        print("Size of union_of_all_kpis:", union_of_all_kpis.count())
        division_df_outlier = apply_outlier_detection(spark_session, union_of_all_kpis)

        # Write to S3
        output_dir = "s3://{0}/prod/publish/BenchmarkDivisionLevel/syscreated={1}"
        division_df_outlier\
            .select('division', 'aggregate', "value", 'entryperiod', "is_1point5_iqr_outlier", "is_3point0_iqr_outlier", "percentile",
                    "group_id", "size_of_merged_group", "size_of_group_per_period", "q1", "q3", "iqr")\
            .withColumn("kpi", col('aggregate'))\
            .repartition(1)\
            .write.mode("overwrite")\
            .partitionBy("kpi")\
            .parquet(output_dir.format(BENCHMARK_BUCKET, datetime.now().strftime("%Y-%m-%d")))

    except Exception as general_exception:
        send_to_slack("Error during ETL")
        send_to_slack(general_exception)
        print(1+'a')

    spark_session.stop()

if __name__ == "__main__":
    main()

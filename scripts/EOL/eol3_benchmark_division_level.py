"""
3rd Step of EOL Benchmarking
"""
from __future__ import print_function
import argparse
from datetime import datetime

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.functions import col, lit, when
from slackclient import SlackClient

DIV_LKP_PATH_PUBLISH = 's3://dsci-eol-data-bucket/prod/publish/'
PATH_PUBLISH = 's3://{0}/prod/publish/'
PROJECTNAME = 'Benchmark'
SCRIPTNAME = 'DivisionLevel'

########################
# SLACK
SLACK_TOKEN = "xoxp-28674668403-481692716498-596758164998-dc313510003e460288a85a43e805c69a"
def send_to_slack(message, target_channel="GHQKCTSNN"): # cig-lago-log
    """
    a generic funciton to send messages on a target channel in Slack
    """
    try:
        slackclient = SlackClient(SLACK_TOKEN)
        message = str(message)
        print(message)
        slackclient.api_call(
            "chat.postMessage",
            channel=target_channel,
            text="%s: %s- %s"%(PROJECTNAME, SCRIPTNAME, message))
    except Exception as general_exception:
        print("cannot send on slack")
        print(general_exception)
########################

# Calculate KPI per division
def calculate_eol_division_level(element_df):
    """
    calculate division level statistics for EOL
    """
    dflt = None

    ### 1. Employee Costs
    when_num = (col('cumvalue') > 0) & (col('element') == 'EmployeeCosts-D')
    when_den = (col('cumvalue') < 0) & (col('element') == 'Revenues-C')
    when_ec = (col('numerator') > 0) & (col('denominator') < 0)
    employee_cost = element_df\
        .filter(col('element').isin(*['Revenues-C', 'EmployeeCosts-D']))\
        .withColumn('numerator', when(when_num, col('cumvalue')).otherwise(lit(dflt)))\
        .withColumn('denominator', when(when_den, col('cumvalue')).otherwise(lit(dflt)))\
        .groupby('division', 'entryperiod')\
        .agg(F.sum('numerator').alias('numerator'), F.sum('denominator').alias('denominator'))\
        .withColumn('value', when(when_ec, col('numerator')/col('denominator') * -100).otherwise(lit(dflt)))\
        .withColumn('kpi', lit('employee_costs'))

    ### 2. Receivable Days
    when_num = (col('cumvalue') > 0) & (col('element') == 'AccountReceivables-D')
    when_den = (col('cumvalue') < 0) & (col('element') == 'Revenues-C')
    when_rd = (col('numerator') > 0) & (col('denominator') < 0)
    receivable_days = element_df\
        .filter(col('element').isin(*['Revenues-C', 'AccountReceivables-D']))\
        .withColumn('numerator', when(when_num, col('cumvalue')).otherwise(lit(dflt)))\
        .withColumn('denominator', when(when_den, col('cumvalue')).otherwise(lit(dflt)))\
        .groupby('division', 'entryperiod')\
        .agg(F.sum('numerator').alias('numerator'), F.sum('denominator').alias('denominator'))\
        .withColumn('value', when(when_rd, col('numerator')/col('denominator') * -365).otherwise(lit(dflt)))\
        .withColumn('kpi', lit('receivable_days'))

    ### 3. Gross Margin
    when_den = (col('cumvalue') < 0) & (col('element') == 'Revenues-C')
    when_gm = (col('denominator') < 0)
    gross_margin = element_df\
        .filter(col('element').isin(*['Revenues-C', 'CostOfGoods-D']))\
        .withColumn('numerator', col('cumvalue'))\
        .withColumn('denominator', when(when_den, col('cumvalue')).otherwise(lit(dflt)))\
        .groupby('division', 'entryperiod')\
        .agg(F.sum('numerator').alias('numerator'), F.sum('denominator').alias('denominator'))\
        .withColumn('value', when(when_gm, 100.0*col('numerator')/col('denominator')).otherwise(lit(dflt)))\
        .withColumn('kpi', lit('gross_margin'))

    ### 4. RoE (Return on Equity)
    when_num = (col('element') == 'NetIncome-C')
    when_den = (col('cumvalue') < 0) & (col('element') == 'Equity-C')
    when_roe = (col('denominator') < 0)
    roe = element_df\
        .filter(col('element').isin(*['NetIncome-C', 'Equity-C']))\
        .withColumn('numerator', when(when_num, col('cumvalue')).otherwise(lit(dflt)))\
        .withColumn('denominator', when(when_den, col('cumvalue')).otherwise(lit(dflt)))\
        .groupby('division', 'entryperiod')\
        .agg(F.sum('numerator').alias('numerator'), F.sum('denominator').alias('denominator'))\
        .withColumn('Value', when(when_roe, 100*col('numerator')/col('denominator')).otherwise(lit(dflt)))\
        .withColumn('kpi', lit('roe'))

    ### 5. Inventory Turnover
    when_num = (col('cumvalue') > 0) & (col('element') == 'CostOfGoods-D')
    when_den = (col('cumvalue') > 0) & (col('element') == 'Stock-D')
    when_it = (col('numerator') > 0) & (col('denominator') > 0)
    inventory_turnover = element_df\
        .filter(col('element').isin(*['CostOfGoods-D', 'Stock-D']))\
        .withColumn('numerator', when(when_num, col('cumvalue')).otherwise(lit(dflt)))\
        .withColumn('denominator', when(when_den, col('cumvalue')).otherwise(lit(dflt)))\
        .groupby('division', 'entryperiod')\
        .agg(F.sum('numerator').alias('numerator'), F.sum('denominator').alias('denominator'))\
        .withColumn('value', when(when_it, col('numerator')/col('denominator')).otherwise(lit(dflt)))\
        .withColumn('kpi', lit('inventory_turnover'))

    ### 6. Quick Ratio
    when_num = (col('cumvalue') > 0) | (col('element') == 'Cash-D')
    when_den = (col('cumvalue') < 0) & (col('element') != 'Cash-D')
    when_qr = (col('denominator') != 0)
    quick_ratio = element_df\
        .filter(col('element').isin(*['Cash-D', 'Bank-D', 'AccountReceivables-D', 'TaxToClaim-D', 'OtherReceivables-D',
                                      'Securities-D', 'AccountPayables-C', 'TaxToPay-C', 'OtherDebts-C']))\
        .withColumn('numerator', when(when_num, col('cumvalue')).otherwise(lit(dflt)))\
        .withColumn('denominator', when(when_den, col('cumvalue')).otherwise(lit(dflt)))\
        .groupby('division', 'entryperiod')\
        .agg(F.sum('numerator').alias('numerator'), F.sum('denominator').alias('denominator'))\
        .withColumn('value', when(when_qr, -1*col('numerator')/col('denominator')).otherwise(lit(dflt)))\
        .withColumn('kpi', lit('quick_ratio'))

    ### 7. Current Ratio

    numerator_quick_ratio = quick_ratio\
        .select('division', 'entryperiod', col('numerator').alias('cumvalue'))\
        .withColumn('element', lit('n_quick_ratio'))
    denominator_quick_ratio = quick_ratio\
        .select('division', 'entryperiod', col('denominator').alias('cumvalue'))\
        .withColumn('element', lit('d_quick_ratio'))

    when_num = ((col('element') == 'WorkInProgress-D') & (col('cumvalue') > 0)) | (col('element').isin('n_quick_ratio', 'Stock-D'))
    when_den = ((col('element') == 'WorkInProgress-D') & (col('cumvalue') < 0)) | (col('element').isin('d_quick_ratio'))
    when_cr = col('denominator') != 0
    current_ratio_raw = element_df\
        .filter(col('element').isin(*['Stock-D', 'WorkInProgress-D']))\
        .union(denominator_quick_ratio.select('division', 'entryperiod', 'element', 'cumvalue'))\
        .union(numerator_quick_ratio.select('division', 'entryperiod', 'element', 'cumvalue'))\
        .withColumn('numerator', when(when_num, col('cumvalue')).otherwise(lit(dflt)))\
        .withColumn('denominator', when(when_den, col('cumvalue')).otherwise(lit(dflt)))\
        .groupby('division', 'entryperiod')\
        .agg(F.sum('numerator').alias('numerator'), F.sum('denominator').alias('denominator'))
    current_ratio = current_ratio_raw\
        .withColumn('value', when(when_cr, -1*col('numerator')/col('denominator')).otherwise(lit(dflt)))\
        .withColumn('kpi', lit('current_ratio'))

    ### 8. Solvability
    denominator_current_ratio = current_ratio_raw\
        .select('division', 'entryperiod', col('denominator').alias('cumvalue'))\
        .withColumn('element', lit('d_current_ratio'))
    when_num = (col('cumvalue') < 0) & (col('element') == 'Equity-C')
    when_den = ((col('cumvalue') < 0) & (col('element').isin(*['Provisions-C', 'LongtermDebt-C']))) | (col('element') == 'd_current_ratio')
    when_s = (col('denominator') != 0)
    solvability = element_df\
        .filter(col('element').isin(*['Equity-C', 'Provisions-C', 'LongtermDebt-C']))\
        .union(denominator_current_ratio.select('division', 'entryperiod', 'element', 'cumvalue'))\
        .withColumn('numerator', when(when_num, -1*col('cumvalue')).otherwise(lit(dflt)))\
        .withColumn('denominator', when(when_den, -1*col('cumvalue')).otherwise(lit(dflt)))\
        .groupby('division', 'entryperiod')\
        .agg(F.sum('numerator').alias('numerator'), F.sum('denominator').alias('denominator'))\
        .withColumn('value', when(when_s, 100*col('numerator')/col('denominator')).otherwise(lit(dflt)))\
        .withColumn('kpi', lit('solvability'))

    # Union all kpis
    cols = ['division', 'entryperiod', 'numerator', 'denominator', 'value', 'kpi']
    benchmark_eol_division = employee_cost.select(*cols)\
        .union(receivable_days.select(*cols))\
        .union(gross_margin.select(*cols))\
        .union(roe.select(*cols))\
        .union(inventory_turnover.select(*cols))\
        .union(quick_ratio.select(*cols))\
        .union(current_ratio.select(*cols))\
        .union(solvability.select(*cols))

    return benchmark_eol_division


def apply_outlier_detection(div_lookup, all_kpis_df, element_df):
    """
    a code to detect outliers from a dataframe and add extra columns
    """

    division_df = all_kpis_df\
        .withColumn("ValueIsNull", all_kpis_df.value.isNull())\
        .join(div_lookup, ["division"])

    negative_stock_cash_df = element_df\
        .filter(col('element').isin(*['Stock-D', 'Cash-D']))\
        .filter(col('cumvalue') < -.1)\
        .select("division", "entryperiod")\
        .withColumn("negative_stock_cash", lit("Y"))

    division_df = division_df\
        .join(negative_stock_cash_df, ["division", "entryperiod"], 'left')\
        .withColumn("negative_stock_cash", when(col("negative_stock_cash") == "Y", "Y").otherwise("N"))

    agg_columns = ["group_id", "entryperiod", "aggregate", "ValueIsNull", "negative_stock_cash"]

    # calculate the percentile values per group, the Q1, Q3 and IQR values.
    division_df = division_df\
        .withColumn("percentile", F.percent_rank().over(Window.partitionBy(agg_columns).orderBy("value")))
    division_iqr1 = division_df\
        .filter("percentile < 0.25")\
        .groupBy(agg_columns)\
        .agg(F.max('value').alias("Q1"))\
        .withColumn('aggregate', col('aggregate'))
    division_iqr2 = division_df\
        .filter("percentile > 0.75")\
        .groupBy(agg_columns)\
        .agg(F.min('value').alias("Q3"))\
        .withColumn('aggregate', col('aggregate'))

    division_iqr = division_iqr1.join(division_iqr2, agg_columns, how="outer")
    division_iqr = division_iqr.withColumn("Q3", when(col("Q3").isNull(), (col("Q1"))).otherwise(col("Q3")))
    division_iqr = division_iqr.withColumn("IQR", division_iqr.Q3 - division_iqr.Q1)

    division_df_outlier = division_df.join(division_iqr, agg_columns, how='left')

    division_df_outlier_groups = division_df_outlier.groupBy(agg_columns).count()
    division_df_outlier_groups = division_df_outlier_groups\
        .withColumnRenamed("count", "size_of_group_per_period")\
        .withColumn('aggregate', col('aggregate'))
    division_df_outlier = division_df_outlier.join(division_df_outlier_groups, agg_columns, how='left')

    # based on the TQR values and also the size of group label the outliers

    condition_null = division_df_outlier.ValueIsNull
    negative_stock_cash_flag = division_df_outlier.negative_stock_cash == "Y"

    condition_1point5_iqr = \
        (division_df_outlier.size_of_group_per_period < 75)|\
        ((division_df_outlier.value > (division_df_outlier.Q3 + 1.5 * division_df_outlier.IQR))|\
        (division_df_outlier.value < (division_df_outlier.Q1 - 1.5 * division_df_outlier.IQR))\
        )

    condition_3point0_iqr = \
        (division_df_outlier.size_of_group_per_period < 75)|\
        ((division_df_outlier.value > (division_df_outlier.Q3 + 3.0 * division_df_outlier.IQR))|\
        (division_df_outlier.value < (division_df_outlier.Q1 - 3.0 * division_df_outlier.IQR))\
        )

    division_df_outlier = division_df_outlier.withColumn(
        "is_1point5_IQR_outlier",
        when(condition_1point5_iqr | condition_null | negative_stock_cash_flag, 'Y').otherwise('N')
        )
    division_df_outlier = division_df_outlier.withColumn(
        "is_3point0_IQR_outlier",
        when(condition_3point0_iqr | condition_null | negative_stock_cash_flag, 'Y').otherwise('N')
        )

    return division_df_outlier


def main():
    """
    the main funciton
    """
    # PARSE PARAM
    parser = argparse.ArgumentParser("BenchmarkEOL")
    parser.add_argument("--input", '-i', metavar="input",
                        type=str, default='dsci-eol-data-bucket',
                        help="s3 bucket for input benchmark")
    parser.add_argument("--output", '-o', metavar="output",
                        type=str, default='dsci-eol-data-bucket',
                        help="s3 bucket for output benchmark")
    conf = parser.parse_args()
    input_bucket = conf.input
    output_bucket = conf.output

    # INITIATE SPARK SESSION
    spark = SparkSession\
        .builder\
        .appName("ETL for Benchmark EOL")\
        .getOrCreate()
    try:
        # Initiate default value & Load Benchmark EOL Elements
        benchmark_element_path = PATH_PUBLISH.format(input_bucket) + 'BenchmarkEOLElement/'
        element_df = spark.read.parquet(benchmark_element_path)

        #TODO: remove the following lines once you make sure that BenchmarkEOLElement has only lowercase columns
        if 'EntryPeriod' in element_df.columns:
            element_df = element_df.withColumn('entryperiod', col('EntryPeriod'))
        if 'CumValue' in element_df.columns:
            element_df = element_df.withColumn('cumvalue', col('CumValue'))
        if 'Division' in element_df.columns:
            element_df = element_df.withColumn('division', col('Division'))
        if 'Element' in element_df.columns:
            element_df = element_df.withColumn('element', col('Element'))

        # Filter out all zero values
        element_df = element_df.filter("cumvalue != 0")

        # Load DivisionLookUp
        div_lookup = spark\
            .read\
            .parquet(*[DIV_LKP_PATH_PUBLISH + 'DivisionLookUp'])\
            .filter("EuroCurrency = '1'")\
            .filter(col('country').contains('NL'))\
            .withColumn('division', col('division').cast(T.StringType()))\

        division_kpi = calculate_eol_division_level(element_df)
        division_kpi = division_kpi\
            .withColumn("aggregate", col("kpi"))\
            .cache()
        print("Size of division_kpi:", division_kpi.count())

        division_df_outlier = apply_outlier_detection(div_lookup, division_kpi, element_df)

        # write to S3
        output_dir = "s3://{0}/prod/publish/BenchmarkEOLDivision/syscreated={1}"
        division_df_outlier\
            .select(
                "division", "entryperiod", "numerator", "denominator", "value", "kpi", "aggregate",
                "is_1point5_iqr_outlier", "is_3point0_iqr_outlier", "percentile", "group_id",
                "size_of_merged_group", "size_of_group_per_period", "q1", "q3", "iqr",
                "negative_stock_cash"
                )\
            .repartition(1)\
            .write.mode("overwrite")\
            .partitionBy("kpi")\
            .parquet(output_dir.format(output_bucket, datetime.now().strftime("%Y-%m-%d")))

    except Exception as general_exception:
        send_to_slack("Error during ETL")
        send_to_slack(general_exception)
        print(1+'a')

    spark.stop()

if __name__ == "__main__":
    main()

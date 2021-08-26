"""
2nd Step of EOL Benchmarking
"""
from __future__ import print_function
import argparse
from functools import reduce
from datetime import datetime
from dateutil.relativedelta import relativedelta

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, concat, when, substring, length, explode, array
import pandas as pd
from slackclient import SlackClient

PATH_PUBLISH = 's3://{0}/prod/publish/'
PATH_DOMAIN = 's3://{0}/prod/domain/'

PROJECTNAME = 'BenchmarkEOL'
SCRIPTNAME = 'ElementsDivisionLevel'

########################
# SLACK
SLACK_TOKEN = "xoxp-28674668403-481692716498-596758164998-dc313510003e460288a85a43e805c69a"
SLACKCLIENT = SlackClient(SLACK_TOKEN)
def send_to_slack(message, target_channel="GHQKCTSNN"):
    '''
    Send slack message to #cig-lago
    '''
    try:
        message = str(message)
        print(message)
        SLACKCLIENT.api_call(
            "chat.postMessage",
            channel=target_channel,
            text="%s: %s- %s"%(PROJECTNAME, SCRIPTNAME, message))
    except Exception as general_exception:
        print("cannot send on slack")
        print(general_exception)
########################


def get_element(benchmark_raw, valid_entryperiod, element, filter_func):
    '''
    Calculate value of single element from benchmark raw
    '''
    agg_element = [F.sum('Value').alias('Value')]
    element_df = benchmark_raw\
        .filter(filter_func)\
        .groupby('Division', 'EntryPeriod').agg(*agg_element)\
        .join(valid_entryperiod, ['Division', 'EntryPeriod'], 'right')\
        .fillna({'Value':0})\
        .withColumn('Element', lit(element))
    return element_df


def calculate_elements(benchmark_raw, valid_entryperiod, dict_element, partition_cols):
    '''
    Calculate cumulative value of elements from benchmark raw and a dictionary of elements
    '''
    w_e = Window\
        .partitionBy(*partition_cols)\
        .orderBy("EntryPeriod")\
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    dfs = map(lambda e: get_element(benchmark_raw, valid_entryperiod, e, dict_element[e]), dict_element.keys())
    elements_df = reduce(lambda df1, df2: df1.union(df2.select(df1.columns)), dfs)\
        .withColumn('CumValue', F.sum('Value').over(w_e))\
        .select('Division', 'FinYear', 'EntryPeriod', 'Element', 'CumValue')
    return elements_df


def calculate_division_elements(spark):
    '''
    Calculate KPI per Division Category
    '''
    # Initiate timeline we are interested in
    end_month = (datetime.now().date().replace(month=1) + relativedelta(years=1)).strftime('%Y-%m') # we're going to calculate the elements until the end of month
    start_month = "2016-12"
    # Get valid financial period per division
    financial_period = spark.read\
        .option("basePath", PATH_DOMAIN + "FinancialPeriods/")\
        .parquet(PATH_DOMAIN + "FinancialPeriods/FinYear=*")\
        .select('Division', 'FinYear', 'FinPeriod', 'StartDate', 'EndDate')\
        .filter(col('FinYear') <= '2019')\
        .groupby('Division', 'FinYear')\
        .agg(F.min('StartDate').alias('StartDate'), F.max('EndDate').alias('EndDate'))\
        .orderBy('Division', 'StartDate')

    # Get Initial benchmark raw data
    cols = ['Division', 'EntryPeriod', 'Aggregate', 'Value', 'Transactions', 'Type']
    initial_benchmark_raw = spark.read.parquet(PATH_PUBLISH + 'BenchmarkRaw/')
    sel_cols = list(map(lambda x: x if x in initial_benchmark_raw.columns else col(x.lower()).alias(x), cols))
    benchmark_raw = initial_benchmark_raw\
        .select(*sel_cols)\
        .filter(~((col('Aggregate').substr(1, 1) == 'W') & col('Type').contains('310')))\
        .withColumn('EntryPeriod', when(col('EntryPeriod') == '2005-2016', lit('2016-12')).otherwise(col('EntryPeriod')))

    # All possible entry date
    list_date = pd.date_range(start=start_month, end=end_month, freq='M').astype(str).str[:7].tolist()
    valid_entryperiod = benchmark_raw.select('Division').distinct()\
        .withColumn('EntryPeriod', array([lit(x) for x in list_date]))\
        .select(col('Division'), explode('EntryPeriod').alias('EntryPeriod'))\
        .join(financial_period, 'Division', 'inner')\
        .filter((concat(col('EntryPeriod'), lit('-01')) >= col('StartDate')) &\
                (concat(col('EntryPeriod'), lit('-01')) <= col('EndDate')))\
        .select('Division', 'EntryPeriod', 'FinYear')

   # Elements and their filter
    dict_pl = {
        'Revenues-C':col('Aggregate').isin(*['WOmz', 'WOvb']),
        'EmployeeCosts-D':col('Aggregate').isin(*['WPer']),
        'CostOfGoods-D':col('Aggregate').isin(*['WKpr']),
        'NetIncome-C':(length('Aggregate') == 4) & (substring('Aggregate', 1, 1) == 'W')
    }
    dict_bs = {
        'AccountReceivables-D':col('Aggregate').isin(*['BVorDeb']),
        'Stock-D':col('Aggregate').isin(*['BVrd']),
        'WorkInProgress-D':col('Aggregate').isin(*['BPro']),
        'Bank-D':col('Aggregate').isin(*['BLimBan', 'BLimKru']),
        'Cash-D':col('Aggregate').isin(*['BLimKas']),
        'TaxToClaim-D':col('Aggregate').isin(*['BVorBtw']),
        'OtherReceivables-D':col('Aggregate').isin(*['BVorOvr', 'BVorOva']),
        'Securities-D':col('Aggregate').isin(*['BEff']),
        'AccountPayables-C':col('Aggregate').isin(*['BSchCre']),
        'Equity-C':col('Aggregate').isin(*['BEiv']),
        'Provisions-C':col('Aggregate').isin(*['BVrz']),
        'LongtermDebt-C':col('Aggregate').isin(*['BLas']),
        'TaxToPay-C':col('Aggregate').isin(*['BSchBtw', 'BSchVpb', 'BSchOvb']),
        'OtherDebts-C':col('Aggregate').isin(*['BSchKol', 'BSchSav', 'BSchSal', 'BSchLhe', 'BSchOvs', 'BSchOpa'])
    }
    # Calculate Profit&Loss and BalanceSheet elements
    benchmark_raw_310 = benchmark_raw\
        .filter(col('Type').contains('310'))\
        .withColumn('Value', -1*col('Value'))
    pl_df = calculate_elements(benchmark_raw, valid_entryperiod, dict_pl, ['Division', 'FinYear', 'Element'])
    bs_df = calculate_elements(benchmark_raw, valid_entryperiod, dict_bs, ['Division', 'Element'])
    bs_310_df = calculate_elements(benchmark_raw_310, valid_entryperiod, dict_bs, ['Division', 'FinYear', 'Element'])
    bs_clean_df = bs_df.union(bs_310_df)\
        .groupby('Division', 'FinYear', 'EntryPeriod', 'Element')\
        .agg(F.sum('CumValue').alias('CumValue'))

    # Remove the bad division based on Stock and Cash cumulative value
    raw_element_df = pl_df.union(bs_clean_df)\
        .filter((col('EntryPeriod') < end_month) & (col('EntryPeriod') >= start_month))
    element_df = raw_element_df\
        .select('Division', 'EntryPeriod', 'Element', 'CumValue')
    return element_df


def main():
    '''
    Main application
    '''
    # PARSE PARAM
    global PATH_PUBLISH, PATH_DOMAIN
    parser = argparse.ArgumentParser("BenchmarkEOL")
    parser.add_argument("--input", '-i', metavar="input",
                        type=str, default='dsci-eol-data-bucket',
                        help="s3 bucket for input benchmark")
    parser.add_argument("--output", '-o', metavar="output",
                        type=str, default='dsci-eol-data-bucket',
                        help="s3 bucket for output benchmark")
    conf = parser.parse_args()
    PATH_PUBLISH = PATH_PUBLISH.format(conf.input)
    PATH_DOMAIN = PATH_DOMAIN.format(conf.input)
    output_bucket = conf.output

    # INITIATE SPARK SESSION
    spark_session = SparkSession\
        .builder\
        .appName("ETL for Benchmark EOL")\
        .getOrCreate()
    try:
        division_element = calculate_division_elements(spark_session).cache()
        print("Size of division_element:", division_element.count())
        # Write to S3
        output_dir = "s3://{0}/prod/publish/BenchmarkEOLElement/"
        division_element.toDF(*[c.lower() for c in division_element.columns])\
            .write.mode("overwrite")\
            .parquet(output_dir.format(output_bucket))
    except Exception as general_exception:
        send_to_slack("Error during ETL")
        send_to_slack(general_exception)
        print(general_exception)
    spark_session.stop()

if __name__ == "__main__":
    main()

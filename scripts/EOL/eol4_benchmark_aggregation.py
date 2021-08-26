"""
4th Step of EOL Benchmarking
"""
from __future__ import print_function
import argparse
from functools import reduce
from datetime import datetime
from dateutil.relativedelta import relativedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, count, avg, expr
from pyspark.sql import types as T
import boto3
from slackclient import SlackClient

SRC_PATH_PUBLISH = 's3://dsci-eol-data-bucket/prod/publish/'

PROJECTNAME = 'BenchmarkEOL'
SCRIPTNAME = 'Aggregated'

########################
# SLACK
SLACK_TOKEN = "xoxp-28674668403-481692716498-596758164998-dc313510003e460288a85a43e805c69a"
SLACK_CLIENT = SlackClient(SLACK_TOKEN)
def send_to_slack(message, target_channel="GHQKCTSNN"):
    '''
    Send slack message to #cig-lago
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

def get_agg_kpi(kpi_df, kpi_name, agg_func, unit, kpi_label):
    '''
    Get single aggregated KPI
    '''
    cols = ['EntryPeriod', 'SizeDescription', 'SectorCode', 'SubsectorCode', 'Value', 'Population']
    dict_agg = {'mean':[avg('Value').alias('Value'), count('Value').alias('Population')],
                'median':[expr('percentile_approx(Value, 0.5)').alias('Value'),
                          count('Value').alias('Population')]}
    kpi_df = kpi_df.filter(col('KPI') == kpi_name)

    kpi_size_all = kpi_df.groupBy('EntryPeriod', 'SectorCode', 'SubsectorCode')\
        .agg(*dict_agg[agg_func])\
        .withColumn('SizeDescription', F.lit('ALL'))\
        .select(*cols)

    kpi_subsector_all = kpi_df.groupBy('EntryPeriod', 'SizeDescription', 'SectorCode')\
        .agg(*dict_agg[agg_func])\
        .withColumn('SubsectorCode', F.lit('ALL'))\
        .select(*cols)

    kpi_size_subsector_all = kpi_df.groupBy('EntryPeriod', 'SectorCode')\
        .agg(*dict_agg[agg_func])\
        .withColumn('SizeDescription', F.lit('ALL'))\
        .withColumn('SubsectorCode', F.lit('ALL'))\
        .select(*cols)

    kpi_comb = kpi_df.groupBy('EntryPeriod', 'SizeDescription', 'SectorCode', 'SubsectorCode')\
        .agg(*dict_agg[agg_func])\
        .select(*cols)

    kpi = kpi_size_all.union(kpi_subsector_all).union(kpi_size_subsector_all).union(kpi_comb)\
        .filter((col('SizeDescription') != "Unknown") &\
                (col('SectorCode') != "Unknown") &\
                (col('SubsectorCode') != "Unknown"))\
        .withColumn('KPI', lit(kpi_label) if kpi_label else lit('{}_{}'.format(kpi_name, agg_func)))\
        .withColumn('Unit', lit(unit))

    return kpi

def calculate_eol_kpi(spark, input_path):
    '''
    Calculated benchmarking kpi for eol aggregated per entryperiod
    '''
    # Initialize kpi dictionary
    d_kpi = {
        'employee_costs': ('median', 'percent', 'employee_costs'),
        'receivable_days': ('median', 'days', 'receivable_days'),
        'gross_margin': ('median', 'percent', 'gross_margin'),
        'quick_ratio': ('median', 'ratio', 'quick_ratio'),
        'roe': ('median', 'percent', 'roe'),
        'inventory_turnover': ('median', 'ratio', 'inventory_turnover'),
        'solvability': ('median', 'percent', 'solvability'),
        'current_ratio': ('median', 'ratio', 'current_ratio')
    }

    # Build dataset from Benchmark EOL Division Level and DivisionLookUp
    cols = ['Division', 'EntryPeriod', 'KPI', 'Value'] + ["is_1point5_iqr_outlier", "is_3point0_iqr_outlier"]
    eol_division_level = spark.read\
        .option("basePath", input_path)\
        .parquet(input_path + 'kpi=*')
    sel_cols = list(map(lambda x: x if x in eol_division_level.columns else col(x.lower()).alias(x), cols))
    division_level = eol_division_level\
        .select(*sel_cols)\
        .filter(col('is_3point0_iqr_outlier') == 'N')

    div_lookup = spark\
        .read\
        .parquet(*[SRC_PATH_PUBLISH + 'DivisionLookUp'])\
        .filter("EuroCurrency = '1'")\
        .filter(F.col('country').contains('NL'))\
        .select(
            F.col('division').alias('Division'),
            F.col('sectorcode').alias('SectorCode'),
            F.col('subsectorcode').alias('SubsectorCode'),
            F.col('sizedescription').alias('SizeDescription')
            )\
        .withColumn('Division', F.col('Division').cast(T.StringType()))\
        .fillna({'SectorCode': "Unknown", 'SubsectorCode': "Unknown", 'SizeDescription': "Unknown"})

    kpi_eol_div = div_lookup\
        .join(division_level, 'Division', 'inner')\
        .filter(~F.col('Value').isin(float('nan'), float('inf'), -float('inf')))\
        .select('EntryPeriod', 'KPI', 'SizeDescription', 'SectorCode', 'SubsectorCode', 'Value')

    # Calculate aggregated kpi
    dfs = map(lambda k: get_agg_kpi(kpi_eol_div, k, *d_kpi[k]), d_kpi.keys())
    eol_df = reduce(lambda df1, df2: df1.union(df2.select(df1.columns)), dfs)
    return eol_df

def main():
    '''
    Main application
    '''
    # PARSE PARAM
    parser = argparse.ArgumentParser("BenchmarkEOL")
    parser.add_argument("--input", '-i', metavar="input",
                        type=str, default='dsci-eol-data-bucket',
                        help="s3 bucket for input benchmark")
    parser.add_argument("--output", '-o', metavar="output",
                        type=str, default='dsci-eol-data-bucket',
                        help="s3 bucket for output benchmark")
    parser.add_argument("--monthtoend", '-m', metavar="month to end",
                        type=str, default='3',
                        help="how many months to go back")
    conf = parser.parse_args()
    input_bucket = conf.input
    output_bucket = conf.output

    end_month = (datetime.now() - relativedelta(months=int(conf.monthtoend))).strftime('%Y-%m')

    # INITIATE SPARK SESSION
    spark_session = SparkSession\
        .builder\
        .appName("ETL for Benchmark KPI")\
        .getOrCreate()
    try:
        # GET the folder address of lates divisionlevel computations
        s3_client = boto3.client('s3')
        folder = 'prod/publish/BenchmarkEOLDivision/'
        result = s3_client.list_objects(Bucket=input_bucket, Prefix=folder, Delimiter='/')
        latest_subfolder = sorted([o['Prefix'].split('/')[-2] for o in result.get('CommonPrefixes')])[-1]
        s3_dir = 's3://{0}/prod/publish/BenchmarkEOLDivision/{1}/'.format(input_bucket, latest_subfolder)

        # Calculate eol kpi
        kpi_eol = calculate_eol_kpi(spark_session, s3_dir)
        kpi_eol = kpi_eol.filter("entryperiod <= '%s'" % end_month)

        # Write to S3
        output_dir = "s3://{0}/prod/publish/BenchmarkEOLAggregated/syscreated={1}"
        kpi_eol.toDF(*[c.lower() for c in kpi_eol.columns])\
            .distinct()\
            .repartition(1)\
            .write.mode('overwrite')\
            .partitionBy("entryperiod")\
            .parquet(output_dir.format(output_bucket, datetime.now().strftime("%Y-%m-%d")))

    except Exception as general_exception:
        send_to_slack("Error during ETL")
        send_to_slack(general_exception)
        print(general_exception)

    spark_session.stop()

if __name__ == "__main__":
    main()

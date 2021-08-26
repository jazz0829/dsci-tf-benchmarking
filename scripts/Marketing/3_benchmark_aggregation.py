'''
Benchmark (Marketing) Aggregation Level
'''
from __future__ import print_function
import argparse
import itertools
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, mean, expr, sum as fsum, count, col, concat, round as fround, when
from pyspark.sql import types as T
import boto3
import pandas as pd
from slackclient import SlackClient
from statsmodels.tsa.x13 import x13_arima_analysis

PATH_PUBLISH = 's3://dsci-eol-data-bucket/prod/publish/'
PROJECTNAME = 'Benchmark'
SCRIPTNAME = 'Marketing_3_Aggregation'

# PARSE PARAM
PARSER = argparse.ArgumentParser("EDA")
PARSER.add_argument("--bucket", '-b', metavar="bucket",
                    type=str, default='dsci-eol-data-bucket',
                    help="s3 bucket for benchmark")
PARSER.add_argument("--monthtoend", '-m', metavar="monthtoend",
                    type=str, default='3',
                    help="how many months to go back")

CONF = PARSER.parse_args()
BENCHMARK_BUCKET = CONF.bucket
DATE_FILTER = (datetime.now() - relativedelta(months=int(CONF.monthtoend))).strftime('%Y-%m')

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

def calculate_kpi(kpi_df, kpi_name, agg_func, div_lookup, unit, coefficient=1.0, kpi_label=''):
    '''
    Calculated aggregated KPI
    '''
    kpi_df = div_lookup.alias('b')\
        .join(kpi_df.alias('a'), ['division'], 'inner')\
        .select("b.*", "a.entryperiod", "a.value")\
        .filter(~col('value').isin(float('nan'), float('inf'), -float('inf')))

    cols = ['entryperiod', 'sectorcode', 'kpi', 'value', 'population']
    if not kpi_label:
        kpi_label = lit('{}_{}'.format(kpi_name, agg_func))

    dict_agg = {
        'average':[mean('value').alias('value'), count('value').alias('population')],
        'mean':[mean('value').alias('value'), count('value').alias('population')],
        'median':[expr('percentile_approx(value, 0.5)').alias('value'), count('value').alias('population')]
        }

    kpi_all = kpi_df.groupBy('entryperiod')\
        .agg(*dict_agg[agg_func])\
        .withColumn('kpi', lit(kpi_label))\
        .withColumn('sectorcode', lit("ALL"))\
        .select(*cols)

    kpi_sector = kpi_df.groupBy('entryperiod', 'sectorcode')\
        .agg(*dict_agg[agg_func])\
        .withColumn('kpi', lit(kpi_label))\
        .select(*cols)

    kpi = kpi_all.union(kpi_sector)
    kpi = kpi\
        .withColumn('value', coefficient * col('value'))\
        .withColumn('unit', lit(unit))
    return kpi

def total_growth(growth, kpi_label, div_lookup):
    '''
    Calculate total growth per entryperiod
    '''
    growth = div_lookup.alias('b').join(growth.alias('a'), 'division', 'inner')
    kpi_df = growth.filter(~col('revenue').isin(float('nan'), float('inf'), -float('inf')))
    kpi_df = growth.filter(~col('lastyear_revenue').isin(float('nan'), float('inf'), -float('inf')))

    label = lit(kpi_label)
    agg = [((fsum('revenue')-fsum('lastyear_revenue'))/fsum('lastyear_revenue')*100.0).alias('value'),
           count('division').alias('population')]
    cols = ['entryperiod', 'sectorcode', 'kpi', 'value', 'population']

    kpi_all = kpi_df.groupBy('entryperiod')\
        .agg(*agg)\
        .withColumn('kpi', label)\
        .withColumn('sectorcode', lit("ALL"))\
        .select(*cols)

    kpi_sector = kpi_df.groupBy('entryperiod', 'sectorcode')\
        .agg(*agg)\
        .withColumn('kpi', label)\
        .select(*cols)

    kpi = kpi_all.union(kpi_sector)

    kpi = kpi.withColumn('unit', lit('percent'))
    return kpi

def benchmark(spark, inputfolder):
    '''
    Calculate KPI per Division Category
    '''
    # Initiate timeline we are interested in
    offset_month = 1
    # start_month = (datetime.now() - relativedelta(months=13+offset_month)).strftime('%Y-%m')
    start_month = '2012-01'
    end_month = (datetime.now() - relativedelta(months=offset_month)).strftime('%Y-%m')

    # read data
    kpi_df = spark.read.parquet(inputfolder + "*/*.parquet")
    div_lookup = spark\
        .read\
        .parquet(*[PATH_PUBLISH + 'DivisionLookUp'])\
        .filter("EuroCurrency = '1'")\
        .filter(col('country').contains('NL'))\
        .withColumn('division', col('division').cast(T.StringType()))
    # introduce a new sector call M69 which is representative for accountancy
    div_lookup = div_lookup\
        .withColumn("sectorcode", when(col("subsectorcode") == '69', "M69").otherwise(col("sectorcode")))

    # create seperate kpi dataframes for different kpis
    kpi_df_d2c_30 = kpi_df\
        .filter(col('aggregate') == "D2C_30d")\
        .filter(col('is_1point5_iqr_outlier') == "N")

    kpi_df_cash_flow_position = kpi_df\
        .filter(col('aggregate') == "CashFlowPosition")\
        .filter(col('is_3point0_iqr_outlier') == "N")\
        .withColumn('value', when(col('value') > 10000, lit(1)).otherwise(lit(0)))

    kpi_df_cash_flow_monthly = kpi_df\
        .filter(col('aggregate') == "CashFlowMonthly")\
        .filter(col('is_3point0_iqr_outlier') == "N")\
        .withColumn('value', when(col('value') > 0, lit(1)).otherwise(lit(0)))

    kpi_df_revenue = kpi_df\
        .filter(col('aggregate') == "Revenue")\
        .filter(col('is_3point0_iqr_outlier') == "N")\
        .withColumnRenamed('value', 'revenue')\
        .withColumn('lastyear_entryperiod', concat(
            (col('entryperiod').substr(1, 4).cast(T.IntegerType())-1).cast(T.StringType()),
            lit("-"),
            col('entryperiod').substr(6, 2)))

    lastyear_kpi_df_revenue = kpi_df_revenue\
        .withColumnRenamed('revenue', 'lastyear_revenue')\
        .drop('lastyear_entryperiod')\
        .withColumnRenamed('entryperiod', 'lastyear_entryperiod')

    kpi_df_revenue_rich = kpi_df_revenue\
        .join(lastyear_kpi_df_revenue, ['division', 'lastyear_entryperiod', 'aggregate'])\
        .drop('lastyear_entryperiod')

    # Calculate KPIs
    kpi_df_d2c_30 = calculate_kpi(kpi_df_d2c_30, 'd2c_30d', 'mean', div_lookup, unit='days')
    kpi_growth_all = total_growth(kpi_df_revenue_rich, 'total_growth_lastyear', div_lookup)
    kpi_df_cash_flow_position = calculate_kpi(kpi_df_cash_flow_position, 'cash_flow_position', 'mean',
                                              div_lookup, unit='percent', coefficient=100.0, kpi_label='positive_cash_flow_position')
    kpi_df_cash_flow_monthly = calculate_kpi(kpi_df_cash_flow_monthly, 'cash_flow_monthly', 'mean',
                                             div_lookup, unit='percent', coefficient=100.0, kpi_label='positive_cash_flow_monthly')

    kpi_list = kpi_df_d2c_30\
            .union(kpi_growth_all)\
            .union(kpi_df_cash_flow_position)\
            .union(kpi_df_cash_flow_monthly)\
            .filter((col('entryperiod') >= start_month) & (col('entryperiod') <= end_month))\
            .withColumn('value', fround(col('value'), 4))

    kpi_list = kpi_list.cache()
    print(kpi_list.count())

    # Filling null values
    distinct_ep = kpi_list.select("entryperiod").distinct()
    distinct_sc = kpi_list.select("sectorcode").distinct()
    distinct_kp = kpi_list.select("kpi").distinct()
    full_combinations = distinct_ep.crossJoin(distinct_sc).crossJoin(distinct_kp)
    df_kpi_full = full_combinations.join(kpi_list, ['sectorcode', 'entryperiod', 'kpi'], 'left')

    return df_kpi_full

def seasonal_analysis(kpi_g, group_idx, history_len=60, output_len=39):
    '''
    Calculate treand and seasonal adjusted value for a single kpi and sector
    '''
    # Prepare ts for specific sectorcode and kpi
    print(group_idx)
    ts_df = kpi_g.get_group(group_idx).sort_values('entryperiod')
    ts_df = ts_df.dropna(subset=['entryperiod']).reset_index(drop=True)
    ts_df.entryperiod = pd.to_datetime(ts_df.entryperiod)
    start_idx = ts_df.entryperiod.min().strftime('%Y-%m-%d')
    end_idx = (ts_df.entryperiod + pd.DateOffset(months=1)).max().strftime('%Y-%m-%d')
    ts_df['idx'] = pd.DatetimeIndex(pd.date_range(start=start_idx, end=end_idx, freq='M'))
    ts_df.set_index('idx', inplace=True)
    # Deseasonalize
    input_ts = ts_df['value'].interpolate().tail(history_len).copy()
    sa_results = x13_arima_analysis(input_ts, maxorder=(2, 1), maxdiff=(2, 1), outlier=False, trading=True, freq='M', x12path='/usr/bin')
    out_df = ts_df.tail(output_len).copy()
    out_df['deseasonalizedvalue'] = sa_results.seasadj.tail(output_len).copy()
    out_df['trendvalue'] = sa_results.trend.tail(output_len).copy()
    out_df['entryperiod'] = out_df['entryperiod'].astype(str).str[:7]
    out_df['population'] = out_df['population'].astype(int)
    out_cols = ['entryperiod', 'sectorcode', 'kpi', 'unit', 'population', 'value', 'deseasonalizedvalue', 'trendvalue']
    return out_df.reindex(columns=out_cols)

def deseasonalize(spark, kpi_agg):
    '''
    Calculate treand and seasonal adjusted value for required kpi and sector
    '''
    l_sectorcode = ['ALL', 'G', 'F', 'I', 'N', 'M69', 'C']
    l_kpi = ['total_growth_lastyear', 'positive_cash_flow_monthly',
             'positive_cash_flow_position', 'd2c_30d_mean']
    l_items = itertools.product(l_sectorcode, l_kpi)
    pd_kpi_agg = kpi_agg.toPandas()
    pd_kpi_g = pd_kpi_agg.sort_values('entryperiod')\
        .loc[pd_kpi_agg.sectorcode.isin(l_sectorcode) & pd_kpi_agg.kpi.isin(l_kpi)]\
        .groupby(['sectorcode', 'kpi'])
    pd_kpi_df = pd.concat(map(lambda x: seasonal_analysis(pd_kpi_g, x), l_items))
    kpi_df = spark.createDataFrame(pd_kpi_df)
    return kpi_df

def main():
    '''
    Main application
    '''
    spark_session = SparkSession\
        .builder\
        .appName("ETL for Benchmark KPI")\
        .getOrCreate()

    try:
        # GET the folder address of lates divisionlevel computations
        s3_client = boto3.client('s3')
        folder = 'prod/publish/BenchmarkDivisionLevel/'
        result = s3_client.list_objects(Bucket=BENCHMARK_BUCKET, Prefix=folder, Delimiter='/')
        latest_subfolder = sorted([o['Prefix'].split('/')[-2] for o in result.get('CommonPrefixes')])[-1]
        input_dir = 's3://' + BENCHMARK_BUCKET + '/prod/publish/BenchmarkDivisionLevel/' + latest_subfolder + '/'

        kpi_agg_df = benchmark(spark_session, input_dir)
        kpi_agg_df = kpi_agg_df.filter("entryperiod <= '{}'".format(DATE_FILTER))
        kpi_full_df = deseasonalize(spark_session, kpi_agg_df.toDF(*[c.lower() for c in kpi_agg_df.columns]))

        # Write to S3
        output_dir = "s3://{0}/prod/publish/BenchmarkAggregated/syscreated={1}"
        kpi_full_df.toDF(*[c.lower() for c in kpi_full_df.columns])\
            .distinct()\
            .repartition(1)\
            .write.mode('overwrite')\
            .parquet(output_dir.format(BENCHMARK_BUCKET, datetime.now().strftime("%Y-%m-%d")))

    except Exception as general_exception:
        send_to_slack("Error during ETL")
        send_to_slack(general_exception)
        print(1+'a')

    spark_session.stop()

if __name__ == "__main__":
    main()

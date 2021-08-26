"""
1st Step of Benchmark Raw Script
"""
from __future__ import print_function

import argparse
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, when, length, countDistinct, mean, count
from pyspark.sql.functions import row_number, lit, abs as fabs, round as fround, sum as fsum
from pyspark.sql import types as T
from slackclient import SlackClient


PATH_PUBLISH = 's3://dsci-eol-data-bucket/prod/publish/'
PATH_DOMAIN = 's3://dsci-eol-data-bucket/prod/domain/'
PATH_PARTITIONED = 's3://dsci-eol-data-bucket/prod/partitioned/'
PROJECTNAME = 'Benchmark'
SCRIPTNAME = 'Marketing_1_Raw'

# PARSE PARAM
PARSER = argparse.ArgumentParser("EDA")
PARSER.add_argument("--bucket", '-b', metavar="bucket",
                    type=str, default='dsci-eol-data-bucket',
                    help="s3 bucket for benchmark")
PARSER.add_argument("--postfix", '-p', metavar="postfix",
                    type=str, default='',
                    help="a postfix to avoid overwritting the main folder")
PARSER.add_argument("--test", '-t', metavar="test",
                    type=str, default='False',
                    help="if True, then reads a subset of data")

CONF = PARSER.parse_args()
BENCHMARK_BUCKET = CONF.bucket
POSTFIX = CONF.postfix
ISTEST = (CONF.test == 'True')

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


def calculate_dso(invoice_d2c_g, dso_type):
    """
    function to calculate DSO
    """
    agg_function = [count(col('Value')).alias("Transactions"), mean(col('Value')).alias("Value")]
    invoice_d2c_agg = invoice_d2c_g.groupBy('Division', 'EntryPeriod')\
        .agg(*agg_function)\
        .withColumn('Aggregate', lit('D2C_{}'.format(dso_type)))\
        .withColumn('type', lit('Days to Cash In'))
    return invoice_d2c_agg

# Aggregate RGS per Division
def aggregate_division_rgs(spark):
    """
    function to aggregate all transactions of division per rgs code
    """

    # Prepare RGS Code for GLAccount
    gla = spark.read\
        .parquet(PATH_DOMAIN + "GLAccountsClassified/")\
        .select('ID', 'Division', 'RGSCode', 'ExactRGSCode')
    # Is customer and exact label valid?
    exact_label = when(
        col('ExactRGSCode').isNotNull(),
        length('ExactRGSCode') >= 10
        ).otherwise(col('ExactRGSCode').isNotNull())
    customer_label = when(
        col('RGSCode').isNotNull(),
        length('RGSCode') >= 7
        ).otherwise(col('RGSCode').isNotNull())
    gla = gla\
        .withColumn('IsCuLabel', customer_label)\
        .withColumn('IsExLabel', exact_label)

    # Choose RGS code for GLAccount
    rgs_code = when(
        col('IsCuLabel') == 'true',
        col('RGSCode')
        ).otherwise(col('ExactRGSCode'))
    gla = gla\
        .filter((col('IsCuLabel') == 'true') | (col('IsExLabel') == 'true'))\
        .withColumn('RGSCode', rgs_code.substr(1, 10))\
        .select('ID', 'Division', 'RGSCode')

    # Prepare GLTransaction Data
    w_dedup = Window\
        .partitionBy('ID')\
        .orderBy(col('sysmodified').desc(), col('x_sysingested').desc())
    # with Column Condition
    condition_entry_period = when(col('EntryDate').substr(1, 7) < '2010-01', lit('2009-12'))\
        .otherwise(col('EntryDate').substr(1, 7))
    condition_type_310 = when(col('Type') == '310', lit('Y'))\
        .otherwise(lit('N'))

    if ISTEST:
        glt = spark.read\
            .option("mergeSchema", "true")\
            .parquet(PATH_PARTITIONED + 'GLTransactions_v2/input_partition_NLC=*/syscreated_year=*/syscreated_month=*/part-00000*.parquet')
    else:
        glt = spark.read\
            .option("mergeSchema", "true")\
            .parquet(PATH_PARTITIONED + 'GLTransactions_v2/input_partition_NLC=*/syscreated_year=*/syscreated_month=*/*.parquet')
    glt = glt\
        .select('ID', 'Type', 'syscreated', 'sysmodified', 'x_sysingested', 'x_isdeleted', 'AmountDC', 'GLAccount', 'EntryDate')\
        .withColumn('_rank', row_number().over(w_dedup))\
        .filter((col('_rank') == 1) & (col('x_isdeleted') == 'N'))\
        .filter((col("EntryDate") >= '1984-01-01') & (col("EntryDate") < '2025-01-01'))\
        .withColumn('EntryPeriod', condition_entry_period)\
        .withColumn('hastype310', condition_type_310)\
        .select('AmountDC', 'GLAccount', 'EntryPeriod', 'hastype310')
    # Join GLTransaction with GLAccount
    glt_j = glt.join(gla, gla.ID == glt.GLAccount, 'inner')\
        .select('Division', 'EntryPeriod', 'RGSCode', 'AmountDC', 'hastype310')
    # Aggregate per Division and Metric
    agg_function1 = [fsum('AmountDC').alias('AmountDC'), count(col('AmountDC')).alias("Transactions")]
    agg_function2 = [fsum('AmountDC').alias('AmountDC'), fsum(col('Transactions')).alias("Transactions")]
    lv3_aggregate = col('RGSCode').substr(1, 7).alias('RGSCode')
    lv2_aggregate = col('RGSCode').substr(1, 4).alias('RGSCode')
    rgs_agg_lv4 = glt_j\
        .groupBy('Division', 'EntryPeriod', 'RGSCode', 'hastype310')\
        .agg(*agg_function1)\
        .withColumn('type', when(col("hastype310") == "Y", lit("RGSL4-hastype310")).otherwise(lit("RGSL4")))\
        .cache()
    rgs_agg_lv3 = rgs_agg_lv4\
        .groupBy('Division', 'EntryPeriod', lv3_aggregate, 'hastype310')\
        .agg(*agg_function2)\
        .withColumn('type', when(col("hastype310") == "Y", lit("RGSL3-hastype310")).otherwise(lit("RGSL3")))
    rgs_agg_lv2 = rgs_agg_lv4\
        .groupBy('Division', 'EntryPeriod', lv2_aggregate, 'hastype310')\
        .agg(*agg_function2)\
        .withColumn('type', when(col("hastype310") == "Y", lit("RGSL2-hastype310")).otherwise(lit("RGSL2")))
    rgs_agg = (rgs_agg_lv3).union(rgs_agg_lv2)
    rgs_agg = rgs_agg.select(
        'Division', 'EntryPeriod',
        col('RGSCode').alias('Aggregate'),
        col('AmountDC').alias('Value'),
        'Transactions', 'type'
        )
    # as the customers rgs code can be as short as level 4, with the following filter
    # we make sure that short rgs codes don't end up in higher levels
    rgs_agg = rgs_agg.filter(
        """
           (length(Aggregate) = 4  AND type LIKE 'RGSL2%')
        OR (length(Aggregate) = 7  AND type LIKE 'RGSL3%')
        OR (length(Aggregate) = 10 AND type LIKE 'RGSL4%')
        """
        )

    return rgs_agg


def aggregate_division_d2c(spark):
    """
    Aggregate DSO per Division
    """
    # Prepare DSO for dataset
    invoice_d2c = spark.read.parquet(PATH_DOMAIN + 'InvoicesDso/')
    invoice_d2c = invoice_d2c\
        .select(col('division').alias('Division'),
                col('PaymentEntryDate').substr(1, 7).alias('EntryPeriod'),
                col('ActualTimeBetweenInvoiceandPayment').alias('ATBIP'),
                col('PaymentPaymentMethod').alias('ActualPaymentMethod'),
                col('PaymentMethod').alias('InvoicePaymentMethod'),
                'PaymentDays', 'PaymentID', 'UniqueInvoiceNumber', 'MatchID')\
        .filter(col('Division').isNotNull())\
        .withColumn('Division', col('Division').cast(T.StringType()))\
        .withColumn('PaymentDays', col('PaymentDays').cast(T.IntegerType()))

    # Classify Transaction
    invoice_matched = invoice_d2c.groupBy('Division', 'MatchID').agg(
        countDistinct('UniqueInvoiceNumber').alias('Invoice_distinct'),
        countDistinct('PaymentID').alias('PaymentID_distinct'))
    sisp_filter = (col('Invoice_distinct') == 1) & (col('PaymentID_distinct') == 1)
    misp_filter = (col('Invoice_distinct') > 1) & (col('PaymentID_distinct') == 1)

    invoice_d2c_all = invoice_matched.filter(sisp_filter | misp_filter).alias('a')\
        .join(invoice_d2c.alias('b'), ['Division', 'MatchID'], 'inner').select('a.Division', 'b.*')\
        .withColumnRenamed('ATBIP', 'Value')\
        .filter(col('InvoicePaymentMethod') == 'B')\
        .filter(col('Value') >= 0)

    # Group based on payment terms
    invoice_d2c_14 = invoice_d2c_all.filter(fabs(col('PaymentDays')-14) < 3)
    invoice_d2c_30 = invoice_d2c_all.filter(fabs(col('PaymentDays')-30) < 3)

    # Calculate average DSO per Division per Period
    invoice_d2c_14_agg = calculate_dso(invoice_d2c_14, '14d')
    invoice_d2c_30_agg = calculate_dso(invoice_d2c_30, '30d')
    invoice_d2c_all_agg = calculate_dso(invoice_d2c_all, 'all')

    # Return the dso agg
    invoice_d2c_agg = invoice_d2c_all_agg\
        .union(invoice_d2c_30_agg)\
        .union(invoice_d2c_14_agg)\
        .withColumn("Value", fround('Value', 4))\
        .select('Division', 'EntryPeriod', 'Aggregate', 'Value', 'Transactions', 'type')
    return invoice_d2c_agg


def calculate_benchmark_raw(spark):
    """
    the function that calculates the benchmark raw
    """
    div_lookup = spark.read.parquet(PATH_PUBLISH + 'DivisionLookUp/EuroCurrency=1')
    div_lookup = div_lookup.filter(col('country').contains('NL'))\
        .select(col('division').alias('Division'),
                col('product').alias('Product'),
                col('sectorcode').alias('SectorCode'),
                col('subsectorcode').alias('SubsectorCode'),
                col('sizedescription').alias('SizeDescription'))\

    # Filter Division (NL-country and EuroCurrency)
    div_lookup = spark\
        .read\
        .parquet(*[PATH_PUBLISH + 'DivisionLookUp'])\
        .filter("EuroCurrency = '1'")\
        .filter(col('country').contains('NL'))\
        .withColumn('Division', col('division').cast(T.StringType()))\

    send_to_slack(
        "%d divisions removed from BenchmarkRaw as they were not allowing Data Sharing!"\
        % div_lookup.filter("allow_data_sharing = 'False'").count()
        )

    div_lookup_specific = div_lookup\
        .filter("allow_data_sharing IS NULL OR allow_data_sharing = 'True'")\
        .select('Division')

    d2c_agg = aggregate_division_d2c(spark)
    rgs_agg = aggregate_division_rgs(spark)
    data_agg = rgs_agg.union(d2c_agg)
    benchmark_raw = data_agg.join(div_lookup_specific, ['Division'], 'inner')

    benchmark_raw = benchmark_raw.select(
        'Division', 'EntryPeriod', 'Aggregate', 'Value',
        'Transactions', 'type')

    # Filter bad division
    bad_divisions_df = rgs_agg\
        .filter(col('Aggregate').isin(*['BVrd', 'BLimKas']))\
        .groupby('Division', 'Aggregate')\
        .agg(fsum('Value').alias('Value'))\
        .filter(col('Value') < -.1)\
        .select("Division")\
        .cache()
    print("number of divisions with negative BLimKas or BVrd: ", bad_divisions_df.count())

    benchmark_raw = benchmark_raw.alias('a')\
        .join(bad_divisions_df.alias('b'), 'Division', 'left_anti')\
        .select('a.*')

    return benchmark_raw

def main():
    """
    main function to run
    """
    spark_session = SparkSession\
        .builder\
        .appName("ETL for Benchmark Raw")\
        .getOrCreate()

    try:
        benchmark_raw = calculate_benchmark_raw(spark_session)

        output_dir = 's3://{}/prod/publish/BenchmarkRaw{}/'
        benchmark_raw\
            .write\
            .mode('overwrite')\
            .parquet(output_dir.format(BENCHMARK_BUCKET, POSTFIX))

    except Exception as general_exception:
        send_to_slack("Error during ETL")
        send_to_slack(general_exception)
        print(1+'a')

    spark_session.stop()


if __name__ == "__main__":
    main()

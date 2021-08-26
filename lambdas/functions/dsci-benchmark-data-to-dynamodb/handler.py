'''
Lambda handler to load parquet to DynamoDB
'''
import traceback
import json
import os
from itertools import groupby
from datetime import datetime
from dateutil.relativedelta import relativedelta
import boto3


def item_value(dict_x, fields):
    '''
    Create a single dictionary item by subseting based on fields
    '''
    return {k: str(round(dict_x[k], 2)) if 'value' in k else str(dict_x[k]) for k in fields}

def aggregate(tuple_t):
    '''
    Aggregate grouped items t -> (key, value)
    key -> kpi_identifier
    value -> list of dictionary
    '''
    fields = ['entryperiod', 'value', 'population', 'unit', 'deseasonalizedvalue', 'trendvalue']
    return {'kpi_identifier':tuple_t[0].lower(),
            'sectorcode':tuple_t[0].split('-')[0].lower(),
            'kpi':tuple_t[0].split('-')[1],
            'data':list(map(lambda x: item_value(x, fields), tuple_t[1]))}

def perform_integration_test(response_list, sectors_list, total_kpis):
    '''
    There is always X kpis to be shown, so we validate if total is equal the expected sector list * X
    '''
    if len(response_list) != len(sectors_list*int(total_kpis)):
        return False

    for item in response_list:
        if item['kpi'] == 'd2c_30d_mean':
            for value in item['data']:
                if not (0 < float(value['value']) < 70 and int(value['population']) > 75):
                    print(item['kpi_identifier'], value['value'])
                    return False
        if item['kpi'] == 'total_growth_lastyear':
            for value in item['data']:
                if not (-100 < float(value['value']) < 100 and int(value['population']) > 75):
                    print(item['kpi_identifier'], value['value'])
                    return False
    return True

def lambda_handler(event, context):
    '''
    Main function of lambda handler
    '''
    # get variables from environment
    dynamo_marketing_table_name = os.environ['BENCHMARK_MARKETING_DYNAMO_DB']
    bookmark_table_name = os.environ['BENCHMARK_BOOKMARK_DYNAMO_DB']
    bucket_name = os.environ['BENCHMARK_MARKETING_BUCKET']
    total_kpis = os.environ['BENCHMARK_KPIS']
    sectors_list = os.environ['BENCHMARK_MARKETING_SECTORS'].split(",")

    # define aws objects to be used
    s3_client = boto3.client('s3')
    dynamodb = boto3.resource('dynamodb', region_name='eu-west-1')
    bookmark_table = dynamodb.Table(bookmark_table_name)
    dynamo_marketing_table = dynamodb.Table(dynamo_marketing_table_name)

    # define variables
    date = datetime.now().strftime("%Y-%m-%d")
    file_path = "prod/publish/BenchmarkAggregated/syscreated={}"
    file_path = file_path.format(date)

    objects = s3_client.list_objects_v2(
        Bucket=bucket_name,
        Prefix=file_path
    )
    s3_key = None
    if objects['KeyCount'] > 0:
        for obj in objects['Contents']:
            if obj['Key'].endswith('.parquet'):
                s3_key = obj['Key']
    else:
        print('file not found')
        return {'Success': False, 'message': 'file not found'}

    if s3_key is None:
        print('s3Key is empty')
        return {'Success': False, 'message': 's3Key is empty'}

    try:
        sql_stmt = f"select s.\"kpi\", s.\"sectorcode\", s.\"entryperiod\", s.\"value\", CAST(s.\"population\" as INTEGER) as \"population\", s.\"unit\", s.\"deseasonalizedvalue\", s.\"trendvalue\" from s3object s where s.sectorcode in {sectors_list}"
        req = s3_client.select_object_content(
            Bucket=bucket_name,
            Key=s3_key,
            ExpressionType='SQL',
            Expression=sql_stmt,
            InputSerialization={'Parquet': {}},
            OutputSerialization={'JSON': {'RecordDelimiter':'\n'}}
            )

        results = ''
        for event in req['Payload']:
            if 'Records' in event:
                results += event['Records']['Payload'].decode('utf-8')

        offset_month = 2
        date_start = str(datetime.now()-relativedelta(months=36+offset_month))[:7]
        date_end = str(datetime.now()- relativedelta(months=offset_month))[:7]

        # Filter empty data
        no_empty = filter(lambda x: len(x) > 0, results.split('\n'))
        # Convert into dictionary
        raw_items = map(lambda x: json.loads(x), no_empty)
        # Filter noise/unused data
        filter_funct = lambda x: (x['entryperiod'] >= date_start) & (x['entryperiod'] < date_end) &\
            (x['sectorcode'] != 'Unknown') & (x['sectorcode'] != '')
        filtered_items = list(filter(filter_funct, raw_items))
        # Sort data
        sort_func = lambda x: '|'.join([x['sectorcode'], x['kpi'], x['entryperiod']])
        sorted_items = sorted(filtered_items, key=sort_func)
        # Group data
        key_func = lambda x: '-'.join([x['sectorcode'], x['kpi']])
        response_list = list(map(lambda t: aggregate(t), groupby(sorted_items, key_func)))

        print("Total item:", len(response_list))

        # Validate if data is as expected
        if perform_integration_test(response_list, sectors_list, total_kpis):
            # Insert (put) items to the table
            bookmark_table.put_item(
                Item={'S3Key': s3_key,
                      'ServiceName':'Bookmark_Marketing',
                      'Updated' : datetime.now().isoformat(),
                      'Status':'OK',
                      'Created':datetime.now().isoformat()})
            with dynamo_marketing_table.batch_writer() as batch:
                for item in response_list:
                    try:
                        batch.put_item(Item={'kpi_identifier':item['kpi_identifier'],
                                             'data':item['data'],
                                             'kpi':item['kpi'],
                                             'sectorcode':item['sectorcode'].lower(),
                                             'updated':datetime.now().isoformat()})
                    except Exception as general_exception:
                        print(general_exception)
                        print(item['kpi_identifier'], item['data'], item['kpi'], item['sectorcode'])
                        print(traceback.format_exc())
            return {'Success': True}
        else:
            print(f"the data on {s3_key} is not as expected.")
            return {'Success': False, 'message' : f"the data on {s3_key} is not as expected."}
    except Exception as general_exception:
        print(general_exception)
        print(traceback.format_exc())
        return {'Success': False, 'message': traceback.format_exc()}

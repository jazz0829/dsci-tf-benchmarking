import traceback
import json
import os
from itertools import groupby
from datetime import datetime
from dateutil.relativedelta import relativedelta
import boto3
from boto3.dynamodb.conditions import Key, Attr

def order_list(data, column):
    l = len(data)
    for i in range(0, l):
        for j in range(0, l-i-1):
            if (data[j][column] > data[j + 1][column]):
                tempo = data[j]
                data[j]= data[j + 1]
                data[j + 1]= tempo
    return data

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
    # get variables from environment
    dynamo_marketing_table_name = os.environ['BENCHMARK_MARKETING_DYNAMO_DB']
    bookmark_table_name = os.environ['BENCHMARK_BOOKMARK_DYNAMO_DB']
    bucket_name = os.environ['BENCHMARK_MARKETING_BUCKET']
    total_kpis = os.environ['BENCHMARK_KPIS']
    sectors_list = os.environ['BENCHMARK_MARKETING_SECTORS'].split(",")
    apigateway_rest_api_id = os.environ['APIGATEWAY_REST_API_ID']
    apigateway_stage_name = os.environ['APIGATEWAY_STAGE_NAME']

    # define aws objects to be used
    s3_client = boto3.client('s3')
    apigateway_client = boto3.client('apigateway')
    dynamodb = boto3.resource('dynamodb',region_name='eu-west-1')
    bookmark_table = dynamodb.Table(bookmark_table_name)
    dynamo_marketing_table = dynamodb.Table(dynamo_marketing_table_name)

    bookmark = bookmark_table.scan(FilterExpression=Attr('ServiceName').eq('Bookmark_Marketing') & Attr('Status').eq('OK'))

    if bookmark['Count']==0:
        print("No data found to be rolled back")
        return { 'Success': False, 'message': "No data found to be rolled back"}
    if bookmark['Count']==1:
        print("Only one record found, rollback cannot be performed")
        return { 'Success': False, 'message': "Only one record found, rollback cannot be performed"}
    data = bookmark['Items']
    listOrdered = order_list(data, 'Updated')
    s3_key = listOrdered[len(listOrdered)-2]['S3Key']
    print(s3_key)

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
        date_start = str(datetime.now()-relativedelta(months=12+offset_month))[:7]
        date_end = str(datetime.now()- relativedelta(months=offset_month))[:7]

        # Filter empty data
        no_empty = filter(lambda x: len(x) > 0, results.split('\n'))
        # Convert into dictionary
        raw_items = map(lambda x: json.loads(x), no_empty)
        # Filter noise/unused data
        filter_funct = lambda x: (x['entryperiod'] >= date_start) & (x['entryperiod'] < date_end) & \
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
            print("try to update bookmark")

            bookmark_table.update_item(
                Key={'S3Key':listOrdered[len(listOrdered)-1]['S3Key']},
                UpdateExpression='SET Updated = :val1, #s= :val2',
                ExpressionAttributeValues={
                    ':val1':datetime.now().isoformat(),
                    ':val2':'Corrupted'
                },
                ExpressionAttributeNames={
                    "#s": "Status"
                },
                ReturnValues="UPDATED_NEW"
            )

            with dynamo_marketing_table.batch_writer() as batch:
                for item in response_list:
                    try:
                        batch.put_item(Item={'kpi_identifier':item['kpi_identifier'],
                                             'data':item['data'],
                                             'kpi':item['kpi'],
                                             'sectorcode':item['sectorcode'].lower(),
                                             'updated':datetime.now().isoformat()})
                    except:
                        print(item['kpi_identifier'],item['data'],item['kpi'],item['sectorcode'])
                        print (traceback.format_exc())
                        return { 'Success': False, 'message': traceback.format_exc()}
            apigateway_client.flush_stage_cache(restApiId=apigateway_rest_api_id, stageName=apigateway_stage_name)
            print(f'Cache flushed: for API {apigateway_rest_api_id}/{apigateway_stage_name}')
            return { 'Success': True }
        else:
            print(f"the data on {s3_key} is not as expected.")
            raise Exception(f"the data on {s3_key} is not as expected.")
    except:
        print(traceback.format_exc())
        return {'Success': False, 'message': traceback.format_exc()}
from datetime import datetime
import boto3
from boto3.dynamodb.conditions import Key, Attr
from decimal import *
import traceback
import json
import os

def order_list(data, column):
    orderedData = []
    l = len(data) 
    for i in range(0, l): 
        for j in range(0, l-i-1): 
            if (data[j][column] > data[j + 1][column]): 
                tempo = data[j] 
                data[j]= data[j + 1] 
                data[j + 1]= tempo 
    return data 

def lambda_handler(event, context):
    # get variables from environment
    dynamo_eol_table_name = os.environ['BENCHMARK_EOL_DYNAMO_DB']
    bookmark_table_name = os.environ['BENCHMARK_BOOKMARK_DYNAMO_DB']
    bucket_name = os.environ['BENCHMARK_BUCKET']
    apigateway_rest_api_id = os.environ['APIGATEWAY_REST_API_ID']
    apigateway_stage_name = os.environ['APIGATEWAY_STAGE_NAME']

    # define aws objects to be used
    apigateway_client = boto3.client('apigateway')
    s3 = boto3.client('s3')
    dynamodb = boto3.resource('dynamodb',region_name='eu-west-1')
    bookmark_table = dynamodb.Table(bookmark_table_name)
    dynamo_eol_table = dynamodb.Table(dynamo_eol_table_name)
    
    bookmark = bookmark_table.scan(FilterExpression=Attr('ServiceName').eq('Bookmark_EOL') & Attr('Status').eq('OK'))
    
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
        sql_stmt = "select * from s3object s"
        req = s3.select_object_content(
            Bucket=bucket_name,
            Key=s3_key,
            ExpressionType='SQL',
            Expression=sql_stmt,
            InputSerialization = {'Parquet': {}},
            OutputSerialization = {'JSON': {'RecordDelimiter':'\n'}}
            )

        results=[]
        for event in req['Payload']:
            if 'Records' in event:
                results.extend(event['Records']['Payload'].decode('utf-8'))
        response_list = ''.join(results).split('\n')

        total=[]
        small_total=[]

        responseUpdate = bookmark_table.update_item(
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

        with dynamo_eol_table.batch_writer() as batch:
            for item in response_list:
                try:
                    #TODO: to be removed when we have proper parquet
                    if item == "":
                        continue
                    item_json = json.loads(item)
                    kpi_identifier = f"{item_json['kpi']}-{item_json['sectorcode']}-{item_json['subsectorcode']}-{item_json['sizedescription']}"
                    batch.put_item(Item={'kpi_identifier':kpi_identifier.lower(),
                        'kpi':item_json['kpi'].lower(),
                        'sector':item_json['sectorcode'].lower(),
                        'subsector':item_json['subsectorcode'].lower(),
                        'companysize':item_json['sizedescription'].lower(),
                        'value':round(Decimal(str(item_json['value'])),2),
                        'population':int(item_json['population']),
                        'updated':datetime.now().isoformat()})
                except:
                    print(item_json['KPI'],item_json['Sector'],item_json['SubSector'],item_json['CompanySize'],item_json['Value'])
                    print (traceback.format_exc())
                    return { 'Success': False, 'message': traceback.format_exc()}

        apigateway_client.flush_stage_cache(restApiId=apigateway_rest_api_id, stageName=apigateway_stage_name)
        print(f'Cache flushed: for API {apigateway_rest_api_id}/{apigateway_stage_name}')
        return { 'Success': True }
    except:
        print (traceback.format_exc())
        return { 'Success': False, 'message': traceback.format_exc()}
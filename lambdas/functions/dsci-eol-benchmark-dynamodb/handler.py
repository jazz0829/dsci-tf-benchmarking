from datetime import datetime
from decimal import *
import boto3
import traceback
import json
import os

def order_list(data, column):
    orderedData = []
    l = len(data)
    for i in range(0, l):
        for j in range(0, l-i-1):
            if data[j][column] > data[j + 1][column]:
                tempo = data[j]
                data[j]= data[j + 1]
                data[j + 1]= tempo
    return data

def lambda_handler(event, context):
    # get variables from environment
    dynamo_eol_table_name = os.environ['BENCHMARK_EOL_DYNAMO_DB']
    bookmark_table_name = os.environ['BENCHMARK_BOOKMARK_DYNAMO_DB']
    bucket_name = os.environ['BENCHMARK_BUCKET']

    # define aws objects to be used
    s3 = boto3.client('s3')
    dynamodb = boto3.resource('dynamodb',region_name='eu-west-1')
    bookmark_table = dynamodb.Table(bookmark_table_name)
    dynamo_eol_table = dynamodb.Table(dynamo_eol_table_name)

    # define variables
    date = datetime.now().strftime("%Y-%m-%d")

    file_path = f"prod/publish/BenchmarkEOLAggregated/syscreated={date}"

    # get the latest entry period
    objects = s3.list_objects_v2(
        Bucket=bucket_name,
        Prefix=file_path
    )

    if objects['KeyCount'] > 0:
        for obj in objects['Contents']:
            if obj['Key'].endswith('.parquet'):
                s3_key=obj['Key']
    else:
        print('file not found')
        return { 'Success': False, 'message': 'file not found'}

    print("Contents", objects["Contents"])
    orderedFolders=order_list(objects["Contents"],"Key")
    print("ordered: ",orderedFolders[len(orderedFolders)-1]["Key"])

    objects = s3.list_objects_v2(
        Bucket=bucket_name,
        Prefix=orderedFolders[len(orderedFolders)-1]["Key"]
    )

    s3_key=None
    if objects['KeyCount'] > 0:
        for obj in objects['Contents']:
            if obj['Key'].endswith('.parquet'):
                s3_key=obj['Key']
    else:
        print('file not found')
        return { 'Success': False, 'message': 'file not found'}

    if s3_key is None:
        print('s3Key is empty')
        return { 'Success': False, 'message': 's3Key is empty'}

    try:
        sql_stmt = "select * from s3object s where CAST(s.population as FLOAT) > 75"
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
        response_list = list(filter(lambda x: x!="", ''.join(results).split('\n')))

        bookmark_table.put_item(Item={'S3Key' : s3_key, 'ServiceName':'Bookmark_EOL', 'Updated' : datetime.now().isoformat(), 'Status':'OK', 'Created':datetime.now().isoformat()})

        scan = dynamo_eol_table.scan(
            ProjectionExpression='#k',
            ExpressionAttributeNames={
                '#k': 'kpi_identifier'
            }
        )

        with dynamo_eol_table.batch_writer() as batch:
            for item in scan['Items']:
                print(f'deleting {item}')
                batch.delete_item(Key=item)


        with dynamo_eol_table.batch_writer() as batch:
            for item in response_list:
                try:
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
                    print(item_json['kpi'],item_json['sectorcode'],item_json['subsectorcode'],item_json['sizedescription'],item_json['value'])
                    print (traceback.format_exc())
                    return { 'Success': False, 'message': traceback.format_exc()}

        return { 'Success': True, 'ItemsCount': len(response_list) }
    except:
        print (traceback.format_exc())
        return { 'Success': False, 'message': traceback.format_exc()}
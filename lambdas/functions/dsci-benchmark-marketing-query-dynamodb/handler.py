import boto3
import os
import traceback

def lambda_handler(event, context):
    try:
        table_name = os.environ['BENCHMARK_MARKETING_DYNAMO_DB']
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(table_name)
        if 'sectorcode' in event and 'kpi' in event:
            sector_code = event['sectorcode'].lower()
            kpi = event['kpi'].lower()
            response = table.get_item(Key={'kpi_identifier':f'{sector_code}-{kpi}'})
            if 'Item' in response:
                if 'history' in event and event['history'].lower() == 'full':
                    item = response['Item']
                    item['body'] = item.pop('data')
                    del item['updated']
                    return item
                else:
                    item = response["Item"]
                    item['body'] = item.pop('data')
                    del item['updated']
                    item['body'] = item['body'][-12:]
                    return item
            else:
                return {}
    except:
        print(traceback.format_exc())
        return {}
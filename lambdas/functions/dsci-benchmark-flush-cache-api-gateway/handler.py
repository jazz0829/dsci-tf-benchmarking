import boto3
import traceback

def lambda_handler(event, context):
    client = boto3.client('apigateway')
    try:
        api_list = event['apis']
        for api in api_list:
            client.flush_stage_cache(
            restApiId=api['api_name'],
            stageName=api['stage']
            )
            print('Cache flushed: ',api['api_name'],api['stage'])
        return {'Success':True}
    except:
        print (traceback.format_exc())
        return {'Success':False, 'message':traceback.format_exc()}
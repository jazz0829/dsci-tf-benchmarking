import urllib.request
import urllib.parse
import json
import os
import traceback
import itertools

api_rest_api_id = os.environ['MARKETING_APIGATEWAY_REST_API_ID']
api_stage_name = os.environ['MARKETING_APIGATEWAY_STAGE_NAME']
api_region = os.environ['MARKETING_APIGATEWAY_REGION']
api_resource_name = os.environ['MARKETING_APIGATEWAY_RESOURCE_NAME']
api_api_key = os.environ['MARKETING_APIGATEWAY_API_KEY']

eol_api_rest_api_id = os.environ['EOL_APIGATEWAY_REST_API_ID']
eol_api_stage_name = os.environ['EOL_APIGATEWAY_STAGE_NAME']
eol_api_resource_name = os.environ['EOL_APIGATEWAY_RESOURCE_NAME']
eol_api_api_key = os.environ['EOL_APIGATEWAY_API_KEY']


def validate_marketing_endpoint(sector_code, kpi):
    headers = {'x-api-key': api_api_key}
    url = f'https://{api_rest_api_id}.execute-api.{api_region}.amazonaws.com/{api_stage_name}/{api_resource_name}?sectorcode={sector_code}&kpi={kpi}'
    req = urllib.request.Request(url, headers=headers)
    response = urllib.request.urlopen(req)
    status_code = response.getcode()
    payload = json.loads(response.read())
    return status_code == 200 and payload['sectorcode'].lower() == sector_code.lower() and payload['kpi'].lower() == kpi.lower() and len(payload['body']) == 12

def validate_eol_endpoint(expected_count):
    headers = {'x-api-key': eol_api_api_key}
    url = f'https://{eol_api_rest_api_id}.execute-api.{api_region}.amazonaws.com/{eol_api_stage_name}/{eol_api_resource_name}'
    req = urllib.request.Request(url, headers=headers)
    response = urllib.request.urlopen(req)
    status_code = response.getcode()
    payload = json.loads(response.read())
    return status_code == 200 and len(payload) == expected_count


def lambda_handler(event, context):
    try:
        l_sectorcode = ['ALL', 'G', 'F', 'I', 'M69', 'C']
        l_kpi = ['total_growth_lastyear', 'positive_cash_flow_monthly',
             'positive_cash_flow_position', 'd2c_30d_mean']
        l_items = itertools.product(l_sectorcode, l_kpi)

        expected_count = event['parallel_parquet_to_dynamodb'][1]['parquet_to_dynamodb_eol']['ItemsCount']

        #return {'Success': all(list(map(lambda x:validate_marketing_endpoint(*x), l_items)))}
        return {"Success": validate_eol_endpoint(expected_count)}
    except:
        print(traceback.format_exc())
        return {'Success': False, 'message': traceback.format_exc()}

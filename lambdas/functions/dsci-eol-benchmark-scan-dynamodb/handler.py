import boto3
import os
import traceback

def lambda_handler(event, context):
    try:
        table_name = os.environ['BENCHMARK_EOL_DYNAMO_DB']
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(table_name)
        response = table.scan()
        items = response['Items']

        while 'LastEvaluatedKey' in response:
            response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            items.extend(response['Items'])

        benchmark_response=[]

        for i in items:
            benchmark={}
            benchmark["Sector"]=i["sector"]
            benchmark["SubSector"]=i["subsector"]
            benchmark["CompanySize"]=i["companysize"]
            benchmark["KPI"]=i["kpi"]
            benchmark["Population"]=int(i["population"])
            benchmark["Value"]=i["value"]
            benchmark_response.append(benchmark)

        return benchmark_response
    except:
        print(traceback.format_exc())
        return []
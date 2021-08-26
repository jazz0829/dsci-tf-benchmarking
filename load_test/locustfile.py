from locust import HttpLocust, TaskSet, task
import json

with open('./locust-config.json') as f:
    config = json.load(f)

class MyTaskSet(TaskSet):
    @task(1)
    def get(self):
        print(config['api_key'])
        print(config['target_host'])
        headers = {'content-type': 'application/json','x-api-key':config['api_key']}
        response  = self.client.get(config['target_host'], headers=headers)
        print(f"Response status code: {response.status_code}")

class MyLocust(HttpLocust):
    task_set = MyTaskSet
    host = config['target_host']
    min_wait = 500
    max_wait = 2000
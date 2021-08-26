# Locust based Load test
Follow the instructions below to run the load test on locust in standlaone mode.

### Get started!

  - Install locust locall by using pip
  - Make sure the locust-config.json has all the parameters configured correctly
  - cd to the folder where the locustfile.py file is located and run

### Configuration
- target_host: The API Gateway URL under load test
- api-key: x-api-key header value to authenticate the calls

### Installation

Loadtest requires [Locust](https://docs.locust.io/en/stable/) to run.

Install the dependencies and devDependencies and start the load test.

```sh
$ cd <projectfolder>/load_test
$ pip install locustio
$ locust -f locustfile.py
```

To actually simulate load
- Navigate to http://localhost:8089
- Start the locust swarm by entering in the number of desired users you wish to simulate
- Enter a hatch rate which is realistic and applicable to your scenario

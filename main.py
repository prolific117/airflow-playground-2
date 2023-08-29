import requests
from requests.auth import HTTPBasicAuth
import secrets
import string
from datetime import datetime, timezone, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import time
import pytz

# Obtain an OAuth access token from your provider (e.g., via an OAuth library)
# another option here is username + password
username = 'tunji@gmail.com'
password = 'tunji'

# Set the Authorization header with the access token
headers = {
    #'Authorization': 'Bearer ' + access_token,
    'Accept': 'application/json',
    'Content-Type': 'application/json',
}

base_url = f"http://localhost:8080/api/v1"

def runner(dag_id, dag_run_id):
    # API endpoint and DAG ID
    # this is the url of your airflow and the dag id
    api_endpoint = f"{base_url}/dags/{dag_id}/dagRuns"
    
    timestamp = (datetime.now(pytz.UTC) + timedelta(seconds=10)).strftime('%Y-%m-%dT%H:%M:%SZ')
    
    data = {'dag_run_id': dag_run_id, 'execution_date': timestamp}

    # Make a POST request with the appropriate headers to trigger the DAG
    response = requests.post(api_endpoint, auth=HTTPBasicAuth(username, password), headers=headers, json=data)

    # Check the response status code
    if response.status_code == 200:
        print(f"DAG {dag_id} with dag run id {dag_run_id} triggered successfully.")
    else:
        print(response.text)
        print(f"Failed to trigger the DAG {dag_id}. Status code:", response.status_code)

    return response.status_code

def check_dag_state(current_dag_id, current_dag_run_id):
    # Make the API request to fetch the DAG run
    state = ""
    while state not in ["success", "failed"]:
        # Make the API request to fetch the DAG run
        response = requests.get(f"{base_url}/dags/{current_dag_id}/dagRuns/{current_dag_run_id}", auth=HTTPBasicAuth(username, password), headers=headers)

        # Check the response status code
        if response.status_code == 200:
            dag_run = response.json()
            state = dag_run["state"]
            print(f"DAG {current_dag_id} run state: {state}")
        else:
            state = "failed"
            print(response.text)
            print(f"Error fetching DAG run for dag: {current_dag_id}, dag run id {dag_run_id}. Status code: {response.status_code}")

        # Sleep for a specified time interval (e.g., 30 seconds)
        time.sleep(30)

    return state

# DAG 1 
ingestion_dag_id = 'dataset_consumes_1'
ingestion_dag_run_id = ''.join(secrets.choice(string.digits) for _ in range(20))
ingestion_response  = runner(dag_id = ingestion_dag_id, dag_run_id = ingestion_dag_run_id)

if ingestion_response == 200:
    state = check_dag_state(ingestion_dag_id, ingestion_dag_run_id)

    if state == "success":
        # run dag 2
        dbt_dag = 'dataset_consumes_1_never_scheduled'
        dbt_dag_run_id = ''.join(secrets.choice(string.digits) for _ in range(20))
        dbt_dag_response = runner(dag_id = dbt_dag, dag_run_id = dbt_dag_run_id)

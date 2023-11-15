import requests
import os
from dotenv import load_dotenv

load_dotenv()


def run_job(access_token, job_id, server_host_name):
    """
    Trigger the Databricks NBA data pipeline job run
    """
    # Setup and make the api request
    url = f"https://{server_host_name}/api/2.0/jobs/run-now"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }
    data = {"job_id": job_id}
    response = requests.post(url, headers=headers, json=data, timeout=100)

    # Check the response status code
    if response.status_code == 200:
        print("Databricks job run successfully triggered....")
    else:
        print(f"Error: {response.status_code}, {response.text}")

    return response.status_code


if __name__ == "__main__":
    db_access_token = os.getenv("DATABRICKS_ACCESS_TOKEN")
    db_job_id = os.getenv("PIPELINE_JOB_ID")
    db_server_host_name = os.getenv("SERVER_HOSTNAME")
    run_job(db_access_token, db_job_id, db_server_host_name)

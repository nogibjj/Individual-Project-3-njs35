import os
from dotenv import load_dotenv
from main import run_job

load_dotenv()


def test_run_job():
    # Make request to Databricks API
    access_token = os.getenv("DATABRICKS_ACCESS_TOKEN")
    job_id = os.getenv("PIPELINE_JOB_ID")
    server_host_name = os.getenv("SERVER_HOSTNAME")

    # Check if environment variables are set
    assert access_token is not None, "DATABRICKS_ACCESS_TOKEN is not set."
    assert job_id is not None, "PIPELINE_JOB_ID is not set."
    assert server_host_name is not None, "SERVER_HOSTNAME is not set."

    response = run_job(access_token, job_id, server_host_name)

    assert response == 200

import os
from dotenv import load_dotenv
from main import run_job

load_dotenv()


def test_run_job():
    # Make request to Databricks API
    access_token = os.getenv("DATABRICKS_ACCESS_TOKEN")
    job_id = os.getenv("PIPELINE_JOB_ID")
    server_host_name = os.getenv("SERVER_HOSTNAME")
    response = run_job(access_token, job_id, server_host_name)

    assert response == 200

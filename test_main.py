import os
import requests
from dotenv import load_dotenv
from main import run_job

load_dotenv()


def test_env_vars():
    # Get environment variables
    access_token = os.getenv("DATABRICKS_ACCESS_TOKEN")
    job_id = os.getenv("PIPELINE_JOB_ID")
    server_host_name = os.getenv("SERVER_HOSTNAME")

    # Check if environment variables are set properly
    assert access_token is not None, "DATABRICKS_ACCESS_TOKEN is not set."
    assert job_id is not None, "PIPELINE_JOB_ID is not set."
    assert server_host_name is not None, "SERVER_HOSTNAME is not set."


def test_csv_data_source():
    url = "https://github.com/fivethirtyeight/data/raw/master/nba-draft-2015/historical_projections.csv"
    # Check if the CSV file is accessible
    response = requests.get(url)
    assert response.status_code == 200, "CSV file is not accessible."

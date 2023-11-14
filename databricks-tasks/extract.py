import requests
import os

def create_dir(dir_path):
    """
    Creates a new directory in the DBFS if needed.
    """
    try:
        # Check if the directory already exists
        if not os.path.exists(dir_path):
            # Create the directory
            os.makedirs(dir_path)
            print(f"Directory created: {dir_path}")
        else:
            print(f"Directory already exists: {dir_path}")
    except Exception as e:
        # Print the error and return None or handle it as needed
        print(f"Error creating directory: {e}")
        return None
    

def extract_csv_from_url(url, file_path):
    """
    Extracts a CSV file from the input url.
    Checks for proper data format and eliminates data overwrites.
    """
    try:
        # Check if the file already exists
        if os.path.exists(file_path):
            print(f"File already exists: {file_path}")
            return
        
        # Use requests to get the CSV content
        response = requests.get(url)
        csv_content = response.text.splitlines()

        # Replace spaces with underscores in the headers
        headers = csv_content[0].split(',')
        headers = [header.replace(' ', '_') for header in headers]

        # Update the header row in the CSV content
        csv_content[0] = ','.join(headers)

        # Write the modified CSV content to a CSV file
        with open(file_path, "w") as file:
            file.write("\n".join(csv_content))

        print(f"CSV file extracted and saved to: {file_path}")
    except Exception as e:
        # Print the error and return None or handle it as needed
        print(f"Error extracting CSV file: {e}")
        return None


if __name__ == "__main__":
    # URL of the CSV file and the path for the CSV file in the Databricks FileStore
    csv_url = "https://github.com/fivethirtyeight/data/raw/master/nba-draft-2015/historical_projections.csv"
    csv_file_path = "/dbfs/FileStore/csv/nba-draft.csv"
    csv_dir = "/dbfs/FileStore/csv"


    # Extract data
    create_dir(csv_dir)
    extract_csv_from_url(csv_url, csv_file_path)
import os
import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlparse
from time import sleep

current_path = os.getcwd()
output_folder = os.path.join(current_path, 'Annual_Reports')

if not os.path.exists(output_folder):
    os.makedirs(output_folder)

# Read data file
data = pd.read_excel(os.path.join(current_path, 'Annual_Reports.xlsx'))


# Function to download files and resources
def download_file(row):
    # Extract necessary data
    stock_code = str(row['Stock Code']).zfill(5)  # Ensure stock code is 5 digits
    release_time = pd.to_datetime(row['Release Time'], dayfirst=True)
    year = release_time.year - 1  # Use the previous year of the release time
    url = row['URL']

    # Skip HTML or HTM files
    if 'htm' in url.lower() or 'html' in url.lower():
        print(f"Skipping HTML file: {url}")
        return
    else:
        download_other_files(url, stock_code, year)


# Function to download and save files
def save_file(url, file_path):
    try:
        # Download the file
        response = requests.get(url)
        response.raise_for_status()  # Check if the request was successful

        # Save the file
        with open(file_path, 'wb') as f:
            f.write(response.content)
        print(f"Downloaded: {file_path}")
    except requests.exceptions.RequestException as e:
        print(f"Error downloading {url}: {e}")


# Download non-HTML files (PDF, DOC, etc.)
def download_other_files(url, stock_code, year):
    try:
        # Try to detect file type based on the URL's file extension
        file_extension = os.path.splitext(urlparse(url).path)[1]  # Default to URL's extension
        if not file_extension:  # If no extension in URL, fallback to .pdf
            file_extension = '.pdf'

        # Create file path
        filename = f"{stock_code}_{year}_Annual_Report{file_extension}"
        file_path = os.path.join(output_folder, filename)

        # Save the file
        save_file(url, file_path)

    except Exception as e:
        print(f"Error processing {url}: {e}")


# Use ThreadPoolExecutor to download files concurrently
def download_files_concurrently():
    with ThreadPoolExecutor(max_workers=10) as executor:
        executor.map(lambda row: download_file(row[1]), data.iterrows())
        # Add a delay between requests to avoid overwhelming the server
        sleep(0.5)  # Adjust sleep time if necessary


# Run the download function
download_files_concurrently()

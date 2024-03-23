import csv
import logging
import requests
import time
import retrying

# Set up logging
logging.basicConfig(filename="stamp_data.log", level=logging.INFO)

# List of asset IDs to retrieve data for
asset_ids = ["A4515906628890571300", "A1449397849937177992", "A1714800323048321163"]

# Base URL for the API endpoint
base_url = "https://stampchain.io/api/v2/stamps"

# Delay between requests (in seconds)
delay = 0.05

# List to store data for retrieved assets
assets = []

# Function to make a request to the API endpoint for a single asset ID
# @retrying.retry(stop_max_attempt_number=3, wait_fixed=1000)  # retry up to 5 times with a 1 second delay between retries
def get_asset_data(asset_id):
    logging.info(f"Fetching Asset: {asset_id}")
    url = f"{base_url}/{asset_id}"
    response = requests.get(url)

    # Check that the request was successful
    if response.status_code == 200:
        # Parse the JSON response
        json_response = response.json()

        # Access the data inside the "data" key
        if 'data' in json_response:  # Ensure the 'data' key exists
            data = json_response['data']

            # Extract the addresses from the 'holders' data
            if 'holders' in data:
                holders = data['holders']
                addresses = [holder['address'] for holder in holders]

                # Add the addresses to the data dictionary
                data['holders'] = addresses

            # Extract the stamp data
            if 'stamp' in data:
                stamp = data['stamp']

                # Return the data for the asset
                return stamp, data['holders']

        else:
            # Log a warning if the 'data' key is missing
            logging.warning(f"'data' key not found in response for asset {asset_id} (status code {response.status_code})")

    # Add the stamp and holders data to the assets list
    assets.append((stamp, holders))

# Call the get_asset_data() function for each asset ID
for asset_id in asset_ids:
    get_asset_data(asset_id)

# Write the data to a CSV file
with open("bosArt-stamp_data.csv", "w", newline="") as csvfile:
    fieldnames = ["stamp", "holders"]
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    for asset in assets:
        stamp, holders = asset
        writer.writerow({"stamp": stamp, "holders": holders})

# Log a message indicating that the script has completed
logging.info("Finished retrieving data and writing to CSV file")
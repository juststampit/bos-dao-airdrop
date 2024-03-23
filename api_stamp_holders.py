from tqdm import tqdm
import requests
import json
import csv
import logging

def get_collection_name():
    # Manually input the collection name for each run
    return input("Please enter the collection name: ")

# Set up logging to output to a file
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
log_file_handler = logging.FileHandler('nft_log.log')
log_file_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
logging.getLogger().addHandler(log_file_handler)

# Provide the array of NFT IDs here
stamp_ids = ['A1369210904326473420', 'A454092392577268841', 'A1087244158713077636', 'A995745323260604787', 'A1087244158713077636', 'A672631645343727476', 'A1233839408890899574', 'A1431903724482972971', 'A624016443443150761', 'A228700604904328280', 'A1122957051225484408', 'A212583391985849809', 'A582541865338232032', 'A1658502707259193937', 'A1111685225107137619', 'A1358429350926785447', 'A1204092088279684083', 'A212381486968599631', 'A499874687676276808', 'A133961632556144042', 'A1817302908167233539']
# Define a function to fetch the holder data from the API endpoint
def get_holder_data(stamp_id, collection_name):  # Add collection_name as a parameter
    url = f'https://stampchain.io/api/v2/stamps/{stamp_id}'
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        logging.info(f'Successfully fetched data for STAMP NFT ID {stamp_id} in collection {collection_name}')
        return data['data']['holders']
    except requests.exceptions.HTTPError as errh:
        logging.error(f'HTTP Error for STAMP NFT ID {stamp_id} in collection {collection_name}: {errh}')
    except requests.exceptions.ConnectionError as errc:
        logging.error(f'Error Connecting for STAMP NFT ID {stamp_id} in collection {collection_name}: {errc}')
    except requests.exceptions.Timeout as errt:
        logging.error(f'Timeout Error for STAMP NFT ID {stamp_id} in collection {collection_name}: {errt}')
    except requests.exceptions.RequestException as err:
        logging.error(f'Something went wrong for STAMP NFT ID {stamp_id} in collection {collection_name}: {err}')
    return []

# Define a function to extract the wallet addresses from the holder data
def extract_wallet_addresses(holder_data):
    addresses = []
    for holder in holder_data:
        address = holder['address']
        addresses.append(address)
    return addresses

# Define a function to write the wallet addresses to a CSV file
def write_to_csv(addresses, collection_name):
    with open(f'collections/holders/col-{collection_name}_holders.csv', 'a', newline='') as csvfile:
        fieldnames = ['address']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        if csvfile.tell() == 0:
            writer.writeheader()
        for address in addresses:
            writer.writerow({'address': address})

# Call the functions for each NFT ID in the array, now with tqdm for progress tracking
wallet_addresses = []
collection_name = get_collection_name()  # Get collection name once at the beginning

for stamp_id in tqdm(stamp_ids, desc="Querying NFT IDs"):
    logging.info(f'Fetching holder data for STAMP NFT ID {stamp_id}...')
    holder_data = get_holder_data(stamp_id, collection_name)  # Pass collection_name to the function
    if holder_data:
        logging.info(f'Extracting wallet addresses from holder data for {collection_name}...')
        addresses = extract_wallet_addresses(holder_data)
        logging.info(f'Writing {len(addresses)} wallet addresses to CSV file for {collection_name}...')
        write_to_csv(addresses, collection_name)
        wallet_addresses.extend(addresses)
    else:
        logging.warning(f'No holder data found for STAMP NFT ID {stamp_id}')

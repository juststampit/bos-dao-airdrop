import requests
import json
import csv
import logging
from pprint import pprint
from tqdm import tqdm


def get_token_info():
    # Manually input the collection name for each run
    return input("Please enter the SRC20 ticker: ")

# Set up logging to output to a file
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
log_file_handler = logging.FileHandler('nft_log.log')
log_file_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
logging.getLogger().addHandler(log_file_handler)

# Provide the array of NFT IDs here
# src20_ids = ['A1369210904326473420', 'A454092392577268841', 'A1087244158713077636', 'A995745323260604787', 'A1087244158713077636', 'A672631645343727476', 'A1233839408890899574', 'A1431903724482972971', 'A624016443443150761', 'A228700604904328280', 'A1122957051225484408', 'A212583391985849809', 'A582541865338232032', 'A1658502707259193937', 'A1111685225107137619', 'A1358429350926785447', 'A1204092088279684083', 'A212381486968599631', 'A499874687676276808', 'A133961632556144042', 'A1817302908167233539']

# Define a function to fetch the holder data from the API endpoint
def get_holder_data(src20_id):
    page_size = 500
    page_number = 1
    data = []
    total_items = 0
    
    url = f'https://openstamp.io/api/v1/explorer/src20/holdersByTick?tick={src20_id}&page={page_number}&pageSize={page_size}'
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()['data']['list']
        total_items = response.json()['data']['total']
        
        while (page_number - 1) * page_size < total_items:
            url = f'https://openstamp.io/api/v1/explorer/src20/holdersByTick?tick={src20_id}&page={page_number}&pageSize={page_size}'
            
            logging.info(f'Fetching page {page_number}...')
            response = requests.get(url)
            response.raise_for_status()
            
            data.extend(response.json()['data']['list'])
            
            logging.info(f'Successfully fetched data for SRC20 TOKEN {src20_id} (Page {page_number})')
            page_number += 1
            
        return data
    except requests.exceptions.HTTPError as errh:
        logging.error(f'HTTP Error for SRC20 TOKEN {src20_id}: {errh}')
    except requests.exceptions.ConnectionError as errc:
        logging.error(f'Error Connecting for SRC20 TOKEN {src20_id}: {errc}')
    except requests.exceptions.Timeout as errt:
        logging.error(f'Timeout Error for SRC20 TOKEN {src20_id}: {errt}')
    except requests.exceptions.RequestException as err:
        logging.error(f'Something went wrong for SRC20 TOKEN {src20_id}: {err}')
    
    return []

# Define a function to extract the wallet addresses from the holder data
def extract_wallet_data(holder_data):
    data_to_store = []
    for holder in holder_data:
        data_entry = {
            'address': holder['address'],
            'balance': holder['balance'],
            'blockHeight': holder['blockHeight']
        }
        data_to_store.append(data_entry)
    return data_to_store
# Define a function to write the wallet addresses to a CSV file
def write_to_csv(data_to_store, src20_id):
    with open(f'staging-{src20_id}_holders.csv', 'a', newline='') as csvfile:
        fieldnames = ['address', 'balance', 'blockHeight']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        if csvfile.tell() == 0:
            writer.writeheader()
        for data_entry in data_to_store:
            writer.writerow(data_entry)

src20_id = get_token_info()

logging.info(f'Fetching holder data for SRC20 TOKEN {src20_id}...')

# Update the script to use the new functions
holder_data = get_holder_data(src20_id)
print(holder_data)


if holder_data:
    # Save holder_data to a text file
    with open(f'temp_holder_data_{src20_id}.txt', 'w') as file:
        file.write(str(holder_data))

    logging.info(f'Extracting wallet data for token {src20_id}...')
    data_to_store = extract_wallet_data(holder_data)
    logging.info(f'Writing {len(data_to_store)} wallet data entries to CSV file for {src20_id}...')
    write_to_csv(data_to_store, src20_id)
else:
    logging.warning(f'No holder data found for SRC20 TOKEN {src20_id}')
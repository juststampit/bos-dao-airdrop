import glob
import json
import csv
import logging
from tqdm import tqdm

def scrape_addresses_and_save_to_csv():
    # Set up logging to output to a file
    logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
    log_file_handler = logging.FileHandler('scraping_log.log')
    log_file_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
    logging.getLogger().addHandler(log_file_handler)

    # Pattern to match files starting with "fetchSRC20_" and ending with ".json"
    file_pattern = 'fetchSRC20_*.json'

    # Use tqdm to show progress
    files = glob.glob(file_pattern)
    for file_path in tqdm(files, desc="Processing JSON files"):
        token_name = file_path.split('_')[1].split('.')[0]
        csv_file_name = f'src-{token_name}_holders.csv'

        addresses = set()

        with open(file_path, 'r') as file:
            data = json.load(file)
            for entry in data:
                creator = entry.get('creator')
                destination = entry.get('destination')
                if creator:
                    addresses.add(creator)
                if destination:
                    addresses.add(destination)

        with open(csv_file_name, 'w', newline='') as csvfile:
            fieldnames = ['address']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for address in addresses:
                writer.writerow({'address': address})

        logging.info(f'Saved {len(addresses)} addresses to {csv_file_name}')

# Don't forget to call the function
scrape_addresses_and_save_to_csv()
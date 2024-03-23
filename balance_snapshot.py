import requests
import logging
from tqdm import tqdm
import pandas as pd
import csv  # Add this import at the top of your file
import concurrent.futures
import os  # Import os to check file existence for appending CSV

# Set up logging to output to a file
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
log_file_handler = logging.FileHandler('wallet_snapshot_log.log')
log_file_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
logging.getLogger().addHandler(log_file_handler)

whitelist = ['$viva', 'bos', 'kevin', 'spad', 'stamp', 'stmap', 'utxo']

def take_snapshot(wallet_addresses, checkpoint_interval=200):
    snapshot_results = []
    wallet_address_list = list(wallet_addresses)  # Convert the generator to a list
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        futures = {executor.submit(get_snapshot, wallet_address): wallet_address for wallet_address in wallet_address_list}
        for future in tqdm(concurrent.futures.as_completed(futures), total=len(futures), desc="Taking snapshots"):
            wallet_address = futures[future]
            try:
                snapshot_data = future.result()
                if snapshot_data:
                    snapshot_results.append(snapshot_data)
                    logging.info(f'Successfully retrieved snapshot for wallet address {wallet_address}')
                # Checkpoint: Save every X snapshots
                if len(snapshot_results) % checkpoint_interval == 0:
                    save_snapshots_to_csv(snapshot_results, 'partial_balances_snapshot_src20.csv')
            except Exception as exc:
                logging.error(f'Error fetching snapshot for {wallet_address}: {exc}')
                continue
    # Final save
    save_snapshots_to_csv(snapshot_results, 'balances_snapshot_src20.csv')
    return snapshot_results

def get_snapshot(wallet_address):
    url = f'https://stampchain.io/api/v2/src20/balance/{wallet_address}'
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raises HTTPError for bad responses
        snapshot_data = response.json()
        if 'error' in snapshot_data:
            logging.error(f'Error generating snapshot for wallet address {wallet_address}: {snapshot_data["error"]}')
        return snapshot_data
    except requests.exceptions.HTTPError as errh:
        logging.error(f'HTTP Error for wallet address {wallet_address}: {errh}')
    except requests.exceptions.ConnectionError as errc:
        logging.error(f'Error Connecting for wallet address {wallet_address}: {errc}')
    except requests.exceptions.Timeout as errt:
        logging.error(f'Timeout Error for wallet address {wallet_address}: {errt}')
    except requests.exceptions.RequestException as err:
        logging.error(f'Something went wrong for wallet address {wallet_address}: {err}')
    return None

def save_snapshots_to_csv(snapshot_results, output_file_path):
    # Filter snapshot_results to include only those with a tick in the whitelist
    filtered_snapshot_results = [item for item in snapshot_results if 'data' in item for data_item in item['data'] if data_item.get('tick') in whitelist]

    # Convert the filtered list of dictionaries to a DataFrame
    df_nested = pd.json_normalize(filtered_snapshot_results, record_path=['data'], sep='_')
    
    try:
        # Adjust these column names based on the actual, verified structure
        df_final = df_nested[['address', 'tick', 'amt', 'block_time']]
        df_final.columns = ['Address', 'Ticker', 'Amount', 'Block Time']
        
        # Convert 'Amount' to float and round to 2 decimal places, then convert back to string if needed
        df_final.loc[:, 'Amount'] = df_final['Amount'].astype(float).round(2).astype(str)
        
    except KeyError as e:
        logging.error(f"Column not found: {e}")
        return
    # Check if the file exists to decide on writing headers or not
    file_exists = os.path.isfile(output_file_path)
    
    # Open the file in append mode ('a') and write the dataframe
    with open(output_file_path, 'a', newline='', encoding='utf-8') as f:
        df_final.to_csv(f, header=not file_exists, index=False, quoting=csv.QUOTE_ALL)
    
    logging.info(f'Snapshot results appended to {output_file_path}')

def process_wallet_addresses(addresses_file):
    """Process wallet addresses from a file """
    with open(addresses_file, 'r') as file:
        wallet_addresses = [line.strip() for line in file if line.strip()]
    return wallet_addresses

if __name__ == "__main__":
    wallet_addresses = process_wallet_addresses('./combined_btc_addresses_rd2.txt')
    snapshots = take_snapshot(wallet_addresses)
    save_snapshots_to_csv(snapshots, 'balances_snapshot_src20_rd2.csv')

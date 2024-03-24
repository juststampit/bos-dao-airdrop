import requests
import logging
from tqdm import tqdm
import pandas as pd
import csv  # Add this import at the top of your file
import concurrent.futures
import os  # Import os to check file existence for appending CSV
import fileinput
import time  # Add this import at the top of your file
import threading
import colorlog

csv_lock = threading.Lock()

# Set up colored logging
logger = colorlog.getLogger()
logger.setLevel(colorlog.INFO)
handler = colorlog.StreamHandler()
handler.setFormatter(colorlog.ColoredFormatter(
    '%(log_color)s%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    log_colors={
        'DEBUG': 'cyan',
        'INFO': 'green',
        'WARNING': 'yellow',
        'ERROR': 'red',
        'CRITICAL': 'red,bg_white',
    }
))
logger.addHandler(handler)

whitelist = ['$viva', 'bos', 'kevin', 'spad', 'stamp', 'stmap', 'utxo']

def take_snapshot(wallet_addresses, checkpoint_interval=25):
    all_snapshot_results = []  # Keep all snapshots for final save
    temp_snapshot_results = []  # Temporary list for checkpoint saves
    wallet_address_list = list(wallet_addresses)  # Convert the generator to a list
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(get_snapshot, wallet_address): wallet_address for wallet_address in wallet_address_list}
        for future in tqdm(concurrent.futures.as_completed(futures), total=len(futures), desc="Taking snapshots"):
            wallet_address = futures[future]
            try:
                snapshot_data = future.result()
                if snapshot_data:
                    # Remove duplicated data
                    snapshot_data = [item for item in snapshot_data if item not in all_snapshot_results and item not in temp_snapshot_results]
                    all_snapshot_results.extend(snapshot_data)
                    temp_snapshot_results.extend(snapshot_data)
                    logger.info(f'Successfully retrieved snapshot for wallet address {wallet_address}')
                # Checkpoint: Save every X snapshots and clear temp_snapshot_results
                if len(temp_snapshot_results) >= checkpoint_interval:
                    save_snapshots_to_csv(temp_snapshot_results, 'balances_snapshot_src20-v3.csv')
                    temp_snapshot_results.clear()  # Clear the temporary list after saving
            except Exception as exc:
                logger.error(f'Error fetching snapshot for {wallet_address}: {exc}')
                continue
    # Final save for any remaining snapshots not saved at the last checkpoint
    if temp_snapshot_results:
        save_snapshots_to_csv(temp_snapshot_results, 'balances_snapshot_src20-v3.csv')
    return all_snapshot_results

def get_snapshot(wallet_address):
    url = f'https://stampchain.io/api/v2/src20/balance/{wallet_address}'
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raises HTTPError for bad responses
        snapshot_data = response.json()
        if 'error' in snapshot_data:
            logger.error(f'Error generating snapshot for wallet address {wallet_address}: {snapshot_data["error"]}')
        return snapshot_data
    except requests.exceptions.HTTPError as errh:
        logger.error(f'HTTP Error for wallet address {wallet_address}: {errh}')
    except requests.exceptions.ConnectionError as errc:
        logger.error(f'Error Connecting for wallet address {wallet_address}: {errc}')
    except requests.exceptions.Timeout as errt:
        logger.error(f'Timeout Error for wallet address {wallet_address}: {errt}')
    except requests.exceptions.RequestException as err:
        logger.error(f'Something went wrong for wallet address {wallet_address}: {err}')
    return None

def save_snapshots_to_csv(snapshot_results, output_file_path):
    # Filter snapshot_results to include only those with a tick in the whitelist
    filtered_snapshot_results = [
        item for item in snapshot_results 
        if isinstance(item, dict) and 'data' in item 
        and isinstance(item['data'], list) 
        for data_item in item['data'] 
        if isinstance(data_item, dict) and data_item.get('tick') in whitelist
    ]
    # Convert the filtered list of dictionaries to a DataFrame
    df_nested = pd.json_normalize(filtered_snapshot_results, record_path=['data'], sep='_')

    try:
        # Adjust these column names based on the actual, verified structure
        df_final = df_nested[['address', 'tick', 'amt', 'block_time']]
        df_final.columns = ['Address', 'Ticker', 'Amount', 'Block Time']
        
        # Convert 'Amount' to float and round to 2 decimal places, then convert back to string if needed
        df_final.loc[:, 'Amount'] = df_final['Amount'].astype(float).round(2).astype(str)
        
        # Log a sample of the DataFrame
        logger.info(f"Sample of data being saved:\n{df_final.head()}")

    except KeyError as e:
        logger.error(f"Column not found: {e}")
        return  # Add a return statement to exit the function in case of an error
    except Exception as e:
        logger.error(f"Error processing DataFrame: {e}")
        return  # Add a return statement to exit the function in case of an error

    with csv_lock:
        try:
            # Check if the file exists to determine whether to write headers
            file_exists = os.path.exists(output_file_path)
            df_final.to_csv(output_file_path, mode='a' if file_exists else 'w', header=not file_exists, index=False, quoting=csv.QUOTE_ALL)
            logger.info(f'Snapshot results saved to {output_file_path}')

            time.sleep(2.5)
        except Exception as e:
            logger.error(f"Error saving to CSV: {e}")

def process_wallet_addresses(addresses_file):
    """Process wallet addresses from a file """
    with open(addresses_file, 'r') as file:
        wallet_addresses = [line.strip() for line in file if line.strip()]
    return wallet_addresses

if __name__ == "__main__":
    wallet_addresses = process_wallet_addresses('./combined_btc_addresses.txt')
    snapshots = take_snapshot(wallet_addresses)
    save_snapshots_to_csv(snapshots, 'balances_snapshot_src20-v3.csv')

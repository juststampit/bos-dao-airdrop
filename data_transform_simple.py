import pandas as pd
import json
import time
import glob

def jsons_to_csv(identifier, csv_file, verbose=True):
    # Use glob to find all files that match the identifier pattern
    json_files = glob.glob(f"{identifier}*.json")
    
    # Initialize an empty list to store dataframes
    dfs = []
    
    # Loop through each file in the list
    for json_file in json_files:
        with open(json_file) as f:
            data = json.load(f)
            # Remove the 'stamp_base64' field from each item in data if it exists
            for item in data:
                item.pop('stamp_base64', None)
            # Convert each file's data into a DataFrame and append to the list
            dfs.append(pd.DataFrame(data))

    # Concatenate all DataFrames in the list if not empty
    if dfs:
        df = pd.concat(dfs, ignore_index=True)
        df.to_csv(csv_file, index=False, escapechar="\\")
        if verbose:
            print(f"Saved {len(df)} rows to {csv_file}")
    else:
        if verbose:
            print("No data to save.")


# Example usage
identifier = 'fetchAllStamps'  # This will match any file that starts with 'fetchSRC20_' in the current directory
csv_file = 'allStampsData_2a.csv'
verbose = True

start_time = time.time()
jsons_to_csv(identifier, csv_file, verbose=verbose)
elapsed_time = time.time() - start_time
if verbose:
    print(f"Elapsed time: {elapsed_time:.2f} seconds")
import pandas as pd
import json
import time

# Function to convert JSON data to a CSV file
def json_to_csv(json_file, csv_file, chunksize=None, verbose=True):
    """
    Convert JSON data to a CSV file using pandas.

    Parameters
    ----------
    json_file : str
        Path to the JSON file.
    csv_file : str
        Path to the CSV file.
    chunksize : int, optional
        Number of rows to read at a time. If None, the entire JSON file will be loaded into memory.
    verbose : bool, optional
        Whether to print progress messages.

    Returns
    -------
    None

    """
    # Read the JSON data
    if chunksize is None:
        # If chunksize is None, read the entire JSON file into memory
        with open(json_file) as f:
            data = json.load(f)
        df = pd.DataFrame(data)
    else:
        # If chunksize is not None, read the JSON file in chunks
        df_list = []
        with open(json_file) as f:
            for i, chunk in enumerate(pd.read_json(f, lines=True, chunksize=chunksize)):
                df_list.append(chunk)
                if verbose:
                    print(f"Read chunk {i+1} of {chunksize}...")
        df = pd.concat(df_list)

    # Write the DataFrame to a CSV file
    df.to_csv(csv_file, index=False)
    if verbose:
        print(f"Saved {len(df)} rows to {csv_file}")

# Example usage
json_file = 'fetchSRC20_BOS_4P.json'
csv_file = 'fetchSRC20_BOS_4P.csv'
chunksize = 10000
verbose = True

start_time = time.time()
json_to_csv(json_file, csv_file, chunksize=chunksize, verbose=verbose)
elapsed_time = time.time() - start_time
if verbose:
    print(f"Elapsed time: {elapsed_time:.2f} seconds")
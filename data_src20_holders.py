import csv
import re
import os
from collections import defaultdict
from glob import glob
import argparse

# Parse command-line arguments
parser = argparse.ArgumentParser(description='Process SRC20 holder addresses.')
parser.add_argument('--dry-run', action='store_true', help='Run the script in dry-run mode to process only a sample of the data.')
args = parser.parse_args()

# Create a nested defaultdict to store data about each address
# Now also storing balance along with low_block and high_block
address_data = defaultdict(lambda: defaultdict(lambda: {'balance': 0, 'low_block': float('inf'), 'high_block': 0}))

# Directory containing the CSV files
data_dir = './data/src20_holders/'

# Pattern to match files starting with "staging-"
file_pattern = os.path.join(data_dir, 'staging-*.csv')

# Set to collect all unique collection names
all_src20s = set()

# Open a file to log invalid addresses
with open('./data/logs/src20_holders.invalid.log', 'w') as invalid_log:
    # Iterate over all files that match the pattern
    for filename in glob(file_pattern):
        # Extract collection name from filename
        src20_name = os.path.basename(filename).split('_holders.csv')[0][8:]
        all_src20s.add(src20_name)  # Add collection name to the set
        with open(filename, 'r') as infile:
            reader = csv.DictReader(infile)
            for row in reader:
                address = row['address'].strip()
                balance = float(row['balance'])  # Assuming balance is a numeric value
                block_height = int(row['blockHeight'])
                # if not re.match(r'^(bc1|[13])[a-zA-HJ-NP-Z0-9]{25,39}$', address):
                #     print(f"Invalid address format in file {filename}: {address}")
                #     invalid_log.write(f"{filename}: {address}\n")
                #     continue
                # Update balance, low_block, and high_block for each address
                address_data[address][src20_name]['balance'] += balance
                address_data[address][src20_name]['low_block'] = min(address_data[address][src20_name]['low_block'], block_height)
                address_data[address][src20_name]['high_block'] = max(address_data[address][src20_name]['high_block'], block_height)

if args.dry_run:
    # Print results to the terminal in dry-run mode
    for address, data in address_data.items():
        print(f"Address: {address}")
        for token, details in data.items():
            print(f"  {token}: {details}")
else:
    # Write the output CSV file
    with open('./data/src20_holders/all_holders.merged.csv', 'w', newline='') as outfile:
        # Ensure all collection names are included in fieldnames
        fieldnames = ['address'] + sorted(all_src20s) + ['lowest_blockHeight', 'highest_blockHeight']
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()
        for address, data in address_data.items():
            row = {'address': address}
            # Initialize lowest and highest blockHeight across all tokens for the address
            lowest_block = float('inf')
            highest_block = 0
            for name, details in data.items():
                if name != '_total':
                    row[name] = details['balance']  # Insert balance instead of count
                    lowest_block = min(lowest_block, details['low_block'])
                    highest_block = max(highest_block, details['high_block'])
            # Update row with lowest and highest blockHeight if they were updated
            row['lowest_blockHeight'] = lowest_block if lowest_block != float('inf') else 0
            row['highest_blockHeight'] = highest_block
            writer.writerow(row)
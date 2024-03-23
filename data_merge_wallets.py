import os
import csv
import re

def extract_valid_btc_addresses(folder_path):
    btc_addresses = set()

    for file_name in os.listdir(folder_path):
        if file_name.startswith("col-") or file_name.startswith("src-"):
            file_path = os.path.join(folder_path, file_name)
            with open(file_path, 'r') as csvfile:
                reader = csv.reader(csvfile)
                for row in reader:
                    for address in row:
                        if re.match(r'^(bc1|[13])[a-zA-HJ-NP-Z0-9]{25,39}$', address):
                            btc_addresses.add(address)

    return list(btc_addresses)

# Folder path to scan for CSV files
folder_path = './collections/'

# Extract valid BTC addresses from CSV files in the specified folder
btc_addresses = extract_valid_btc_addresses(folder_path)

# Write the unique BTC addresses to a new file
with open('./collections/col-MERGED.txt', 'w') as output_file:
    for address in btc_addresses:
        output_file.write(address + '\n')
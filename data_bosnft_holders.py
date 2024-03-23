import csv
from collections import defaultdict
import re

# Create a defaultdict to count the occurrences of each address
address_count = defaultdict(int)

# Open a file to log invalid addresses
with open('./data/collections/col-bos_nft_holders.invalid.log', 'w') as invalid_log:
    # Read the input CSV file
    with open('./data/collections/col-bos_nft_holders.csv', 'r') as infile:
        reader = csv.reader(infile)
        next(reader)  # Skip the header row
        for row in reader:
            # Strip leading/trailing whitespace and validate the address format
            address = row[0].strip()
            if not re.match(r'^(bc1|[13])[a-zA-HJ-NP-Z0-9]{25,39}$', address):
                print(f"Invalid address format: {address}")
                invalid_log.write(f"{address}\n")
                continue
            address_count[address] += 1

# Write the output CSV file
with open('./data/collections/col-bos_nft_holders.counted.csv', 'w', newline='') as outfile:
    fieldnames = ['address', 'count']
    writer = csv.DictWriter(outfile, fieldnames=fieldnames)
    writer.writeheader()
    for address, count in address_count.items():
        writer.writerow({'address': address, 'count': count})

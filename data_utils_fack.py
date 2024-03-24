import csv
import re

def is_p2wsh_address(address):
    # P2WSH addresses start with bc1 and can be up to 90 characters long.
    pattern = r'^bc1[a-zA-HJ-NP-Z0-9]{25,90}$'
    return re.match(pattern, address) is not None

def clean_csv_file(file_path):
    print(f"Processing {file_path}...")
    cleaned_data = []
    seen = set()
    
    with open(file_path, 'r', newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        original_fieldnames = reader.fieldnames
        # Ensure 'eligible' is included in the fieldnames
        fieldnames = original_fieldnames + ['eligible'] if 'eligible' not in original_fieldnames else original_fieldnames
        
        for row in reader:
            address = row['address']
            # Check if the address is a P2WSH address
            if is_p2wsh_address(address):
                row['eligible'] = 'No'  # Mark as ineligible
            else:
                row['eligible'] = 'Yes'  # Mark as eligible
            
            # Iterate over each column in the row
            for key in row.keys():
                # Assuming asset balance columns follow a specific naming pattern or are all columns except 'address', 'blockHeight', and 'eligible'
                if key not in ['address', 'blockHeight', 'eligible']:
                    # Ensure asset balance is set to '0' if it's empty or null
                    row[key] = row[key] or '0'
            
            # Create a unique key based on address and all asset balances
            unique_key = tuple([row['address']] + [row[key] for key in row.keys() if key not in ['address', 'blockHeight', 'eligible']])
            if unique_key not in seen:
                seen.add(unique_key)
                cleaned_data.append(row)
    
    # Write the cleaned data back to the file, including the eligibility column
    with open(file_path, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(cleaned_data)

# Path to the specific CSV file
file_path = './data/src20_holders/all_holders.merged.csv'
clean_csv_file(file_path)
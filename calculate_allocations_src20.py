import csv

def calculate_airdrop_allocations(file_path):
    # Define the assets and their corresponding allocation amounts
    assets = ['STAMP', 'KEVIN', 'UTXO', 'STMAP', 'VIVA', 'SATO']
    asset_allocation = 3000  # Allocation amount per asset
    bonus_asset = 'SPAD'
    bonus_allocation = 9000  # Bonus allocation for holding SPAD
    
    # Prepare the output data structure
    allocations = []

    with open(file_path, 'r', newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            address = row['address']
            # Initialize the allocation amount for the address
            allocation_amount = 0
            # Check for each asset and add to the allocation amount if present
            for asset in assets:
                if asset in row and float(row[asset]) > 0:
                    allocation_amount += asset_allocation
            # Check for the bonus asset
            if bonus_asset in row and float(row[bonus_asset]) > 0:
                allocation_amount += bonus_allocation
            # Store the allocation amount for the address
            allocations.append({'address': address, 'allocation': allocation_amount})
    
    # Write the allocation amounts to a new CSV file
    output_file_path = file_path.replace('.csv', '_allocations.csv')
    with open(output_file_path, 'w', newline='') as csvfile:
        fieldnames = ['address', 'allocation']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(allocations)
    
    print(f"Allocation amounts written to {output_file_path}")

# Path to the specific CSV file
file_path = './data/src20_holders/all_holders.merged.csv'
calculate_airdrop_allocations(file_path)
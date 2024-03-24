import pandas as pd
import csv

def remove_columns_from_csv(csv_file, columns_to_remove, output_csv_file=None):
    """
    Removes specified columns from a CSV file and saves the result.

    Parameters:
    - csv_file: Path to the input CSV file.
    - columns_to_remove: List of column names to be removed.
    - output_csv_file: Path to the output CSV file. If None, overwrites the input file.

    Returns:
    None
    """
    # Load the dataset
    df = pd.read_csv(csv_file)
    
    # Remove the specified columns
    df.drop(columns=columns_to_remove, errors='ignore', inplace=True)
    
    # Determine the output file path
    if output_csv_file is None:
        output_csv_file = csv_file
    
    # Save the modified DataFrame back to a CSV file
    df.to_csv(output_csv_file, index=False)
    print(f"File saved successfully to {output_csv_file} with specified columns removed.")


def remove_hourly_data_from_column(csv_file, column_name, output_csv_file=None):
    """
    Removes hourly data (everything after "T") from a specified column in a CSV file and saves the result.

    Parameters:
    - csv_file: Path to the input CSV file.
    - column_name: Name of the column to modify.
    - output_csv_file: Path to the output CSV file. If None, overwrites the input file.

    Returns:
    None
    """
    # Load the dataset
    df = pd.read_csv(csv_file)
    
    # Modify the specified column to remove hourly data
    df[column_name] = df[column_name].apply(lambda x: x.split('T')[0])
    
    # Determine the output file path
    if output_csv_file is None:
        output_csv_file = csv_file
    
    # Save the modified DataFrame back to a CSV file
    df.to_csv(output_csv_file, index=False)
    print(f"File saved successfully to {output_csv_file} with hourly data removed from {column_name}.")

def convert_amount_to_int(csv_file, column_name='Amount', output_csv_file=None):
    """
    Converts the values in the "Amount" column from float to int in a CSV file and saves the result.

    Parameters:
    - csv_file: Path to the input CSV file.
    - column_name: Name of the column to modify, default is 'Amount'.
    - output_csv_file: Path to the output CSV file. If None, overwrites the input file.

    Returns:
    None
    """
    # Load the dataset
    df = pd.read_csv(csv_file)
    
    # Convert the specified column to int
    df[column_name] = df[column_name].astype(int)
    
    # Determine the output file path
    if output_csv_file is None:
        output_csv_file = csv_file
    
    # Save the modified DataFrame back to a CSV file
    df.to_csv(output_csv_file, index=False)
    print(f"File saved successfully to {output_csv_file} with {column_name} converted to int.")

# Example usage (uncomment the following lines to test)
# csv_file = 'merged_SRC20_data.csv'
# columns_to_remove = ['id', 'tick_hash', 'creator', 'deci', 'lim', 'max', 'locked_amt', 'locked_block', 'creator_bal', 'creator_name', 'destination_name']
# remove_columns_from_csv(csv_file, columns_to_remove)

def add_count_unique_column(input_file_path, output_file_path):
    """
    Adds a 'count_unique' column to a CSV file that counts the unique collections per address.

    Parameters:
    - input_file_path: Path to the existing CSV file.
    - output_file_path: Path to the new (or the same) CSV file with the added 'count_unique' column.
    """

    # Read the existing data
    with open(input_file_path, mode='r', newline='') as file:
        reader = csv.DictReader(file)
        data = list(reader)
        # Assuming the first column is 'address' and the last is 'count__total'
        fieldnames = reader.fieldnames + ['count_unique']  # Add 'count_unique' to the list of fieldnames

    # Calculate count_unique for each row
    for row in data:
        # Count how many collections each address is in, excluding 'address' and 'count__total' columns
        unique_collections = sum(1 for key, value in row.items() if key.startswith('count_') and key != 'count__total' and int(value) > 0)
        row['count_unique'] = unique_collections

    # Write the updated data to a new CSV file
    with open(output_file_path, mode='w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)


#input_file_path = './data/collections/col-all_nft_holders.collection_count.csv'
#output_file_path = './data/collections/col-all_nft_holders.collection_count.final.csv'
#add_count_unique_column(input_file_path, output_file_path)

def count_unique_addresses(csv_file_path, column_name='destination'):
    """
    Counts the number of unique addresses in a specified column of a CSV file.

    Parameters:
    - csv_file_path: Path to the CSV file.
    - column_name: Name of the column containing addresses. Defaults to 'destination'.

    Returns:
    The number of unique addresses.
    """
    unique_addresses = set()

    try:
        with open(csv_file_path, mode='r', encoding='utf-8') as csv_file:
            csv_reader = csv.DictReader(csv_file)
            
            for row in csv_reader:
                address = row[column_name]
                unique_addresses.add(address)
                
        print(f"Total unique addresses in column '{column_name}': {len(unique_addresses)}")
        return len(unique_addresses)
    except FileNotFoundError:
        print(f"File not found: {csv_file_path}")
        return None
    except KeyError:
        print(f"Column '{column_name}' does not exist in the CSV file.")
        return None

csv_file_path = './data/merged_SRC20_data_prepped.csv'
column_name = 'destination'  # Adjust the column name as needed
count_unique_addresses(csv_file_path, column_name)

#csv_file = 'final_balances_snapshot_src20.csv'
# column_name = 'Block Time'
# remove_hourly_data_from_column(csv_file, column_name)
#convert_amount_to_int(csv_file)

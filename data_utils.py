import pandas as pd

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

csv_file = 'final_balances_snapshot_src20.csv'
# column_name = 'Block Time'
# remove_hourly_data_from_column(csv_file, column_name)
convert_amount_to_int(csv_file)

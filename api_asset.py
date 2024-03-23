from tqdm import tqdm
import requests
import json
import csv
import logging


# Set up logging to output to a file
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
log_file_handler = logging.FileHandler('nft_log.log')
log_file_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
logging.getLogger().addHandler(log_file_handler)

# Provide the array of NFT IDs here
stamp_ids = ["A670945986851746376", "A11677883691826442144", "A16153129961778252046", "A7705178439046848221", "A2094261641901868047", "A7769027528277942227", "A15970822181156540906", "A16847951255226243427", "A6514021031863757452", "A17051133627381506641", "A9937207344008675000", "A17790972678853323078", "A1397990226586964852", "A15875017493938280172", "A14081981675717497000", "A11113361676954898000", "A11067508925308063735", "A12650419581474556000", "A13645802522989998128", "A14224827358161296686", "A14571539890130820786", "A14027205259384663537", "A12236109058931544000", "A10835789004916134063", "A1526129941379416693", "A15300339219733429682", "A15865692368416380000", "A2872108195460527600", "A3129394035950379000", "A3204416779555954000", "A524963645881041655", "A5497718381522320000", "A5900800532160602000", "A781030276237811600", "A8096760539403132000", "A849765072772731617", "A894157316546797589", "A9859380993329100000", "A14370551717893044000", "A5252594366990739000", "A11369437878122220845", "A6616193069324940000", "A16967247672378074530", "A3944934309963272700", "A1186517576097865372", "A1463910612295399774", "A1451999256382920035", "A1110050872345550048", "A201958828097511334", "A290381630342616679", "A1130824321920642810", "A17034173491729061864", "A6132960741114233000", "A6669168558299443000", "A6701432453749743000", "A1617635266229289134", "A877561162786236924", "A293554703980060224", "A1314106240414291460", "A834048251229341149", "A215016210451610235", "A517301080302283367", "A829607875113324600", "A1088448628465452866", "A848059089374596035", "A1137038081874001885", "A1463019422005547679", "A991687113456599461", "A1644051087433137775", "A454928595107763512", "A1162918520108425665", "A693574227815644635", "A1037387860494896069", "A445960098736817255", "A146241674251905267", "A1769913752874513189", "A1754284611905702369"]
def get_collection_name():
    # Manually input the collection name for each run
    return "vivalastamps"

# Define a function to fetch the holder data from the API endpoint
def get_holder_data(stamp_id):
    url = f'https://stampchain.io/api/v2/stamps/{stamp_id}'
    collection_name = get_collection_name()  # Get collection name for the log
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        logging.info(f'Successfully fetched data for STAMP NFT ID {stamp_id} in collection {collection_name}')
        return data['data']['holders']
    except requests.exceptions.HTTPError as errh:
        logging.error(f'HTTP Error for STAMP NFT ID {stamp_id} in collection {collection_name}: {errh}')
    except requests.exceptions.ConnectionError as errc:
        logging.error(f'Error Connecting for STAMP NFT ID {stamp_id} in collection {collection_name}: {errc}')
    except requests.exceptions.Timeout as errt:
        logging.error(f'Timeout Error for STAMP NFT ID {stamp_id} in collection {collection_name}: {errt}')
    except requests.exceptions.RequestException as err:
        logging.error(f'Something went wrong for STAMP NFT ID {stamp_id} in collection {collection_name}: {err}')
    return []

# Define a function to extract the wallet addresses from the holder data
def extract_wallet_addresses(holder_data):
    addresses = []
    for holder in holder_data:
        address = holder['address']
        addresses.append(address)
    return addresses

# Define a function to write the wallet addresses to a CSV file
def write_to_csv(addresses):
    with open('col-vivalastamps_nft_holders.csv', 'a', newline='') as csvfile:
        fieldnames = ['address']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        if csvfile.tell() == 0:
            writer.writeheader()
        for address in addresses:
            writer.writerow({'address': address})

# Call the functions for each NFT ID in the array, now with tqdm for progress tracking
wallet_addresses = []
for stamp_id in tqdm(stamp_ids, desc="Querying NFT IDs"):
    logging.info(f'Fetching holder data for STAMP NFT ID {stamp_id}...')
    holder_data = get_holder_data(stamp_id)
    if holder_data:
        collection_name = get_collection_name()  # Assuming you can get the collection name
        logging.info(f'Extracting wallet addresses from holder data for {collection_name}...')
        addresses = extract_wallet_addresses(holder_data)
        logging.info(f'Writing {len(addresses)} wallet addresses to CSV file for {collection_name}...')
        write_to_csv(addresses)
        wallet_addresses.extend(addresses)
    else:
        logging.warning(f'No holder data found for STAMP NFT ID {stamp_id}')

# Print the total number of wallet addresses collected
logging.info(f'Collected {len(wallet_addresses)} wallet addresses')
import requests
import json
import random
import string

def download_data(api_url, data_key, max_pages=None, debug=False):
    page_number = 1
    more_pages = True
    data = []

    while more_pages:
        if debug and max_pages is not None and page_number > max_pages:
            print("Debug mode: Reached max_pages limit.")
            break

        # Adjust for existing query parameters in api_url
        separator = '&' if '?' in api_url else '?'
        print(f"Fetching page {page_number}...")
        response = requests.get(f"{api_url}{separator}page={page_number}")

        if response.status_code == 200:
            page_data = response.json()
            if data_key not in page_data:
                print(f"Invalid data_key '{data_key}'.")
                break
            data.extend(page_data[data_key])

            more_pages = page_number < page_data['totalPages']
            page_number += 1
        else:
            print(f"Request failed with status code {response.status_code}")
            break

    return data


def save_data_to_file(data, filename):
    # Generate a random string to append to the filename to prevent accidental overwrites
    random_str = ''.join(random.choices(string.ascii_letters + string.digits, k=2))
    modified_filename = f"{filename}_{random_str}.json"
    with open(modified_filename, 'w') as file:
        json.dump(data, file)
    print(f"Data saved to {modified_filename}")

data = download_data('https://stampchain.io/api/v2/src20/tick/STMAP?limit=5000', 'data', max_pages=2, debug=False)

if data:
    save_data_to_file(data, "fetchSRC20_STMAP")
else:
    print("No data received.")
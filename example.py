import requests
import random
import polars as pl

# ------ SET ENDPOINT HERE -------
# API endpoint
ENDPOINT = 'https://server/ncbi/'

# Read in GenBank strain names
strains = pl.read_csv('data/genbank_strains.csv')['SEQUENCE_GENBANK_STRAIN']

# Define and print seed before sampling
seed = random.randint(1, 100000)
print(f"Seed: {seed}")

# Define the terms you want to search for
terms = strains.sample(20, seed=seed)

# Add terms to search for
url = ENDPOINT + 'fetch-accession/?terms=' + ','.join(terms)

# Set the headers, including the Posit Connect api key which will be read in from the local json file
headers = {
    'accept': 'application/json',
    'Authorization': f'Key {pl.read_json('connect_api_key.json')['connect_api_key'][0]}'  # Pull in API key
}

# Send GET request
response = requests.get(url, headers=headers, verify=False)  # verify set to false due to SSL cert failures

# Check if the request was successful and print the returned dictionary or terms (keys) and accessions (values)
if response.status_code == 200:
    response_dict = response.json()
    response_df = pl.DataFrame({
        'SEQUENCE_GENBANK_STRAIN': response_dict.keys(),
        'SEQUENCE_GENBANK_ACCESSION': response_dict.values()
    })
    print(response_df)
else:
    print(f"Error {response.status_code}: {response.text}")

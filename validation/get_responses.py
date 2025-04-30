import requests
import random
import polars as pl
import glob
import os

# Get api keys
keys = pl.read_json('../api_keys.json')
# Connect key
connect_key = keys['connect_api_key'][0]
# NCBI Key
ncbi_key = keys['ncbi_api_key'][0]

# Read in GenBank strain names
strains = pl.read_csv('data/genbank_strains.csv')['SEQUENCE_GENBANK_STRAIN']

# Define and print seed before sampling
seed = random.randint(1, 100000)
print(f"Seed: {seed}")

# Set the headers, including the Posit Connect api key which will be read in from the local json file
headers = {
    'accept': 'application/json',
    'Authorization': f'Key {connect_key}'  # Pull in API key
}
# API endpoint
endpoint = 'https://server/ncbi/'
num_workers = 200
batch_count = 0
final_count = 100000
while (batch_count * num_workers) < final_count:
    # Define the terms you want to search for
    terms = strains.sample(num_workers)

    # Add terms to search for
    url = (
            endpoint
            + 'fetch-accession/?terms='
            + ','.join(terms)  # Add terms
            + (f'&api_key={ncbi_key}' if ncbi_key not in ['your_key_here', '', None] else '')  # Add NCBI api key if present
    )

    # Send GET request
    response = requests.get(url, headers=headers, verify=False, timeout=300)  # verify set to false due to SSL cert failures

    # Check if the request was successful and print the returned dictionary or terms (keys) and accessions (values)
    if response.status_code == 200:
        response_dict = response.json()
        pl.DataFrame({
            'SEQUENCE_GENBANK_STRAIN': response_dict.keys(),
            'SEQUENCE_GENBANK_ACCESSION': response_dict.values()
        }).write_csv(f'data/api_output_{batch_count}.csv')
        batch_count += 1
        strains = strains.filter(~strains.is_in(terms))
        print(f'Completed {batch_count * num_workers} strain look ups... {final_count - (batch_count * num_workers)} to go!')
        if (final_count - (batch_count * num_workers)) % final_count == 0:
            # Specify the folder path and pattern for your CSV files
            file_paths = glob.glob("data/api_output_*.csv")
            (
                # Read all CSV files and concatenate them into a single DataFrame
                pl.concat([pl.read_csv(file) for file in file_paths], how="vertical")
                # Write the combined dataframe as a CSV
                .write_csv(f'data/api_output_combined_{num_workers}_{batch_count}.csv')
            )
            # Remove files
            [os.remove(file) for file in file_paths]
            # Print message after cleanup
            print("--- Cleaned up file outputs ---")
    else:
        print(f"Error {response.status_code}: {response.text}")

# Combine/rename final file(s)
file_paths = glob.glob("data/api_output_*.csv")
(
    # Read all CSV files and concatenate them into a single DataFrame
    pl.concat([pl.read_csv(file) for file in file_paths], how="vertical")
    # Write the combined dataframe as a CSV
    .write_csv(f'data/api_output.csv')
)
# Remove the non-combined CSV files
[os.remove(file) for file in file_paths]

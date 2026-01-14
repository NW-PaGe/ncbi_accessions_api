import asyncio
import aiohttp
import re
from fastapi import FastAPI, Query, Depends
from pydantic import BaseModel, Field, RootModel
from typing import Optional
import xml.etree.ElementTree as ET
from enum import Enum

# Constants for timeout, retry settings
MAX_RETRIES = 5
REQUEST_TIMEOUT = 15
REQUEST_DELAY = 0.5
NUM_WORKERS = 5

# Pattern to check against returned accessions
# A12345 or AB123456 or AB12345678
ACCESSION_PATTERN_NUCLEOTIDE = r'^[A-Za-z]\d{5}\.|^[A-Za-z]{2}\d{6}\.|^[A-Za-z]{2}\d{8}\.'
ACCESSION_PATTERN_BIOSAMPLE = r'SAMN\d{3,}'

class SRAAccessionType(str, Enum):
    """Types of accessions that could be pulled in the SRA Accession GET"""
    srr = "srr"
    sra = "sra"
    srp = "srp"
    srs = "srs"
    srx = "srx"

app = FastAPI()

class FetchAccessionParams(BaseModel):
    f"""
    Pydantic class for validating parameter inputs. terms is removed and called directly in fetch_nucleotide_accessions since
    that was required to display examples in the swagger ui.
    
    Fields:
        api_key (str, default=None): User's NCBI API key.
        timeout (int, default={REQUEST_TIMEOUT}): Timeout for requests in seconds.
        num_workers (int, default={NUM_WORKERS}): Number of concurrent workers.
        max_retries (int, default={MAX_RETRIES}): Maximum number of retries per term.
        request_delay (float, default={REQUEST_DELAY}): Delay between requests in seconds.
    """
    api_key: Optional[str] = Field(None, description="User's NCBI API key")
    timeout: int = Field(REQUEST_TIMEOUT, ge=0, le=500, description='Timeout for requests in seconds')
    num_workers: int = Field(NUM_WORKERS, ge=1, le=10, description='Number of concurrent workers')
    max_retries: int = Field(MAX_RETRIES, ge=0, le=10, description='Maximum number of retries per term')
    request_delay: float = Field(REQUEST_DELAY, ge=0.001, le=60, description='Delay between requests in seconds')
    accession_types: list[str] = []

class FetchNucleotideAccessionResponse(RootModel[dict[str, Optional[str]]]):
    model_config = {
        'json_schema_extra': {
            'example': {
                'WA-PHL-007327': 'PQ880188.1',
                'USA/WA-PHL-007328/2021': 'PQ880189.1'
            }
        }
    }

@app.get('/fetch-nucleotide-accession/', response_model=FetchNucleotideAccessionResponse)
async def fetch_nucleotide_accession(
        terms: str = Query(...,
                           description='Search term(s) to retrieve accession numbers. Separate multiple terms with commas.',
                           example='WA-PHL-007327,USA/WA-PHL-007328/2021',
                           examples=['WA-PHL-007327', 'USA/WA-PHL-007328/2021']
                           ),
        params: FetchAccessionParams = Depends()
):
    f""" Fetches GenBank accession numbers for the provided search terms.

    ## Parameters
    - **terms** (`str`, *required*): Search term to retrieve accession numbers.
    - **api_key** (`str`, *optional*): User's NCBI API key.
    - **timeout** (`int`, *optional*, default=`{REQUEST_TIMEOUT}`): Timeout for requests in seconds.
    - **num_workers** (`int`, *optional*, default=`{NUM_WORKERS}`): Number of concurrent workers.
    - **max_retries** (`int`, *optional*, default=`{MAX_RETRIES}`): Maximum number of retries per term.
    - **request_delay** (`float`, *optional*, default=`{REQUEST_DELAY}`): Delay between requests in seconds.

    ## Returns
    A `dict` containing the results, where:
    - The keys are the search terms.
    - The values are their corresponding accession numbers.
    """
    results = await fetch_all_db(
        # Split terms and remove leading/trailing whitespace if there are multiple terms in the query string
        terms=[term.strip() for term in terms.split(',')],
        params=params,
        db='nucleotide'
    )
    return results


class FetchBioSampleAccessionResponse(RootModel[dict[str, Optional[str]]]):
    model_config = {
        'json_schema_extra': {
            'example': {
                'WAPHL-188937': 'SAMN51251905',
                'WNV/USA/WAPHL-520992/2025': 'SAMN51590889'
            }
        }
    }

@app.get('/fetch-biosample-accession/', response_model=FetchBioSampleAccessionResponse)
async def fetch_biosample_accession(
        terms: str = Query(...,
                           description='Search term(s) to retrieve accession numbers. Separate multiple terms with commas.',
                           example='WAPHL-188937,WNV/USA/WAPHL-520992/2025',
                           examples=['WAPHL-188937', 'WNV/USA/WAPHL-520992/2025']
                           ),
        params: FetchAccessionParams = Depends()
):
    f""" Fetches BioSample accession numbers for the provided search terms.

    ## Parameters
    - **terms** (`str`, *required*): Search term to retrieve accession numbers.
    - **api_key** (`str`, *optional*): User's NCBI API key.
    - **timeout** (`int`, *optional*, default=`{REQUEST_TIMEOUT}`): Timeout for requests in seconds.
    - **num_workers** (`int`, *optional*, default=`{NUM_WORKERS}`): Number of concurrent workers.
    - **max_retries** (`int`, *optional*, default=`{MAX_RETRIES}`): Maximum number of retries per term.
    - **request_delay** (`float`, *optional*, default=`{REQUEST_DELAY}`): Delay between requests in seconds.

    ## Returns
    A `dict` containing the results, where:
    - The keys are the search terms.
    - The values are their corresponding accession numbers.
    """
    results = await fetch_all_db(
        # Split terms and remove leading/trailing whitespace if there are multiple terms in the query string
        terms=[term.strip() for term in terms.split(',')],
        params=params,
        db='biosample'
    )
    return results


class FetchSRAAccessionResponse(RootModel[dict[str, dict[str, Optional[str]]]]):
    model_config = {
        'json_schema_extra': {
            'example': {
                'WA-PHL-033153': {
                    'srr': 'SRR31232922',
                    'sra': 'SRA2005952',
                    'srp': 'SRP446846',
                    'srs': 'SRS23111612',
                    'srx': 'SRX26612931'
                },
                'USA/WA-CDC-LC1021650/2023': {
                    'srr': 'SRR23850971',
                    'sra': 'SRA1603506',
                    'srp': 'SRP325386',
                    'srs': 'SRS17034203',
                    'srx': 'SRX19663757'
                }
            }
        }
    }

@app.get('/fetch-sra-accession/', response_model=FetchSRAAccessionResponse)
async def fetch_sra_accession(
        terms: str = Query(...,
                           description='Search term(s) to retrieve accession numbers. Separate multiple terms with commas.',
                           example='WAPHL-188937,WNV/USA/WAPHL-520992/2025',
                           examples=['WAPHL-188937', 'WNV/USA/WAPHL-520992/2025']
                           ),
        accession_types: list[SRAAccessionType] = Query(
                default=list(SRAAccessionType),
                description="SRA accession types to return",
            example=['sra', 'srr'],
            examples=[['sra', 'srr'], ['srp']]
            ),
        params: FetchAccessionParams = Depends()
):
    f""" Fetches SRA accession numbers for the provided search terms.

    ## Parameters
    - **terms** (`str`, *required*): Search term to retrieve accession numbers.
    - **accession_types** (`list[str]`, *required*, default=`{', '.join(a.value for a in SRAAccessionType)}`): SRA accession types to return.
    - **api_key** (`str`, *optional*): User's NCBI API key.
    - **timeout** (`int`, *required*, default=`{REQUEST_TIMEOUT}`): Timeout for requests in seconds.
    - **num_workers** (`int`, *required*, default=`{NUM_WORKERS}`): Number of concurrent workers.
    - **max_retries** (`int`, *required*, default=`{MAX_RETRIES}`): Maximum number of retries per term.
    - **request_delay** (`float`, *required*, default=`{REQUEST_DELAY}`): Delay between requests in seconds.
    
    ## Returns
    A `dict` containing the results, where:
    - The keys are the search terms.
    - The values are their corresponding accession numbers.
    """

    params.accession_types = [a.value for a in accession_types]

    results = await fetch_all_db(
        # Split terms and remove leading/trailing whitespace if there are multiple terms in the query string
        terms=[term.strip() for term in terms.split(',')],
        params=params,
        db='sra'
    )
    return results


async def fetch_db(term: str,
                   params: FetchAccessionParams,
                   db: str,
                   session: aiohttp.ClientSession,
                   semaphore: asyncio.Semaphore | None = None):
    """ Fetches database accession information for a given term using the NCBI Entrez API.

    Parameters:
        term (str): The search term to use for fetching database data.
        params (FetchAccessionParams): The parameters set by the API call.
        db (str): The NCBI database to search within. Must equal one of the databases listed by https://eutils.ncbi.nlm.nih.gov/entrez/eutils/einfo (e.g., nucleotide, biosample, sra)
        session (aiohttp.ClientSession): The active HTTP session to send requests.
        semaphore (asyncio.Semaphore | None): A semaphore to limit concurrent requests (default is None).

    Returns:
        Tuple[str, str | None]: A tuple containing the search term and the accession result. If no result is found, the accession result is None.
    """
    eutils = 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils'
    retries = 0
    api_key_flag = f'api_key={params.api_key}' if params.api_key else ''

    # Set title term to match on
    title_term = term if '/' in term else f'/{term}/'
    title_term = re.sub(r'/+', '/', title_term)  # dedup slashes

    async with semaphore:
        while retries < params.max_retries:
            try:
                async with asyncio.timeout(params.timeout):
                    data = await fetch_data(session=session,
                                            url=f'{eutils}/esearch.fcgi?db={db}&term={term}&retmode=json&{api_key_flag}',
                                            retries=retries,
                                            params=params)

                id_list = data.get('esearchresult', {}).get('idlist', [])
                if not id_list:
                    return term, None
                if len(id_list) > 10:
                    # Exit if sra is being searched (there is no tried and true way to search for strain name in results)
                    if db == 'sra':
                        return term, {acc: None for acc in params.accession_types}
                    # Otherwise, limit id search to first 10 ids
                    id_list = id_list[:10]

                for uid in id_list:
                    summary_data = await fetch_data(session=session,
                                                    url=f'{eutils}/esummary.fcgi?db={db}&id={uid}&retmode=json&{api_key_flag}',
                                                    retries=retries,
                                                    params=params)

                    result = summary_data['result'].get(uid, {})

                    if db == 'sra':
                        accession_result = {acc: None for acc in ["srr", "sra", "srp", "srs", "srx"]}
                        if 'srr' in params.accession_types:
                            runs = result.get('runs')
                            if runs:
                                try:
                                    accession_result['srr'] = ET.fromstring(runs.strip()).attrib.get('acc')
                                except ET.ParseError:
                                    pass
                        if any(x in params.accession_types for x in ['sra', 'srp', 'srs', 'srx']):
                            exp = result.get('expxml')
                            if exp:
                                try:
                                    exp_xml = ET.fromstring(f'<root>{exp.strip()}</root>')
                                    tag_map = {
                                        'sra': 'Submitter',
                                        'srp': 'Study',
                                        'srs': 'Sample',
                                        'srx': 'Experiment',
                                    }
                                    for acc, tag in tag_map.items():
                                        if acc in params.accession_types:
                                            elem = exp_xml.find(tag)
                                            if elem is not None:
                                                accession_result[acc] = elem.attrib.get('acc')
                                except ET.ParseError:
                                    pass
                        return term, {acc: accession_result[acc] for acc in params.accession_types}

                    elif db in ['nucleotide', 'biosample']:
                        if db == 'nucleotide':
                            accession_tag = 'accessionversion'
                            strain_tag = 'title'
                            accession_re = ACCESSION_PATTERN_NUCLEOTIDE
                        elif db == 'biosample':
                            accession_tag = 'accession'
                            strain_tag = 'infraspecies'
                            accession_re = ACCESSION_PATTERN_BIOSAMPLE

                        accession_result = result.get(accession_tag)
                        title_result = summary_data['result'].get(uid, {}).get(strain_tag, '')
                        title_result = re.sub(r'/+', '/', title_result)  # dedup slashes
                        if accession_result and re.match(accession_re, accession_result) and title_term in title_result:
                            return term, accession_result

                    else:
                        raise ValueError("`db` must be one of 'biosample', 'nucleotide', or 'sra'")

                return term, None

            except (asyncio.TimeoutError, aiohttp.ClientError) as e:
                wait_time = await handle_retry_error(e, retries)
                retries += 1
                await asyncio.sleep(wait_time)
            except Exception as e:
                print(f'!!! Unexpected error fetching {term}: {e}')
                return term, None

    print(f'!!! Failed after {params.max_retries} retries: {term}')
    return term, None


async def fetch_data(session, url, retries, params):
    """ Fetches data from the given URL with retry logic for rate limits or transient errors.

    Parameters:
        session (aiohttp.ClientSession): The active HTTP session to send requests.
        url (str): The URL to fetch data from.
        retries (int): The current retry attempt (used for rate limiting).
        params (FetchAccessionParams): API query parameters.

    Returns:
        dict: The JSON data returned from the request.

    Raises:
        aiohttp.ClientError: If there is an issue with the request (e.g., network error).
        asyncio.TimeoutError: If the request exceeds the timeout limit.
    """
    try:
        async with session.get(url, timeout=REQUEST_TIMEOUT) as response:
            await asyncio.sleep(params.request_delay)
            data = await response.json()

            if 'error' in data and 'API rate limit exceeded' in data['error']:
                wait_time = 2 ** retries
                await asyncio.sleep(wait_time)
                return await fetch_data(session=session, url=url, retries=retries + 1, params=params)
            return data

    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        print(f'@@@ Error fetching data: {e}')
        raise


async def handle_retry_error(error, retries):
    """ Handles retry logic for errors, implementing exponential backoff.

    Parameters:
        error (Exception): The error that occurred during the request.
        retries (int): The current retry attempt number.

    Returns:
        int: The number of seconds to wait before retrying.
    """
    wait_time = min(1 + (2 ** retries), 10)
    print(f'@@@ Error occurred, retrying in {wait_time} seconds:\n{error}')
    return wait_time


async def fetch_all_db(terms, params, db):
    """ Fetches database accession numbers for a list of terms in parallel using asynchronous workers.

    Parameters:
        terms (list | str): A search term or comma-separated search terms.
        params (FetchAccessionParams): API query parameters.
        db (str): The NCBI database to search within.

    Returns:
        dict: A dictionary where each key is a term, and each value is its corresponding database accession result.
    """
    queue = asyncio.Queue()
    results = {}

    semaphore = asyncio.Semaphore(3)

    # Ensure terms is always a list
    if isinstance(terms, str):
        terms = [terms]

    for term in terms:
        queue.put_nowait(term)

    async with aiohttp.ClientSession() as session:
        workers = [
            asyncio.create_task(
                worker(queue=queue,
                       session=session,
                       results=results,
                       semaphore=semaphore,
                       params=params,
                       db=db)
            ) for _ in range(params.num_workers)
        ]

        await queue.join()

        for _ in range(params.num_workers):
            queue.put_nowait(None)

        await asyncio.gather(*workers, return_exceptions=True)

    return results


async def worker(queue, session, results, semaphore, params, db):
    """ Worker function to process tasks from the queue asynchronously.

    Parameters:
        queue (asyncio.Queue): The queue containing search terms to be processed.
        session (aiohttp.ClientSession): The active HTTP session to send requests.
        results (dict): A dictionary to store the results (term -> accession).
        semaphore (asyncio.Semaphore): A semaphore to limit concurrent requests.
        params (FetchAccessionParams): API query parameters.
        db (str): The NCBI database to search within.

    Returns:
        None: The results are stored in the `results` dictionary.
    """
    while True:
        term = await queue.get()
        if term is None:
            queue.task_done()
            break

        try:
            term, result = await fetch_db(
                term=term,
                params=params,
                db=db,
                session=session,
                semaphore=semaphore
            )
            results[term] = result
        except Exception as e:
            print(f'!!! Worker error on {term}: {e}')
            results[term] = None

        queue.task_done()

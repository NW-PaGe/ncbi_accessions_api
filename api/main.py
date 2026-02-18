import asyncio
import aiohttp
import time
import re
from fastapi import FastAPI, Query, Depends
from pydantic import BaseModel, Field, RootModel
from typing import Optional
import xml.etree.ElementTree as ET
from enum import Enum

# Defaults for settings
MAX_RETRIES = 5
REQUEST_TIMEOUT = 15
NUM_WORKERS = 5
VALIDATE = False


# Regex patters:
# A12345 or AB123456 or AB12345678:
ACCESSION_PATTERN_NUCLEOTIDE = re.compile(r'^[A-Za-z]\d{5}\.|^[A-Za-z]{2}\d{6}\.|^[A-Za-z]{2}\d{8}\.')
# SAMN123(456...)
ACCESSION_PATTERN_BIOSAMPLE = re.compile(r'SAMN\d{3,}')
# Multiple slashes
SLASH_PATTERN = re.compile('/+')

class RateLimiter:
    """
    Async rate limiter enforcing N requests per second. NCBI enforces a 3 rps limit for requets w/o an API key
    or 10 rps for requests w/an API key.
    """
    def __init__(self, rate: int):
        self.interval = 1.0 / rate
        self._lock = asyncio.Lock()
        self._last_call = 0.0

    async def acquire(self):
        async with self._lock:
            now = time.monotonic()
            wait = self.interval - (now - self._last_call)
            if wait > 0:
                await asyncio.sleep(wait)
            self._last_call = time.monotonic()

class FetchAccessionParams(BaseModel):
    f"""
    Pydantic class for validating parameter inputs. terms is removed and called directly in fetch_nucleotide_accessions since
    that was required to display examples in the swagger ui.
    
    Fields:
        api_key (str, default=None): User's NCBI API key.
        timeout (int, default={REQUEST_TIMEOUT}): Timeout for requests in seconds.
        num_workers (int, default={NUM_WORKERS}): Number of concurrent workers.
        max_retries (int, default={MAX_RETRIES}): Maximum number of retries per term.
    """
    api_key: Optional[str] = Field(None, description="User's NCBI API key")
    timeout: int = Field(REQUEST_TIMEOUT, ge=0, le=500, description='Timeout for requests in seconds')
    num_workers: int = Field(NUM_WORKERS, ge=1, le=10, description='Number of concurrent workers')
    max_retries: int = Field(MAX_RETRIES, ge=0, le=10, description='Maximum number of retries per term')

class FetchNucleotideAccessionResponse(RootModel[dict[str, Optional[str]]]):
    model_config = {
        'json_schema_extra': {
            'example': {
                'WA-PHL-007327': 'PQ880188.1',
                'USA/WA-PHL-007328/2021': 'PQ880189.1'
            }
        }
    }

class FetchBioSampleAccessionResponse(RootModel[dict[str, Optional[str]]]):
    model_config = {
        'json_schema_extra': {
            'example': {
                'WAPHL-188937': 'SAMN51251905',
                'WNV/USA/WAPHL-520992/2025': 'SAMN51590889'
            }
        }
    }

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

class SRAAccessionType(str, Enum):
    """Types of accessions that could be pulled in the SRA Accession GET"""
    srr = "srr"
    sra = "sra"
    srp = "srp"
    srs = "srs"
    srx = "srx"

app = FastAPI()

@app.get('/fetch-nucleotide-accession/', response_model=FetchNucleotideAccessionResponse)
async def fetch_nucleotide_accession(
        terms: str = Query(...,
                           description='Search term(s) to retrieve accession numbers. Separate multiple terms with commas.',
                           example='WA-PHL-007327,USA/WA-PHL-007328/2021',
                           examples=['WA-PHL-007327', 'USA/WA-PHL-007328/2021']
                           ),
        params: FetchAccessionParams = Depends(),
        validate: bool = Query(VALIDATE, description='Whether to match search term to result strain name')
):
    f""" Fetches GenBank accession numbers for the provided search terms.

    ## Parameters
    - **terms** (`str`): Search term to retrieve accession numbers.
    - **api_key** (`str`, *optional*): User's NCBI API key.
    - **timeout** (`int`, default=`{REQUEST_TIMEOUT}`): Timeout for requests in seconds.
    - **num_workers** (`int`, default=`{NUM_WORKERS}`): Number of concurrent workers.
    - **max_retries** (`int`, default=`{MAX_RETRIES}`): Maximum number of retries per term.
    - **validate** (`bool`, default=`{VALIDATE}`): Whether to match search term to result strain name.

    ## Returns
    A `dict` containing the results, where:
    - The keys are the search terms.
    - The values are their corresponding accession numbers.
    """
    results = await fetch_all_db(
        # Split terms and remove leading/trailing whitespace if there are multiple terms in the query string
        terms={term.strip() for term in terms.split(',')},
        params=params,
        db='nucleotide',
        validate=validate
    )
    return results


@app.get('/fetch-biosample-accession/', response_model=FetchBioSampleAccessionResponse)
async def fetch_biosample_accession(
        terms: str = Query(...,
                           description='Search term(s) to retrieve accession numbers. Separate multiple terms with commas.',
                           example='WAPHL-188937,WNV/USA/WAPHL-520992/2025',
                           examples=['WAPHL-188937', 'WNV/USA/WAPHL-520992/2025']
                           ),
        params: FetchAccessionParams = Depends(),
        validate: bool = Query(VALIDATE, description='Whether to match search term to result strain name')
):
    f""" Fetches BioSample accession numbers for the provided search terms.

    ## Parameters
    - **terms** (`str`, *required*): Search term to retrieve accession numbers.
    - **api_key** (`str`, *optional*): User's NCBI API key.
    - **timeout** (`int`, *optional*, default=`{REQUEST_TIMEOUT}`): Timeout for requests in seconds.
    - **num_workers** (`int`, *optional*, default=`{NUM_WORKERS}`): Number of concurrent workers.
    - **max_retries** (`int`, *optional*, default=`{MAX_RETRIES}`): Maximum number of retries per term.
    - **validate** (`bool`, default=`{VALIDATE}`): Whether to match search term to result strain name.

    ## Returns
    A `dict` containing the results, where:
    - The keys are the search terms.
    - The values are their corresponding accession numbers.
    """
    results = await fetch_all_db(
        # Split terms and remove leading/trailing whitespace if there are multiple terms in the query string
        terms={term.strip() for term in terms.split(',')},
        params=params,
        db='biosample',
        validate=validate
    )
    return results


@app.get('/fetch-sra-accession/', response_model=FetchSRAAccessionResponse)
async def fetch_sra_accession(
        terms: str = Query(...,
                           description='Search term(s) to retrieve accession numbers. Separate multiple terms with commas.',
                           example='WA-PHL-033153,USA/WA-CDC-LC1021650/2023',
                           examples=['WA-PHL-033153', 'USA/WA-CDC-LC1021650/2023']
                           ),
        acc: list[SRAAccessionType] = Query(
                default=list(SRAAccessionType),
                description="SRA accession types to return",
            example=[SRAAccessionType.sra, SRAAccessionType.srr],
            examples=[[SRAAccessionType.sra, SRAAccessionType.srr], [SRAAccessionType.srp]]
            ),
        params: FetchAccessionParams = Depends()
):
    f""" Fetches SRA accession numbers for the provided search terms.

    ## Parameters
    - **terms** (`str`, *required*): Search term to retrieve accession numbers.
    - **acc** (`SRAAccessionType`, *required*, default=`{', '.join(a.value for a in SRAAccessionType)}`): SRA accession types to return.
    - **api_key** (`str`, *optional*): User's NCBI API key.
    - **timeout** (`int`, *required*, default=`{REQUEST_TIMEOUT}`): Timeout for requests in seconds.
    - **num_workers** (`int`, *required*, default=`{NUM_WORKERS}`): Number of concurrent workers.
    - **max_retries** (`int`, *required*, default=`{MAX_RETRIES}`): Maximum number of retries per term.
    
    ## Returns
    A `dict` containing the results, where:
    - The keys are the search terms.
    - The values are their corresponding accession numbers.
    """
    results = await fetch_all_db(
        # Split terms and remove leading/trailing whitespace if there are multiple terms in the query string
        terms={term.strip() for term in terms.split(',')},
        params=params,
        db='sra',
        acc=acc
    )
    return results


async def fetch_db(term: str,
                   params: FetchAccessionParams,
                   db: str,
                   session: aiohttp.ClientSession,
                   semaphore: asyncio.Semaphore,
                   limiter: RateLimiter,
                   validate: bool = False,
                   acc: list[SRAAccessionType] | None = None):
    """ Fetches database accession information for a given term using the NCBI Entrez API.

    Parameters:
        term (str): The search term to use for fetching database data.
        params (FetchAccessionParams): The parameters set by the API call.
        db (str): The NCBI database to search within. Must equal one of the databases listed by https://eutils.ncbi.nlm.nih.gov/entrez/eutils/einfo (e.g., nucleotide, biosample, sra)
        session (aiohttp.ClientSession): The active HTTP session to send requests.
        semaphore (asyncio.Semaphore): A semaphore to limit concurrent requests.
        limiter (RateLimiter): rate limiter to keep requests under NCBI's rps cap.
        validate (bool): Whether to match search term to result strain name.
        acc (list[SRAAccessionType] | None): If querying SRA, specifies the types of accessions to return.

    Returns:
        Tuple[str, str | None]: A tuple containing the search term and the accession result. If no result is found, the accession result is None.
    """
    eutils = 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils'

    api_key_flag = f'api_key={params.api_key}' if params.api_key else ''

    # Set title term to match on
    title_term = term if '/' in term else f'/{term}/'
    title_term = re.sub(SLASH_PATTERN, '/', title_term)  # dedup slashes

    # Set default return values
    ret_none = {a: None for a in acc} if db == 'sra' else None

    async with semaphore:
        data = await fetch_data(session=session,
                                url=f'{eutils}/esearch.fcgi?db={db}&term={term}&retmode=json&{api_key_flag}',
                                limiter=limiter,
                                params=params,
                                retries=0)

        id_list = data.get('esearchresult', {}).get('idlist', [])

        if not id_list:
            return term, ret_none

        if len(id_list) > 10:
            # Exit if sra is being searched (there is no tried and true way to search for strain name in results)
            if db == 'sra':
                return term, ret_none
            id_list = id_list[:10]  # limit id search to first 10 ids

        for uid in id_list:
            summary_data = await fetch_data(session=session,
                                            url=f'{eutils}/esummary.fcgi?db={db}&id={uid}&retmode=json&{api_key_flag}',
                                            limiter=limiter,
                                            params=params,
                                            retries=0)

            result = summary_data['result'].get(uid, {})

            if db == 'sra':
                accession_result = {a.value: None for a in SRAAccessionType}
                if SRAAccessionType.srr in acc:
                    runs = result.get('runs')
                    if runs:
                        try:
                            accession_result['srr'] = ET.fromstring(runs.strip()).attrib.get('acc')
                        except ET.ParseError:
                            pass
                if any(a in acc for a in SRAAccessionType if a is not SRAAccessionType.srr):
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
                            for a, tag in tag_map.items():
                                if SRAAccessionType(a) in acc:
                                    elem = exp_xml.find(tag)
                                    if elem is not None:
                                        accession_result[a] = elem.attrib.get('acc')
                        except ET.ParseError:
                            pass

                return term, {a.value: accession_result[a.value] for a in acc}

            elif db == 'nucleotide':
                accession_tag = 'accessionversion'
                strain_tag = 'title'
                accession_re = ACCESSION_PATTERN_NUCLEOTIDE
            else:  # implied db == 'biosample'
                accession_tag = 'accession'
                strain_tag = 'infraspecies'
                accession_re = ACCESSION_PATTERN_BIOSAMPLE

            accession = result.get(accession_tag)  # extract accession
            title = re.sub(SLASH_PATTERN, '/', result.get(strain_tag, ''))  # extract title & dedup slashes

            if accession:
                if not validate or (accession_re.match(accession) and title_term.lower() in title.lower()):
                    return term, accession

        return term, ret_none


async def fetch_data(session, url, limiter, params, retries):
    """ Fetches data from the given URL with retry logic for rate limits or transient errors.

    Parameters:
        session (aiohttp.ClientSession): The active HTTP session to send requests.
        url (str): The URL to fetch data from.
        limiter (RateLimiter): rate limiter to keep requests under NCBI's rps cap
        params (FetchAccessionParams): API query parameters.
        retries (int): The current retry attempt (used for rate limiting).

    Returns:
        dict: The JSON data returned from the request.

    Raises:
        aiohttp.ClientError: If there is an issue with the request (e.g., network error).
        asyncio.TimeoutError: If the request exceeds the timeout limit.
    """
    await limiter.acquire()

    try:
        async with session.get(url, timeout=params.timeout) as response:
            data = await response.json()

            if 'error' in data and 'API rate limit exceeded' in data['error']:
                raise aiohttp.ClientError("NCBI rate limit exceeded")

            return data

    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        if retries >= params.max_retries:
            raise
        wait = min(2 ** retries, 10)
        await asyncio.sleep(wait)
        return await fetch_data(session, url, limiter, params, retries + 1)


async def fetch_all_db(terms, params, db, acc=None, validate=False):
    """ Fetches database accession numbers for a list of terms in parallel using asynchronous workers.

    Parameters:
        terms (set | str): A search term or comma-separated search terms.
        params (FetchAccessionParams): API query parameters.
        db (str): The NCBI database to search within.
        acc (list[SRAAccessionType] | None): If querying SRA, specifies the types of accessions to return.
        validate (bool): Whether to match search term to result strain name.

    Returns:
        dict: A dictionary where each key is a term, and each value is its corresponding database accession result.
    """
    if isinstance(terms, str):
        terms = {terms}

    # RPS selection
    ncbi_cap = 10 if params.api_key else 3  # Max rps from NCBI docs
    limiter = RateLimiter(ncbi_cap)

    semaphore = asyncio.Semaphore(params.num_workers)
    results = {}

    async with aiohttp.ClientSession() as session:
        async def run(term):
            try:
                key, result = await fetch_db(term=term,
                                             params=params,
                                             db=db,
                                             session=session,
                                             semaphore=semaphore,
                                             limiter=limiter,
                                             acc=acc,
                                             validate=validate)
                results[key] = result
            except Exception:
                results[term] = None

        await asyncio.gather(*(run(t) for t in terms))

    return results

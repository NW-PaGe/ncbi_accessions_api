import asyncio
import aiohttp
import re
from fastapi import FastAPI, Query
import warnings

# Constants for timeout, retry settings
MAX_RETRIES = 5
REQUEST_TIMEOUT = 15
REQUEST_DELAY = 0.5

# Pattern to check against returned accessions
# A12345 or AB123456 or AB12345678
ACCESSION_PATTERN = r'^[A-Za-z]\d{5}\.|^[A-Za-z]{2}\d{6}\.|^[A-Za-z]{2}\d{8}\.'

app = FastAPI()

@app.get("/fetch-accession/")
async def fetch_accession(
    terms: str = Query(default=..., description="Search term to retrieve accession numbers",
                       examples=["WA-PHL-007327"], example="WA-PHL-007327, USA/WA-PHL-007328/2021"),
    api_key: str = Query(default=None, description="User's NCBI API key"),
    timeout: int = Query(default=REQUEST_TIMEOUT, description="Timeout for requests"),
    num_workers: int = Query(default=5, description="Number of concurrent workers"),
    max_retries: int = Query(default=MAX_RETRIES, description="Maximum number of retries per term"),
    request_delay: int | float = Query(default=REQUEST_DELAY, description="Delay between requests (in seconds)")
) -> dict[str, str | None]:
    """ Fetches GenBank accession numbers for the provided search terms.

    ## Parameters
    - **terms** (`str`, *required*): Search term to retrieve accession numbers.
    - **api_key** (`str`, *optional*): User's NCBI API key.
    - **timeout** (`int`, *optional*, default=`15`): Timeout for requests in seconds.
    - **num_workers** (`int`, *optional*, default=`5`): Number of concurrent workers.
    - **max_retries** (`int`, *optional*, default=`5`): Maximum number of retries per term.
    - **request_delay** (`int | float`, *optional*, default=`0.5`): Delay between requests in seconds.

    ## Returns
    A `dict` containing the results, where:
    - The keys are the search terms.
    - The values are their corresponding accession numbers.
    """
    print('Testing changes')
    warnings.warn('Testing warnings')
    # Split terms and remove leading/trailing whitespace if there are multiple terms in the query string
    terms_list = [term.strip() for term in terms.split(",")]
    results = await fetch_all_nuccore(
        terms=terms_list, api_key=api_key, timeout=timeout, num_workers=num_workers,
        max_retries=max_retries, request_delay=request_delay
    )
    return results


async def fetch_nuccore(term, session, semaphore=None, api_key=None, timeout=REQUEST_TIMEOUT, max_retries=MAX_RETRIES, request_delay=REQUEST_DELAY):
    """ Fetches GenBank accession information for a given term using the NCBI Entrez API.

    Parameters:
        term (str): The search term to use for fetching GenBank data.
        session (aiohttp.ClientSession): The active HTTP session to send requests.
        semaphore (asyncio.Semaphore | None): A semaphore to limit concurrent requests (default is None).
        api_key (str | None): The NCBI API key to use (optional).
        timeout (int): The timeout for each request (default is 15 seconds).
        max_retries (int): The maximum number of retries in case of failure (default is 5).
        request_delay (int): Delay between requests (default is 0.5 seconds).

    Returns:
        Tuple[str, str | None]: A tuple containing the search term and the accession result. If no result is found, the accession result is None.
    """
    eutils = 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils'
    retries = 0
    api_key_flag = f'api_key={api_key}' if api_key else ''

    # Set title term to match on
    title_term = term if '/' in term else f'/{term}/'

    async with semaphore:
        while retries < max_retries:
            try:
                async with asyncio.timeout(timeout):
                    data = await fetch_data(session, f'{eutils}/esearch.fcgi?db=nuccore&term={term}&retmode=json&{api_key_flag}', retries, request_delay)

                id_list = data.get('esearchresult', {}).get('idlist', [])
                if not id_list:
                    return term, None
                if len(id_list) > 10:  # Limit id search to first 10 ids
                    id_list = id_list[:10]

                for uid in id_list:
                    summary_data = await fetch_data(session, f'{eutils}/esummary.fcgi?db=nuccore&id={uid}&retmode=json&{api_key_flag}', retries, request_delay)
                    accession_result = summary_data['result'].get(uid, {}).get('accessionversion')
                    title_result = summary_data['result'].get(uid, {}).get('title')

                    if accession_result and re.match(ACCESSION_PATTERN, accession_result) and title_term in title_result:
                        return term, accession_result

                return term, None

            except (asyncio.TimeoutError, aiohttp.ClientError) as e:
                wait_time = await handle_retry_error(e, retries)
                retries += 1
                await asyncio.sleep(wait_time)
            except Exception as e:
                print(f'!!! Unexpected error fetching {term}: {e}')
                return term, None

    print(f'!!! Failed after {max_retries} retries: {term}')
    return term, None


async def fetch_data(session, url, retries, request_delay):
    """ Fetches data from the given URL with retry logic for rate limits or transient errors.

    Parameters:
        session (aiohttp.ClientSession): The active HTTP session to send requests.
        url (str): The URL to fetch data from.
        retries (int): The current retry attempt (used for rate limiting).
        request_delay (int): Delay between requests.

    Returns:
        dict: The JSON data returned from the request.

    Raises:
        aiohttp.ClientError: If there is an issue with the request (e.g., network error).
        asyncio.TimeoutError: If the request exceeds the timeout limit.
    """
    try:
        async with session.get(url, timeout=REQUEST_TIMEOUT) as response:
            await asyncio.sleep(request_delay)
            data = await response.json()

            if 'error' in data and 'API rate limit exceeded' in data['error']:
                wait_time = 2 ** retries
                await asyncio.sleep(wait_time)
                return await fetch_data(session, url, retries + 1, request_delay)
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


async def fetch_all_nuccore(terms, api_key, timeout=15, num_workers=5, max_retries=MAX_RETRIES, request_delay=REQUEST_DELAY):
    """ Fetches GenBank accession numbers for a list of terms in parallel using asynchronous workers.

    Parameters:
        terms (list | str): A search term or comma-separated search terms.
        api_key (str | None): The NCBI API key for authentication (optional).
        timeout (int): The timeout for requests (default is 15 seconds).
        num_workers (int): Number of concurrent workers (default is 5).
        max_retries (int): The maximum number of retries for failed requests (default is 5).
        request_delay (int): Delay between requests in seconds (default is 1).

    Returns:
        dict: A dictionary where each key is a term, and each value is its corresponding GenBank accession result.
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
                       api_key=api_key,
                       timeout=timeout,
                       max_retries=max_retries,
                       request_delay=request_delay)
            ) for _ in range(num_workers)
        ]

        await queue.join()

        for _ in range(num_workers):
            queue.put_nowait(None)

        await asyncio.gather(*workers, return_exceptions=True)

    return results


async def worker(queue, session, results, semaphore, api_key, timeout, max_retries, request_delay):
    """ Worker function to process tasks from the queue asynchronously.

    Parameters:
        queue (asyncio.Queue): The queue containing search terms to be processed.
        session (aiohttp.ClientSession): The active HTTP session to send requests.
        results (dict): A dictionary to store the results (term -> accession).
        semaphore (asyncio.Semaphore): A semaphore to limit concurrent requests.
        api_key (str | None): The NCBI API key for authentication.
        timeout (int): The timeout for each request.
        max_retries (int): The maximum number of retries in case of failure.
        request_delay (int): The delay between requests.

    Returns:
        None: The results are stored in the `results` dictionary.
    """
    while True:
        term = await queue.get()
        if term is None:
            queue.task_done()
            break

        try:
            term, result = await fetch_nuccore(
                term=term,
                session=session,
                semaphore=semaphore,
                api_key=api_key,
                timeout=timeout,
                max_retries=max_retries,
                request_delay=request_delay
            )
            results[term] = result
        except Exception as e:
            print(f'!!! Worker error on {term}: {e}')
            results[term] = None

        queue.task_done()

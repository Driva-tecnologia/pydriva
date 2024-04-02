
import multiprocessing
import asyncio
import async_timeout
import pandas as pd
from datetime import datetime
import numpy as np
from time import sleep
import httpx
import json
import pydriva


async def fetch(url: str, semaphore: asyncio.Semaphore, client: httpx.AsyncClient, headers: dict) -> dict:

    try:
        async with async_timeout.timeout(120):
            resp = await client.get(url, follow_redirects=True, headers = headers)
    except Exception as e:
        #if e.__class__.__name__ == "TimeoutError":
        #    logger.error(f"Process {os.getpid()}. Task {asyncio.current_task().get_name()} TIMED OUT on url {url}!")
        response = {
                    'url': url,
                    'domain': pydriva.site.extract_domain(url, raise_if_invalid=False),
                    'redirect': '',
                    'headers': '',
                    'cookies': '',
                    'html': '',
                    'date': datetime.now().isoformat(),
                    'status': np.nan,
                    #'content_type': '',
                    #'content_length': np.nan,
                    'error': e.__class__.__name__,
                    }
        semaphore.release()
        return response

    semaphore.release()
    try:
        response = {
                    'url': url,
                    'domain': pydriva.site.extract_domain(url, raise_if_invalid=False),
                    'redirect': str(resp.url),
                    'headers': json.dumps(dict(resp.headers)),
                    'cookies': json.dumps(dict(resp.cookies)),
                    'html': resp.text.encode("UTF-8"),
                    'date': datetime.now().isoformat(),
                    'status': float(resp.status_code),
                    #'content_type': '',#str(resp.headers["content-type"]),
                    #'content_length': '',#float(resp.headers["content-length"]),
                    'error': '',
                    }
        #logger.success(f"Process {os.getpid()}. Task {asyncio.current_task().get_name()}: Fetched {url}: ({resp.status_code})")
    except Exception as e:
        response = {
                    'url': url,
                    'domain': pydriva.site.extract_domain(url, raise_if_invalid=False),
                    'redirect': '',
                    'headers': '',
                    'cookies': '',
                    'html': '',
                    'date': datetime.now().isoformat(),
                    'status': np.nan,
                    #'content_type': '',
                    #'content_length': np.nan,
                    'error': e.__class__.__name__,
                    }
        #logger.error(f"Process {os.getpid()}. Task {asyncio.current_task().get_name()}: Error requesting {url}: {e.__class__.__name__}")

    

    return response


async def async_worker(url_list: list, tasks: int, headers) -> list:



    async with httpx.AsyncClient() as client:

        semaphore = asyncio.Semaphore(tasks)
        tasks = []
    
        while len(url_list) > 0:
            url = url_list.pop()
            #logger.info(f"Process {os.getpid()}. Task {asyncio.current_task().get_name()} Awaiting semaphore ({semaphore._value})")
            await semaphore.acquire()
            #acquire_semafinho(semafinho)
            #logger.info(f"Process {os.getpid()}. Task {asyncio.current_task().get_name()} Acquired semaphore")
            tasks.append(asyncio.create_task(fetch(url, semaphore, client, headers)))

        #logger.info(f"Process {os.getpid()}. Gathering tasks")
        results = await asyncio.gather(*tasks)
        return results

def sync_worker(x) -> list:
    return asyncio.run(async_worker(x[0], x[1], x[2]))

def split_list(l, n, tasks, headers):
    for i in range(0, len(l), n):
        yield (l[i:i + n], tasks, headers)

def fast_request(input_generator, manage_output, headers: dict = {}, number_of_processes: int = 4, tasks: int = 12):

    if tasks == 0:
        tasks = 100000000
    
    pool = multiprocessing.Pool(processes=number_of_processes)

    acm = 0

    for bulk in input_generator:
        results = []
        for responses_list in pool.imap_unordered(sync_worker, split_list(bulk, len(bulk)//(3*number_of_processes) + 1, tasks, headers)):  
            results = results + responses_list

        manage_output(results, acm)
        acm += 1
        del results
        



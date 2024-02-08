from multiprocessing import Process, Manager, Queue
import asyncio
import aiohttp
import json
import numpy as np
from datetime import datetime
from loguru import logger
from . import cnpj, site

async def fetch(session, url, semaphoro):
    fetch_result = {
        'url': url,
        'response': '',
        'content': '',
        'error': '',
    }
    try:
        async with session.get(url, timeout=15) as response:

            fetch_result['response'] = response
            fetch_result['content'] = await response.text()

            logger.success(f'Fetched {url}: {response.status}')
    except Exception as e:
        fetch_result['error'] = e.__class__.__name__
        logger.error(f'Error fetching {url}: {e.__class__.__name__}')

    semaphoro.release()
    response = response_handler(fetch_result)

    return response

def response_handler(fetch_result):
    result = {
        'url': fetch_result['url'],
        'domain': site.extract_domain(fetch_result['url'], raise_if_invalid=False),
        'redirect': '',
        'headers': '',
        'cookies': '',
        'html': '',
        'date': datetime.now().isoformat(),
        'status': np.nan,
        'content_type': '',
        'content_length': np.nan,
        'error': fetch_result['error'],

    }

    if fetch_result['response']:
        response = fetch_result['response']

        result['redirect'] = str(response.url)
        result['status'] = int(response.status)
        result['headers'] = json.dumps(dict(response.headers))
        result['cookies'] = json.dumps(dict(response.cookies))
        result['content_type'] = response.headers.get('content-type', '')
        result['content_length'] = response.headers.get('content-length', np.nan)
        result['html'] = fetch_result['content']
    
    return result


async def session_worker(url_queue:Queue, semaphore_count:int=0):
    session = aiohttp.ClientSession()
    semaphore = asyncio.Semaphore(semaphore_count)

    tasks = []
    while not url_queue.empty():
        if semaphore_count > 0: 
            await semaphore.acquire()
            if url_queue.empty(): break

        url = url_queue.get()
        task = asyncio.create_task(fetch(session, url, semaphore))
        tasks.append(task)

    if tasks:
        await asyncio.gather(*tasks)
    await session.close()

    results = []
    for task in tasks:
        results.append(task.result())

    return results


def session_worker_sync(url_queue:Queue, results, semaphore_count:int=0):
    results.extend(asyncio.run(session_worker(url_queue, semaphore_count)))
    return results

def fast_requests(urls, workers:int=4, semaphore_count:int=0):
    url_queue = Queue()
    results = Manager().list()

    for url in urls:
        url_queue.put(url)
    
    processes = []
    for _ in range(workers):
        process = Process(target=session_worker_sync, args=(url_queue, results, semaphore_count))
        process.start()
        processes.append(process)
        
    # wait process
    for i, process in enumerate(processes):
        logger.info(f'Waiting for process {i}')
        process.join()

    results = list(results)
    return results

if __name__ == '__main__':
    urls = [
        'https://www.google.com',
        'https://www.yahoo.com'
    ]
    results = fast_requests(urls, workers=4, semaphore_count=0)
    breakpoint()
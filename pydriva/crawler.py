import pydriva
from datetime import datetime
import numpy as np
from math import ceil

import multiprocessing
from itertools import repeat, cycle

import asyncio
import async_timeout
import httpx
import json

import pandas as pd


# def fast_requests(urls):
#     """
#     This function receives a list of URLs and returns a list of responses.
#     """
#     responses = []
#     for url in urls:
#         responses.append(None)
#     return responses

def parse_objs(objs):
    new_objs = []
    for url in objs:
        if isinstance(url, dict):
            new_objs.append(url)
        elif isinstance(url, str):
            new_objs.append({'url': url})
        else:
            raise TypeError('Input must be a list of URLs or a list of dictionaries with URLs.')
    return new_objs

def fast_requests(objs, proxies=[], n_processes: int = 0, tasks: int = 20):
    if n_processes == 0: n_processes = multiprocessing.cpu_count()
    if tasks == 0: tasks = 100000000
    if not isinstance(proxies, list): proxies = [proxies]

    objs = parse_objs(objs)

    results = []

    objs_lists = split_list(objs, partitions=n_processes, max_values=500)
    circular_proxies = cycle(proxies)

    with multiprocessing.Pool(processes=n_processes) as pool:
        for responses_list in pool.imap_unordered(sync_worker, zip(objs_lists, repeat(tasks), circular_proxies)):
            results.extend(responses_list)

    return results


def split_list(list_values, partitions, max_values: int = 200):
    n_values = ceil(len(list_values)/partitions)
    n_values = min(n_values, max_values)
    for i in range(0, len(list_values), n_values):
        yield (list_values[i:i + n_values])

def sync_worker(args):
    return asyncio.run(async_worker(*args))

async def async_worker(objs_list: list, tasks: int, proxies: dict = {}) -> list:
    async with httpx.AsyncClient(proxies=proxies) as client:
        semaphore = asyncio.Semaphore(tasks)
        tasks = []
    
        while len(objs_list) > 0:
            obj = objs_list.pop()
            print(obj)
            await semaphore.acquire()
            tasks.append(asyncio.create_task(fetch(semaphore, client, **obj)))

        results = await asyncio.gather(*tasks)
        return results

async def fetch(semaphore: asyncio.Semaphore, client: httpx.AsyncClient, **kwargs) -> dict:
    try:
        async with async_timeout.timeout(120):
            url = kwargs['url']
            headers = kwargs.get('headers', {})
            params = kwargs.get('params', {})
            resp = await client.get( url, headers=headers, params=params, follow_redirects=True)

        response = {
            'url': url,
            'domain': pydriva.site.extract_domain(url, raise_if_invalid=False),
            'redirect': '',
            'headers': '',
            'cookies': '',
            'html': '',
            'date': datetime.now().isoformat(),
            'status': np.nan,
            'error': ''
        }
        response['redirect'] = str(resp.url)
        response['headers'] = json.dumps(dict(resp.headers))
        response['cookies'] = json.dumps(dict(resp.cookies))
        response['html'] = resp.text
        response['status'] = float(resp.status_code)

    except Exception as e:
        response['error'] = e.__class__.__name__

    semaphore.release()
    return response

async def test(url, proxies):
    async with httpx.AsyncClient(proxies=proxies) as client:
        response = await client.get(url)
        return response

import asyncio
import aiohttp
import json
from random import randint
import time

async def make_request(session, url, payload):
    async with session.post(url, json=payload) as response:
        res = await response.text()
        # print(res)
        return res

async def generate_requests():
    url = "http://localhost:5000/read"  # Replace with your actual endpoint

    async with aiohttp.ClientSession() as session:
        tasks = []
        start_time = time.time()
        for _ in range(10000):
            a=randint(0,16383)
            payload = {"Stud_id": {"low":a, "high":a}}
            tasks.append(make_request(session, url, payload))
        
        
        responses = await asyncio.gather(*tasks)
        end_time = time.time()
        total_time = end_time - start_time
        print("Total time required:", total_time, "seconds")

    return responses

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    result = loop.run_until_complete(generate_requests())

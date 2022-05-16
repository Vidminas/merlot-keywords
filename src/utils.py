import asyncio
from tqdm.asyncio import tqdm


async def gather_unlimited_concurrency(description: str, *coroutines):
    return await tqdm.gather(*coroutines, desc=description)


async def gather_limited_concurrency(limit: int, description: str, *coroutines):
    semaphore = asyncio.Semaphore(limit)

    async def sem_task(task):
        async with semaphore:
            return await task

    return await tqdm.gather(*(sem_task(coro) for coro in coroutines), desc=description)


def as_completed(description: str, coroutines):
    return tqdm.as_completed(coroutines, desc=description)

import asyncio
from multiprocessing import Process, Lock


async def asd():
    print('asd')
    await asyncio.sleep(5)

def f(l, i):
    loop = asyncio.get_event_loop()
    loop.run_until_complete(asd())


if __name__ == '__main__':
    lock = Lock()

    for num in range(10):
        Process(target=f, args=(lock, num)).start()
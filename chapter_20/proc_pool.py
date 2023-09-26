import math
import sys
from concurrent import futures
from time import perf_counter
from typing import NamedTuple

NUMBERS = [
    9999999999999999,
    9999999999999917,
    7777777777777777,
    7777777777777753,
    7777777536340681,
    6666667141414921,
    6666666666666719,
    6666666666666666,
    5555555555555555,
    5555555555555503,
    5555553133149889,
    4444444488888889,
    4444444444444444,
    4444444444444423,
    3333335652092209,
    3333333333333333,
    3333333333333301,
    299593572317531,
    142702110479723,
    2,
]


class PrimeResult(NamedTuple):
    n: int
    flag: bool
    elapsed: float


def is_prime(n: int) -> bool:
    if n < 2:
        return False
    if n == 2:
        return True
    if n % 2 == 0:
        return False
    root = math.isqrt(n)
    for i in range(3, root + 1, 2):
        if n % i == 0:
            return False
    return True


def check(n: int) -> PrimeResult:
    t0 = perf_counter()
    res = is_prime(n)
    return PrimeResult(n, res, perf_counter() - t0)


def main() -> None:
    if len(sys.argv) < 2:
        workers = None
    else:
        workers = int(sys.argv[1])

    executor = futures.ProcessPoolExecutor(workers)
    actual_workers = executor._max_workers  # type: ignore

    print(f'Checking {len(NUMBERS)} numbers with {actual_workers} processes:')

    t0 = perf_counter()
    numbers = sorted(NUMBERS, reverse=True)

    with executor:
        # executor.map is a generator: numbers returned in the same order as they were given.
        for n, prime, elapsed in executor.map(check, numbers):
            label = 'P' if prime else ' '
            print(f'{n:16} {label} {elapsed:9.6f}s')

    time = perf_counter() - t0
    print(f'Total time: {time:.2f}s')


if __name__ == '__main__':
    main()

import itertools
import time
from multiprocessing import Process, Event
from multiprocessing import synchronize


def spin(msg: str, done: synchronize.Event) -> None:
    for char in itertools.cycle(r'\|/-'):
        status = f'\r{char} {msg}'
        print(status, end='', flush=True)
        if done.wait(.1):  # No GIL. It's OS Scheduler who does the job.
            break
    blanks = ' ' * len(status)
    print(f'\r{blanks}\r', end='')


def slow() -> int:
    time.sleep(3)  # No GIL. It's OS Scheduler who does the job.
    return 42


def supervisor() -> int:
    done = Event()
    spinner = Process(target=spin, args=('thinking!', done))
    print(f'spinner object: {spinner}')
    spinner.start()
    result = slow()
    done.set()
    spinner.join()
    return result


def main():
    result = supervisor()
    print(f'Answer: {result}')


if __name__ == '__main__':
    main()


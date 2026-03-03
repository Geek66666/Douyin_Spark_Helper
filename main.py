import argparse
import asyncio

from core.tasks import runTasks


def main():
    parser = argparse.ArgumentParser(description="DouYin Spark Flow")
    parser.add_argument(
        "--doTask",
        action="store_true",
        help="Run tasks once and exit.",
    )
    args = parser.parse_args()

    if args.doTask:
        asyncio.run(runTasks())
        return

    from app import run_server

    run_server()


if __name__ == "__main__":
    main()
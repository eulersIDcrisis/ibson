#!/usr/bin/env python
"""benchmark.py.

File to benchmark python BSON-parsing libraries.
"""
import argparse
import cProfile
import bson
import ibson


def run_load_bytes_bson_test():
    with open("test.bson", "rb") as stm:
        data = stm.read()
    # Start the test here.
    with cProfile.Profile() as pr:
        bson.loads(data)
        pr.print_stats(sort="time")


def run_load_bytes_ibson_test():
    with open("test.bson", "rb") as stm:
        data = stm.read()
    # Start the test here.
    with cProfile.Profile() as pr:
        ibson.loads(data)
        pr.print_stats(sort="time")


def run():
    parser = argparse.ArgumentParser(description="Run profiling around bson libraries.")
    parser.add_argument("lib_to_test", choices=["ibson", "bson"], help="Run the tests with this library.")

    args = parser.parse_args()
    lib = args.lib_to_test
    print(f"TESTING LIBRARY {lib}")
    if lib == "ibson":
        run_load_bytes_ibson_test()
    elif lib == "bson":
        run_load_bytes_bson_test()
    else:
        print("(no matching library found)")


if __name__ == "__main__":
    run()

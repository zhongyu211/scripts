#!/bin/python
import argparse
import multiprocessing

def handle_commandline():
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--concurrency", type=int,
                        default=multiprocessing.cpu_count(),
                        help="specify the concurrency(for debugging and " 
                        "timing)[default:%(default)d]")
    parser.add_argument("-s", "--size", default=400, type=int,
                        help="make a scaled image that fits the given dimension "
                        "[default:%(default)d]")
    parser.add_argument("-S", "--smooth", action="store_true",
                        help="use smooth scaling (slow bug good for text)")
    parser.add_argument("source",
                        help="the directory for the scaled .xpm images")
    parser.add_argument("target",
                        help="the directory for the scaled .xpm images")

    args = parser.parse_args()
    print args
    return args.concurrency, args.size, args.smooth, args.source, args.target
if __name__ == "__main__":
    handle_commandline()


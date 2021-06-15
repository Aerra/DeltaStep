import json
import argparse

if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser(description="Compare results of algorithms")
    arg_parser.add_argument('--in_1', type=str, default=None, help="Json file with first results")
    arg_parser.add_argument('--in_2', type=str, default=None, help="Json file with second results")

    args = arg_parser.parse_args()
    if args.in_1 == None or args.in_2 == None:
        ValueError("No input data")

    d_1 = {}
    d_2 = {}
    with open(args.in_1, "r") as f_1:
        d_1 = json.load(f_1)
    with open(args.in_2, "r") as f_2:
        d_2 = json.load(f_2)

    print(d_1 == d_2)

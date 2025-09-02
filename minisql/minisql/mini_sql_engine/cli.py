# This file implements the command-line interface for the SQL engine.

import argparse
from mini_sql_engine import sql_engine

def main():
    parser = argparse.ArgumentParser(description='Mini SQL Engine Command Line Interface')
    parser.add_argument('command', type=str, help='SQL command to execute')
    parser.add_argument('--params', type=str, nargs='*', help='Parameters for the SQL command')

    args = parser.parse_args()

    engine = sql_engine.SQLEngine()

    if args.params:
        result = engine.execute(args.command, args.params)
    else:
        result = engine.execute(args.command)

    print(result)

if __name__ == '__main__':
    main()
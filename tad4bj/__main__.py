import argparse


def main():
    parser = argparse.ArgumentParser("tad4bj")
    parser.add_argument('--database', '-d', type=str, action='store',
                        help='path to database file')
    parser.add_argument('--table', '-t', type=str, action='store',
                        help='name of the table to work with')
    parser.add_argument('--verbose', '-v', help='verbose output')

    subparsers = parser.add_subparsers(help='available commands')
    parser_init = subparsers.add_parser('init')
    parser_init.add_argument('--schema-file', '-s', type=str, action='store',
                             help='schema of the table that will be initialized')
    parser_clean = subparsers.add_parser('clean')
    parser_get = subparsers.add_parser('get')
    parser_set = subparsers.add_parser('set')

    parser.parse_args()


if __name__ == '__main__':
    main()

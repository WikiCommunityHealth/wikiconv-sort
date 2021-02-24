"""Main module that parses command line arguments."""
import argparse
import pathlib

from . import processors, utils, file_utils


def get_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        prog='wikiconv-crunch',
        description='Graph snapshot features extractor.',
    )
    parser.add_argument(
        'files',
        metavar='FILE',
        type=pathlib.Path,
        nargs='+',
        help='Wikidump file to parse, can be compressed.',
    )
    parser.add_argument(
        'output_dir_path',
        metavar='OUTPUT_DIR',
        type=pathlib.Path,
        help='XML output directory.',
    )
    parser.add_argument(
        '--output-compression',
        choices={None, '7z', 'bz2', 'gzip'},
        required=False,
        default=None,
        help='Output compression format [default: no compression].',
    )
    parser.add_argument(
        '--dry-run', '-n',
        action='store_true',
        help="Don't write any file",
    )

    subparsers = parser.add_subparsers(help='sub-commands help')
    processors.pageid_filter.configure_subparsers(subparsers)

    parsed_args = parser.parse_args()
    if 'func' not in parsed_args:
        parser.print_usage()
        parser.exit(1)

    return parsed_args


def main():
    """Main function."""
    args = get_args()

    if not args.output_dir_path.exists():
        args.output_dir_path.mkdir(parents=True)

    args.func(args.files, args)


if __name__ == '__main__':
    main()

"""Main module that parses command line arguments."""
import argparse
import pathlib

from . import sorter

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
        'sort_by',
        choices={'user', 'page', 'replyToUser'},
        help='Sorting field'
    )
    parser.add_argument(
        '--output-compression',
        choices={None, '7z', 'bz2', 'gz'},
        required=False,
        default=None,
        help='Output compression format [default: no compression].',
    )
    parser.add_argument(
        '--dry-run', '-n',
        action='store_true',
        help="Don't write any file",
    )
    parser.add_argument(
        '--bucket-size',
        type=int,
        required=False,
        default=200000,
        help='Bucket size'
    )

    parsed_args = parser.parse_args()
    return parsed_args


def main():
    """Main function."""
    args = get_args()

    if not args.output_dir_path.exists():
        args.output_dir_path.mkdir(parents=True)

    sorter.sortFiles(
        inputFiles=args.files,
        outputPath=args.output_dir_path,
        bucketSize=args.bucket_size,
        compression=args.output_compression,
        sortBy=args.sort_by
    )


if __name__ == '__main__':
    main()

# python -m wikiconv-sort /files . replyToUser --output-compression gz
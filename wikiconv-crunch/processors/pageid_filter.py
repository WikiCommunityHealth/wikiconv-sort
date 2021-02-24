"""
Extract snapshots from list of revisions.

The output format is csv.
"""

import os
import json
import argparse
import datetime
import math

from typing import Iterable, Iterator, Mapping

from .. import file_utils as fu
from .. import dumper
from .. import types
from .. import utils
from .. import file_utils


from operator import itemgetter
from pprint import pprint

# print a dot each NPRINTREVISION revisions
NPRINTREVISION = 10000

# templates
stats_template = '''
<stats>
    <performance>
        <start_time>${stats['performance']['start_time'] | x}</start_time>
        <end_time>${stats['performance']['end_time'] | x}</end_time>
        <input>
            <objects>${stats['performance']['input']['objects'] | x}</objects>
            <filtered>${stats['performance']['input']['filtered'] | x}</filtered>
        </input>
        <sort>
            <start_time>${stats['performance']['sort']['start_time'] | x}</start_time>
            <end_time>${stats['performance']['sort']['end_time'] | x}</end_time>
        </sort>
    </performance>
</stats>
'''


def process_lines(
        dump: Iterable[list],
        outFiles: Iterable[list],
        bucketSize: int):
    """ Assign each revision to the snapshot or snapshots to which they
        belong.
    """
    nobjs = 0
    for raw_obj in dump:
        obj = types.cast_json(raw_obj)
        obj["timestamp"] = obj["timestamp"].isoformat()
        bucketN = math.floor(obj['pageId'] / bucketSize)
        
        outFiles[bucketN].write(f"{obj['pageId']}\t{obj['timestamp']}\t{json.dumps(obj)}\n")

        if (nobjs-1) % NPRINTREVISION == 0:
            utils.dot()
        nobjs += 1


def configure_subparsers(subparsers):
    """Configure a new subparser ."""
    parser = subparsers.add_parser(
        'filter-pageid',
        help='Filter page IDs.',
    )
    parser.add_argument(
        '--start-id',
        type=int,
        required=True,
        help='Start ID.'
    )
    parser.add_argument(
        '--end-id',
        type=int,
        required=True,
        help='End ID.'
    )

    parser.set_defaults(func=main)


def main(
        input_files: Iterable[list],
        args: argparse.Namespace) -> None:
    """Main function that parses the arguments and writes the output."""


    assert (args.start_id < args.end_id), \
            "Start ID must be smaller than end ID"

    # OUTPUT FILES
    nrOfPages = 100000000
    bucketSize = 10000000
    filesNames = [str(args.output_dir_path / (f"pippo-{i}.json")) for i in range(math.ceil(nrOfPages / bucketSize))]
    outFiles = [fu.output_writer(
                    path=filename,
                    compression=args.output_compression,
                ) for filename in filesNames]

    # Analize dump
    for input_file_path in args.files:
        utils.log(f"Analyzing {input_file_path}...")
        dump = file_utils.open_jsonobjects_file(str(input_file_path))

        process_lines(dump, outFiles, bucketSize)
        dump.close()
        utils.log(f"Done Analyzing {input_file_path}.")

    # Sort files
    for filename in filesNames:
        utils.log(f"Sorting {filename}")
        os.system(f"sort {filename} -o {filename.replace('pippo', 'pluto')}")
        utils.log(f"Done sorting {filename}")

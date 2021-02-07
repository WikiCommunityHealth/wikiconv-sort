"""
Extract snapshots from list of revisions.

The output format is csv.
"""

import os
import json
import argparse
import datetime

from typing import Iterable, Iterator, Mapping

from .. import file_utils as fu
from .. import dumper
from .. import types
from .. import utils

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
        ids: Iterable[set],
        stats: Mapping) -> Iterator[list]:
    """Assign each revision to the snapshot or snapshots to which they
       belong.
    """

    filtered_objs = list()
    for raw_obj in dump:
        obj = types.cast_json(raw_obj)

        stats['performance']['input']['objects'] += 1
        if obj['pageId'] in ids:
            filtered_objs.append(obj)
            stats['performance']['input']['filtered'] += 1

        nobjs = stats['performance']['input']['objects']
        if (nobjs-1) % NPRINTREVISION == 0:
            utils.dot()

    stats['performance']['sort']['start_time'] = datetime.datetime.utcnow()
    filtered_objs.sort(key=itemgetter('pageId', 'timestamp'))
    stats['performance']['sort']['end_time'] = datetime.datetime.utcnow()

    for obj in filtered_objs:
        obj["timestamp"] = obj["timestamp"].isoformat()

        yield obj


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
        dump: Iterable[list],
        basename: str,
        args: argparse.Namespace) -> None:
    """Main function that parses the arguments and writes the output."""
    stats = {
        'performance': {
            'start_time': None,
            'end_time': None,
            'input': {
                'objects': 0,
                'filtered': 0
            },
            'sort': {
                'start_time': None,
                'end_time': None,
            }
        },
    }
    stats['performance']['start_time'] = datetime.datetime.utcnow()

    assert (args.start_id < args.end_id), \
           "Start ID must be smaller than end ID"

    output = open(os.devnull, 'wt')
    stats_output = open(os.devnull, 'wt')
    if not args.dry_run:
        output_filename = str(args.output_dir_path /
                              (basename + '.filtered.json'))
        stats_filename = str(args.output_dir_path /
                             (basename + '.stats.xml'))

        output = fu.output_writer(
            path=output_filename,
            compression=args.output_compression,
        )
        stats_output = fu.output_writer(
            path=stats_filename,
            compression=args.output_compression,
        )

    ids = set(range(args.start_id, args.end_id+1))

    res = process_lines(
        dump,
        ids=ids,
        stats=stats,
    )

    for obj in res:
        output.write(json.dumps(obj))

    stats['performance']['end_time'] = datetime.datetime.utcnow()

    with stats_output:
        dumper.render_template(
            stats_template,
            stats_output,
            stats=stats,
        )

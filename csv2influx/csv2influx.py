# -*- coding: utf-8 -*-
"""csv2influx: write CSV data into InfluxDB thanks to Influx's Line Protocol syntax.
The CSV files must have a header row so csv2inlux can properly match and write
fields' and tags' labels.
If you target more than one input CSV file, csv2influx will consider they are all
identical in terms of structure and CSV options (delimiter, quoting, etc).
csv2influx won't validate fields' types; it will just add trailing 'i' for
integers and double quotes strings.
Finally, tags won't be automatically sorted to improve performances.  Feel free to
do so manually by providing --tag-columns with a sorted list if you prefer.

Usage:
  csv2influx.py PATH [--output-path PATH] [--measurement NAME] [--tag-columns TAGS] --field-columns FIELDS [--timestamp TIME] [--influx-url URL]

Arguments:
  PATH                      path to input file(s) (can contain wildcards)

Options:
  -h --help                 show this message
  --output-path PATH        point to a file or a directory (path ending with os' separator) where the result
                            will be written
  --measurement NAME        name of the measurement [default: sample_measurement]
  --tag-columns TAGS        comma-separated list of columns to use as tags
  --field-columns FIELDS    comma-separated list of columns to use as fields followed by a colon and its type (float, int, str or bool)
  --timestamp TIME          any format that Arrow can read with Arrow.get()
  --influx-url URL          url of the InfluxDB's write endpoint where you'd like to post data
                            (example: http://localhost:8086/write?db=mydb)

Examples:
  csv2influx.py fixtures/*.csv --output-path=output/ --field-columns=speed:int --timestamp=2016-09-26T02:00:00+00:00
  csv2influx.py fixtures/sample.csv --output-path=output/result.out --field-columns=speed:int,strength:float --tag-columns=name,class --timestamp=2016-09-26
"""

# TODO:
#   --no-header               specify that the input file has no header row (TODO)
#   --csv-options             specify CSV options like the delimiter, etc (TODO)
#   --timestamp-precision     specify the precision to be used in the final result; possible values
#                             are n, u, ms, s, m, and h for nanoseconds, microseconds, milliseconds,
#                             seconds, minutes, and hours, respectively (TODO) [default: n]

import cStringIO
import csv
import logging
import os
import shutil
import tempfile
from glob import glob

import arrow
import requests
from docopt import docopt

from influxexporter import InfluxExporter

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s %(levelname)s %(message)s',
    datefmt='%m/%d/%Y %H:%M:%S')


def csv_dialect_to_str(dialect):
    """
    Make CSV dialect human readable.
    """
    return ' // '.join(['%s: %s' % (k, repr(v)) for k, v in dialect.__dict__.iteritems() if not k.startswith('_')])


def arrow_string_to_nano_ts(s):
    """
    Extremely naïvely convert any arrow-ready string into "nano seconds" timestamp.
    Notice the quotation marks around "nano seconds".
    """
    timestamp = arrow.get(s)

    # Mimic nano seconds like a retard
    return ("%.6f" % timestamp.float_timestamp).replace('.', '') + '000'


def process_input_file(input_path, output_path, arguments):
    with open(input_path, 'rb') as input_file, open(output_path % {'basename': os.path.basename(input_path)}, 'ab+') if output_path else tempfile.TemporaryFile() as output_file:
        has_header = csv.Sniffer().has_header(input_file.readline())
        input_file.seek(0)
        dialect = csv.Sniffer().sniff(input_file.readline())
        input_file.seek(0)
        logging.info('CSV sniffer results: %s' % 'header detected' if has_header else 'no header detected')
        logging.info('CSV sniffer results: %s' % csv_dialect_to_str(dialect))

        csvreader = csv.reader(input_file, dialect)
        if not has_header:
            raise Exception('csv2influx needs CSV headers to properly match and write fields\' and tags\' labels')

        # Extract header labels
        labels = [s.lower() for s in csvreader.next()]

        influx_exporter = InfluxExporter(
            labels=labels,
            measurement=arguments['--measurement'],
            tag_columns=arguments['--tag-columns'].split(',') if arguments['--tag-columns'] else [],
            field_columns=arguments['--field-columns'].split(','),
            timestamp=arrow_string_to_nano_ts(arguments['--timestamp']) if arguments['--timestamp'] else None,
        )

        # Using a buffer instead of writing directly to the file is more efficient
        # when writing to a file *and* to a database at the same time
        buf = cStringIO.StringIO()
        for j, row in enumerate(csvreader):
            buf.write(influx_exporter.to_line_protocol_format(row))

        buf.seek(0)
        shutil.copyfileobj(buf, output_file)
        logging.info('Wrote %d lines to file %s' % (j, output_file.name))

        if arguments['--influx-url']:
            r = requests.post(
                url=arguments['--influx-url'],
                data=buf.getvalue(),
                headers={'Content-Type': 'application/x-www-form-urlencoded'}
            )
            logging.info('Wrote %d lines to database %s' % (j, arguments['--influx-url']))


if __name__ == '__main__':
    arguments = docopt(__doc__)

    count_input_files = len(glob(arguments['PATH']))
    logging.info('Found %d input file' % count_input_files)

    # Determine the output path (directory, file or none)
    if arguments['--output-path'] and arguments['--output-path'].endswith(os.sep):
        logging.debug('Output path looks like a directory (ends with "%s"): will write one output file for each input file' % os.sep)
        output_path = os.path.join(arguments['--output-path'],  '%(basename)s.out')
    elif arguments['--output-path'] and not arguments['--output-path'].endswith(os.pathsep):
        logging.debug('Output path looks like a file: will write the whole output in this file')
        output_path = arguments['--output-path']
    else:
        logging.debug('Output path is empty: will not generate any output')
        output_path = None

    for i, input_path in enumerate(glob(arguments['PATH'])):
        logging.info('-- Processing file %d/%d: %s --' % (i+1, count_input_files, os.path.basename(input_path)))
        process_input_file(input_path, output_path, arguments)

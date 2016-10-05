# -*- coding: utf-8 -*-
"""csv2influx: write CSV data into InfluxDB thanks to Influx's Line Protocol syntax.
The CSV files must have a header row so csv2inlux can properly match and write
fields' and tags' labels.
If you target more than one input CSV file, csv2influx will consider they are all
identical in terms of structure and CSV options (delimiter, quoting, etc).
Finally, tags won't be automatically sorted to improve performances.  Feel free to
do so manually by providing --tag-columns with a sorted list if you prefer.

Usage:
  csv2influx.py PATH [--output-path PATH] [--measurement NAME] [--tag-columns TAGS] --field-columns FIELDS [--timestamp TIME] [--load-url URL]

Arguments:
  PATH                      path to input file(s) (can contain wildcards)

Options:
  -h --help                 show this message
  --output-path PATH        point to a file or a directory (path ending with os' separator) where the result
                            will be written
  --measurement NAME        name of the measurement [default: sample_measurement]
  --tag-columns TAGS        comma-separated list of columns to use as tags; use * (star)
                            to select all columns minus those specified as fields
  --field-columns FIELDS    comma-separated list of columns to use as fields
  --timestamp TIME          any format that Arrow can read with Arrow.get()
  --load-url URL            url of the InfluxDB's write endpoint where you'd like to post data
                            (example: http://localhost:8086/write?db=mydb)

Examples:
  csv2influx.py fixtures/*.csv --output-path=output/ --field-columns=speed --timestamp=2016-09-26T02:00:00+00:00
  csv2influx.py fixtures/sample.csv --output-path=output/result.out --field-columns=speed,strength --tag-columns=name,class --timestamp=2016-09-26
"""

# TODO:
#   --no-header               specify that the input file has no header row (TODO)
#   --csv-options             specify CSV options like the delimiter, etc (TODO)
#   --timestamp-precision     specify the precision to be used in the final result; possible values
#                             are n, u, ms, s, m, and h for nanoseconds, microseconds, milliseconds,
#                             seconds, minutes, and hours, respectively (TODO) [default: n]

import csv
from docopt import docopt
import arrow
from glob import glob
import logging
import tempfile
import os
import requests
import shutil
import cStringIO
import io


logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s %(levelname)s %(message)s')


def sanitize_measurement(value):
    """
    Measurement names must escape commas and spaces.
    """
    return value.replace(',', '\,').replace(' ', '\ ')


def sanitize_tag(value):
    """
    Tag keys and tag values must escape commas, spaces, and equal signs.
    """
    return value.replace(',', '\,').replace(' ', '\ ').replace('=', '\=')


def sanitize_field(value):
    """
    Field keys are always strings and follow the same syntactical rules as described above for tag keys and values.
    """
    return sanitize_tags(value)


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


    for i, p in enumerate(glob(arguments['PATH'])):
        logging.info('-- Processing file %d/%d: %s --' % (i+1, count_input_files, os.path.basename(p)))

        with open(p, 'rb') as input_file, open(output_path % {'basename': os.path.basename(p)}, 'ab+') if output_path else tempfile.TemporaryFile() as output_file:
            csvreader = csv.reader(input_file, delimiter=';')

            if not arguments.get('--no-header', False):
                # Extract header
                header = [s.lower() for s in csvreader.next()]

                # Convert field columns names into indexes
                field_columns_indexes = [header.index(s.lower()) for s in arguments['--field-columns'].split(',')]

                # Convert tag columns names into indexes
                if arguments['--tag-columns'] == '*':
                    tag_columns_indexes = filter(lambda i: i not in field_columns_indexes, range(0, len(header)))
                else:
                    tag_columns_indexes = [header.index(s.lower()) for s in arguments['--tag-columns'].split(',')]

            logging.debug('Header: %s' % header)
            logging.debug('Fields_indexes: %s' % field_columns_indexes)
            logging.debug('Tags_indexes: %s' % tag_columns_indexes)

            # Using a buffer instead of writing directly to the file helps to save some memory when writing to a file
            # *and* to a database at the same time
            buf = cStringIO.StringIO()
            for j, row in enumerate(csvreader):
                # Sanitize header since these values will be used as keys
                sanitized_header = [sanitize_tag(s) for s in header]

                key = '%s,%s' % (
                    sanitize_measurement(arguments['--measurement']),
                    ','.join(['%s=%s' % (sanitized_header[k], sanitize_tag(row[k])) for k in tag_columns_indexes])
                )

                fields = ','.join(['%s=%s' % (sanitized_header[l], sanitize_tag(row[l])) for l in field_columns_indexes])

                nano_timestamp = None
                if arguments['--timestamp']:
                    t = arrow.get(arguments['--timestamp'])

                    # Mimic nano seconds like a retard
                    nano_timestamp = ("%.6f" % t.float_timestamp).replace('.', '') + '000'

                buf.write('%s %s%s\n' % (key, fields, ' ' + nano_timestamp if nano_timestamp else ''))

            buf.seek(0)
            shutil.copyfileobj (buf, output_file)
            logging.info('Wrote %d lines to %s' % (j, output_file.name))

            if arguments['--load-url']:
                r = requests.post(arguments['--load-url'], data=buf.getvalue(), headers={'Content-Type': 'application/x-www-form-urlencoded'})
                logging.info('Wrote %d lines to %s' % (j, arguments['--load-url']))

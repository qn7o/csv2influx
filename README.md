# csv2influx

Write CSV data into InfluxDB thanks to Influx's Line Protocol syntax.
The CSV files must have a header row so csv2inlux can properly match and write fields' and tags' labels.
If you target more than one input CSV file, csv2influx will consider they are all identical in terms of structure and CSV options (delimiter, quoting, etc).
csv2influx won't validate fields' types; it will just add trailing 'i' for integers and double quotes strings.
Finally, tags won't be automatically sorted to improve performances.  Feel free to do so manually by providing --tag-columns with a sorted list if you prefer.

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
      csv2influx.py some/data/*.csv --output-path=output/ --measurement=movement --field-columns=speed:int --timestamp=2016-09-26T02:00:00+00:00
      csv2influx.py some/data/sample.csv --output-path=output/result.out --field-columns=speed:int,strength:float --tag-columns=name,class --timestamp=2016-09-26
      csv2influx.py some/data/*/sample.csv --field-columns=speed:int --timestamp=2016-09-26T02:00:00+00:00 --influx-url=http://localhost:8086/write?db=mydb

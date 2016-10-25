# -*- coding: utf-8 -*-
import logging


logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s %(levelname)s %(message)s',
    datefmt='%m/%d/%Y %H:%M:%S')


class LineProtocolExporter(object):
    """
    Export data to InfluxDB’s Line Protocol format based on specified input
    data structure (columns names) and desired output (measurement, fields,
    tags, timestamp).

    Any data going through the exporter is sanitized according to Influx's
    recommendations:
    https://docs.influxdata.com/influxdb/v1.0/write_protocols/line_protocol_tutorial/#data-types
    https://docs.influxdata.com/influxdb/v1.0/write_protocols/line_protocol_tutorial/#special-characters-and-keywords

    InfluxDB’s Line Protocol documentation can be found here:
    https://docs.influxdata.com/influxdb/v1.0/write_protocols/
    """
    VALID_FIELD_TYPES = ['float', 'int', 'str', 'bool']

    def __init__(self, labels, measurement, tag_columns, field_columns, field_types, timestamp):
        self.labels = [self.sanitize(s) for s in labels]
        self.measurement = self.sanitize_measurement(measurement)
        self.tag_columns = [self.sanitize(s) for s in tag_columns]
        self.timestamp = timestamp

        if any(typ not in self.VALID_FIELD_TYPES for typ in field_types):
            raise Exception('wrong field type.  Valid types are: %s' % self.VALID_FIELD_TYPES)
        elif len(field_columns) != len(field_types):
            raise Exception('number of field columns (%s) and types (%s) don\'t match.' % (
                len(field_columns),
                len(field_types)))
        else:
            self.field_columns = [self.sanitize(s) for s in field_columns]
            self.field_types = field_types

        # Consistency check: each specified tags and fields should match a label
        tags_and_fields_set = set(self.tag_columns + self.field_columns)
        labels_set = set(self.labels)
        if not tags_and_fields_set.issubset(labels_set):
            raise Exception('some specified tags or fields are not matching any label: %s' % ', '.join(tags_and_fields_set.difference(labels_set)))

        # Match field columns against provided arguments and convert labels into indexes
        self.field_columns_indexes = [self.labels.index(label) for label in self.field_columns]

        # Match tag columns against provided arguments and convert labels into indexes
        self.tag_columns_indexes = [labels.index(s) for s in self.tag_columns]

        logging.debug(
            'Fields labels and indexes: %s' % ', '.join(['%s: %s' % (i, field_col) for i, field_col in zip(self.field_columns_indexes, self.field_columns)])
        )
        logging.debug(
            'Tags labels and indexes: %s' % ', '.join(['%s: %s' % (i, tag_col) for i, tag_col in zip(self.tag_columns_indexes, self.tag_columns)])
        )

    @staticmethod
    def sanitize(value):
        """
        Measurements, tag keys, tag values, and field keys are always strings.
        For tag keys, tag values, and field keys always use a backslash character `\` to escape:
        commas `,`, equal signas `=`, spaces.
        """
        return value.replace(',', '\,').replace(' ', '\ ').replace('=', '\=').lower()

    @staticmethod
    def sanitize_measurement(value):
        """
        Measurements, tag keys, tag values, and field keys are always strings.
        For measurements always use a backslash character `\` to escape: commas `,`, spaces.
        """
        return value.replace(',', '\,').replace(' ', '\ ').lower()

    @staticmethod
    def sanitize_field_value(value, typ):
        """
        Field values can be floats (float), integers (int), strings (str), or booleans (bool).
        For string field values use a backslash character `\` to escape: double quotes `"`.
        """
        if typ == 'int':
            return '%si' % value
        elif typ == 'float':
            return value
        elif typ == 'str':
            return '"%s"' % value.replace('"', '\"')
        elif typ == 'bool':
            return value

    def export(self, lst):
        """
        Convert any iterable list of values to an InfluxDB’s Line Protocol valid text line.
        """
        if not len(lst) == len(self.labels):
            raise Exception('received %s values for %s labels' % (len(lst), len(self.labels)))

        tag_set = ','.join(['%s=%s' % (self.labels[i], self.sanitize(lst[i])) for i in self.tag_columns_indexes])
        key = ','.join([self.measurement] + [tag_set]) if tag_set else self.measurement
        field_set = ','.join(['%s=%s' % (self.labels[k], self.sanitize_field_value(value=lst[k], typ=self.field_types[j])) for j, k in enumerate(self.field_columns_indexes)])
        timestamp = self.timestamp

        return '%s %s %s' % (
            key,
            field_set,
            timestamp
        )

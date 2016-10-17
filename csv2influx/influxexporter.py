import logging


logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s %(levelname)s %(message)s',
    datefmt='%m/%d/%Y %H:%M:%S')


class InfluxExporter(object):
    FIELD_LABEL_TYPE_SEP = ':'
    FIELD_VALID_TYPES = ['float', 'int', 'str', 'bool']

    def __init__(self, labels, measurement, tag_columns, field_columns, timestamp):
        self.labels = [self.sanitize(s) for s in labels]
        self.measurement = self.sanitize_measurement(measurement)
        self.tag_columns = [self.sanitize(s) for s in tag_columns]
        self.field_columns = []
        self.field_types = {}
        for e in field_columns:
            label, typ = e.split(self.FIELD_LABEL_TYPE_SEP)
            if typ not in self.FIELD_VALID_TYPES:
                raise Exception('Wrong field type.  Valid types are: %s' % self.FIELD_VALID_TYPES)
            self.field_columns.append(self.sanitize(label))
            self.field_types[self.sanitize(label)] = typ
        self.timestamp = timestamp

        # Match field columns against provided arguments and convert labels into indexes
        self.field_columns_indexes = [self.labels.index(label) for label in self.field_columns]

        # Match tag columns against provided arguments and convert labels into indexes
        self.tag_columns_indexes = [labels.index(s) for s in self.tag_columns]

        logging.debug(
            'Fields labels and indexes: %s' % ','.join(['%s: %s' % (i, field_col) for i, field_col in zip(self.field_columns_indexes, self.field_columns)])
        )
        logging.debug(
            'Tags labels and indexes: %s' % ','.join(['%s: %s' % (i, tag_col) for i, tag_col in zip(self.tag_columns_indexes, self.tag_columns)])
        )

    @staticmethod
    def sanitize(value):
        """
        Measurements, tag keys, tag values, and field keys are always strings.
        For tag keys, tag values, and field keys always use a backslash character `\` to escape:
        commas `,`, equal signas `=`, spaces.
        https://docs.influxdata.com/influxdb/v1.0/write_protocols/line_protocol_tutorial/#data-types
        https://docs.influxdata.com/influxdb/v1.0/write_protocols/line_protocol_tutorial/#special-characters-and-keywords
        """
        return value.replace(',', '\,').replace(' ', '\ ').replace('=', '\=').lower()

    @staticmethod
    def sanitize_measurement(value):
        """
        Measurements, tag keys, tag values, and field keys are always strings.
        For measurements always use a backslash character `\` to escape: commas `,`, spaces.
        https://docs.influxdata.com/influxdb/v1.0/write_protocols/line_protocol_tutorial/#data-types
        https://docs.influxdata.com/influxdb/v1.0/write_protocols/line_protocol_tutorial/#special-characters-and-keywords
        """
        return value.replace(',', '\,').replace(' ', '\ ').lower()

    @staticmethod
    def sanitize_field_value(value, typ):
        """
        Field values can be floats (float), integers (int), strings (str), or booleans (bool).
        For string field values use a backslash character `\` to escape: double quotes `"`.
        https://docs.influxdata.com/influxdb/v1.0/write_protocols/line_protocol_tutorial/#data-types
        https://docs.influxdata.com/influxdb/v1.0//write_protocols/line_protocol_tutorial/#special-characters-and-keywords
        """
        if typ == 'int':
            return '%si' % value
        elif typ == 'float':
            return value
        elif typ == 'str':
            return '"%s"' % value.replace('"', '\"')
        elif typ == 'bool':
            return value

    def to_line_protocol_format(self, lst):
        """
        Convert any iterable list of values to an influxdb line protocol valid text line.
        """
        tag_set = ','.join(['%s=%s' % (self.labels[i], self.sanitize(lst[i])) for i in self.tag_columns_indexes])
        key = ','.join([self.measurement] + [tag_set]) if tag_set else self.measurement
        field_set = ','.join(['%s=%s' % (self.labels[j], self.sanitize_field_value(value=lst[j], typ=self.field_types[self.labels[j]])) for j in self.field_columns_indexes])
        timestamp = self.timestamp

        return '%s %s %s\n' % (
            key,
            field_set,
            timestamp
        )

import csv
import sys
csv.field_size_limit(sys.maxsize)


class CSVReaderGenerator(object):

    def __init__(self, infile, **kwargs):
        self.infile = infile
        self.filter_column_keys = set()
        try:
            self.delimiter = kwargs['delimiter']
            self.filter_column = kwargs['filter_column']
        except KeyError:
            self.delimiter = ','
            self.filter_column = None

    def get_header(self):
        with open(self.infile, 'r') as csv_file:
            header = next(csv.reader(csv_file, delimiter=self.delimiter))
            return header

    def __iter__(self):
        with open(self.infile, 'r') as csv_file:
            for row in csv.DictReader(csv_file, delimiter=self.delimiter):
                if self.filter_column:
                    if row[self.filter_column] not in self.filter_column_keys:
                        self.filter_column_keys.add(row[self.filter_column])
                        yield row
                else:
                    yield row



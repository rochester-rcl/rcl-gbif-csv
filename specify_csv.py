#!/usr/bin/env python

import argparse
import csv
import os
import sys
from tempfile import NamedTemporaryFile
from csv_generator import CSVReaderGenerator
from gbif_fetch import GBIFSpeciesFetcher


def reader_to_tempfile(csv_reader):
    # sort should also remove duplicated
    sorted_result = sorted(csv_reader, key=lambda row: row['species'])
    with NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as tmp:
        writer = csv.DictWriter(tmp, fieldnames=sorted_result[0].keys())
        writer.writeheader()
        writer.writerows(sorted_result)
        return os.path.abspath(tmp.name)


def species_to_csv(csv_path, outpath):
    header = None
    with open(csv_path, 'r') as in_csv:
        with open(outpath, 'w') as out_csv:
            reader = csv.DictReader(in_csv)
            next(reader)
            writer = csv.DictWriter(out_csv, fieldnames=GBIFSpeciesFetcher.filtered_fields, extrasaction='ignore', quoting=csv.QUOTE_ALL)
            writer.writeheader()
            for row in reader:
                print('Fetching Synonyms for {} with ID {}'.format(row['species'], row['specieskey']))
                species_data = GBIFSpeciesFetcher(row['specieskey'])
                species_data.fetch_all()
                print('Successfully fetched {} results'.format(len(species_data.results)))
                for data in species_data.results:
                    writer.writerow(data)


def clean_data(in_csv, out_csv):
    with open(out_csv) as output_csv:
        reader = CSVReaderGenerator(in_csv, filter_column='scientificname')
        writer = csv.DictWriter(out_csv, fieldnames=reader.get_header())
        result = sorted(reader, key=lambda row: row['scientificname'])
        writer.writerows(result)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Chunks out a huge csv into smaller ones for Specify import")
    parser.add_argument('-i', '--input', help="The path to the input file", required=True, type=str)
    parser.add_argument('-o', '--output', help="The path to the output file you want to save",
                        required=True, type=str)

    args = vars(parser.parse_args())
    file_input = args['input']
    outfile = args['output']
    reader = CSVReaderGenerator(file_input, delimiter='\t', filter_column='specieskey')
    header = reader.get_header()
    print('Filtering {} by column {}'.format(file_input, reader.filter_column))
    tmp_path = reader_to_tempfile(reader)
    print(tmp_path)
    # tmp_out = NamedTemporaryFile(mode='w', delete=False, suffix='.csv')
    species_to_csv(tmp_path, outfile)
    # clean_data(tmp_out.name, outfile)

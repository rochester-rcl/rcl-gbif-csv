import mysql.connector
from mysql.connector import errorcode
import os
import json
import sys
import argparse
import csv


class DBInfo:
    DEFAULT_HOST = '127.0.0.1'
    DEFAULT_CONFIG_FILENAME = 'specify_config.json'

    def __init__(self, config_file):
        if not config_file:
            self.config = DBInfo.configure()
        else:
            self.config = DBInfo.parse_config(config_file)

    @staticmethod
    def parse_config(config_file):
        try:
            with open(config_file, 'r') as config:
                return json.load(config)
        except IOError as error:
            print(error)
            sys.exit()

    @staticmethod
    def configure(**kwargs):
        print('Creating a config file to connect to the Specify database')
        config = {}
        config['database'] = input('Enter the name of your Specify database: ')
        config['user'] = input('Enter your Specify username: ')
        config['password'] = input('Enter your Specify password: ')
        config['host'] = input('Enter the host address for your Specify server: ')
        filepath = "{}/{}{}".format(os.path.abspath(os.getcwd()),
                                    kwargs['config_prefix'] if 'config_prefix' in kwargs else '',
                                    DBInfo.DEFAULT_CONFIG_FILENAME)
        try:
            with open(filepath, 'w') as config_file:
                json.dump(config, config_file)
                print('Successfully saved config to {}'.format(filepath))
        except IOError as error:
            print(error)
        return config


class SpecifySynonimize:
    REPORT_FILENAME = 'specify_synonyms_report.csv'
    REPORT_ACCEPTED_FILENAME = 'specify_accepted_report.csv'
    REPORT_FIELDNAMES = ['synonym', 'synonym_guid', 'synonym_specify_id', 'accepted_name', 'accepted_guid',
                         'accepted_specify_id']
    REPORT_ACCEPTED_FIELDNAMES = ['accepted_name', 'accepted_guid', 'accepted_specify_id']

    def __init__(self, db_config, input_csv):
        self.db_config = db_config
        self.connection = None
        self.input_csv = input_csv

    def connect(self):
        try:
            self.connection = mysql.connector.connect(**self.db_config)
            print('successfully connected to Database {}'.format(self.db_config['database']))
            return self
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print('Invalid username or password')
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print('Database does not exist')

    def find_synonyms(self):
        csv_file = open(self.input_csv, 'r')
        reader = csv.DictReader(csv_file)
        synonyms = [row for row in reader if row['synonym'] == 'True']
        csv_file.seek(0)
        accepted = [row for row in reader if row['synonym'] == 'False']
        results = []
        for synonym in synonyms:
            csv_file.seek(0)
            for row in reader:
                if synonym['speciesKey'] == row['taxonID']:
                    info = {
                        'synonym': synonym['canonicalName'],
                        'accepted_name': row['canonicalName'],
                        'synonym_guid': synonym['taxonID'],
                        'accepted_guid': row['taxonID']
                    }
                    results.append(info)
                    break
        csv_file.close()
        return results, accepted

    def synonymize_records(self, synonyms, accepted, **kwargs):
        query = 'SELECT TaxonID AS species_id, CommonName AS species_name, GUID as species_guid FROM taxon'
        cursor = self.connection.cursor(dictionary=True)
        cursor.execute(query)
        results = cursor.fetchall()
        try:
            if kwargs['dry_run']:
                self.do_report(synonyms, accepted, results)
            else:
                self.update_records(cursor, synonyms, accepted, results)
        except KeyError:
            pass
        cursor.close()

    @staticmethod
    def format_accepted(accepted, records):
        formatted = []
        for species in accepted:
            for record in records:
                if species['taxonID'] == record['species_guid']:
                    info = {
                        'accepted_specify_id': record['species_id'],
                        'accepted_name': species['canonicalName'],
                        'accepted_guid': species['taxonID']
                    }
                    formatted.append(info)
                    break
        return formatted

    @staticmethod
    def format_synonyms(synonyms, records):
        formatted = []
        for synonym in synonyms:
            for record in records:
                if synonym['synonym_guid'] == record['species_guid']:
                    info = {
                        'synonym_specify_id': record['species_id'],
                    }
                    for _record in records:
                        if synonym['accepted_guid'] == _record['species_guid']:
                            info['accepted_specify_id'] = _record['species_id']
                            formatted.append({**synonym, **info})
                            break
                    break
        return formatted

    def do_report(self, synonyms, accepted, records):
        filename = '{}/{}'.format(os.path.abspath(os.getcwd()), self.REPORT_FILENAME)
        with open(filename, 'w') as report:
            writer = csv.DictWriter(report, fieldnames=self.REPORT_FIELDNAMES)
            writer.writeheader()
            writer.writerows(SpecifySynonimize.format_synonyms(synonyms, records))

        accepted_filename = '{}/{}'.format(os.path.abspath(os.getcwd()), self.REPORT_ACCEPTED_FILENAME)
        with open(accepted_filename, 'w') as report:
            writer = csv.DictWriter(report, fieldnames=self.REPORT_ACCEPTED_FIELDNAMES)
            writer.writeheader()
            writer.writerows(SpecifySynonimize.format_accepted(accepted, records))

    def update_records(self, cursor, synonyms, accepted, records):
        accepted_query = 'UPDATE taxon SET IsAccepted = %(accepted_val)s WHERE TaxonID = %(accepted_id)s'
        synonym_query = 'UPDATE taxon SET AcceptedID = %(accepted_id)s, IsAccepted = %(accepted_val)s, FullName = %(synonym_name)s WHERE TaxonID =' \
                        ' %(synonym_id)s'

        formatted_accepted = SpecifySynonimize.format_accepted(accepted, records)
        formatted_synonyms = SpecifySynonimize.format_synonyms(synonyms, records)
        total_accepted = len(formatted_accepted)
        total_synonyms = len(formatted_synonyms)

        print('Setting {} accepted records as accepted taxa'.format(total_accepted))
        for _accepted in formatted_accepted:
            try:
                data = {
                    'accepted_id': _accepted['accepted_specify_id'],
                    'accepted_val': 1,
                }
                cursor.execute(accepted_query, data)
                self.connection.commit()
            except mysql.connector.Error as error:
                print(error)
                sys.exit()

        print('Synonymizing {} records'.format(total_synonyms))

        for synonym in formatted_synonyms:
            try:
                data = {
                    'accepted_id': synonym['accepted_specify_id'],
                    'synonym_id': synonym['synonym_specify_id'],
                    'synonym_name': synonym['synonym'],
                    'accepted_val': 0
                }
                cursor.execute(synonym_query, data)
                self.connection.commit()
            except mysql.connector.Error as error:
                print(error)
                sys.exit()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Uses a csv file to synonymize Specify Taxa")
    parser.add_argument('-i', '--input_file', help="input csv file", required=True,
                        type=str)
    parser.add_argument('-c', '--config', help="path to the db config file (json)", required=False, type=str)
    parser.add_argument('-d', '--dry_run',
                        help="If selected, this will export a csv report listing all species with their accepted names",
                        action='store_true', required=False, default=False)

    args = vars(parser.parse_args())
    csvfile = args['input_file']
    configfile = args['config']
    dry_run = args['dry_run']

    db_info = DBInfo(configfile)
    sp = SpecifySynonimize(db_info.config, csvfile)
    sp.connect()
    synonyms, accepted = sp.find_synonyms()
    results = sp.synonymize_records(synonyms, accepted, dry_run=dry_run)

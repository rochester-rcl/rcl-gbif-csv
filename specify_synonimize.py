import os
import json
import sys
import argparse
import csv
import mysql.connector
from mysql.connector import errorcode
from specify_base import SpecifyBase
from db_info import DBInfo


class SpecifySynonimize(SpecifyBase):
    REPORT_FILENAME = "specify_synonyms_report.csv"
    REPORT_ACCEPTED_FILENAME = "specify_accepted_report.csv"
    REPORT_FIELDNAMES = [
        "synonym",
        "synonym_guid",
        "synonym_specify_id",
        "accepted_name",
        "accepted_guid",
        "accepted_specify_id",
        "treedef",
        "species_treedef",
    ]
    REPORT_ACCEPTED_FIELDNAMES = [
        "accepted_name",
        "accepted_guid",
        "accepted_specify_id",
    ]

    def __init__(self, db_config, input_csv):
        super(SpecifySynonimize, self).__init__(db_config, input_csv)
        self.db_config = db_config
        self.connection = None
        self.input_csv = input_csv

    def find_synonyms(self):
        csv_file = open(self.input_csv, "r")
        reader = csv.DictReader(csv_file)
        synonyms = [row for row in reader if row["synonym"] == "True"]
        csv_file.seek(0)
        accepted = [row for row in reader if row["synonym"] == "False"]
        results = []
        for synonym in synonyms:
            csv_file.seek(0)
            for row in reader:
                if synonym["speciesKey"] == row["taxonID"]:
                    info = {
                        "synonym": synonym["canonicalName"],
                        "accepted_name": row["canonicalName"],
                        "synonym_guid": synonym["taxonID"],
                        "accepted_guid": row["taxonID"],
                    }
                    results.append(info)
                    break
        csv_file.close()
        return results, accepted

    def synonymize_records(self, synonyms, accepted, **kwargs):
        query = "SELECT TaxonID AS species_id, CommonName AS species_name, GUID AS species_guid, TaxonTreeDefID AS treedef FROM taxon"
        cursor = self.connection.cursor(dictionary=True)
        cursor.execute(query)
        results = cursor.fetchall()
        try:
            if kwargs["dry_run"]:
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
                if species["taxonID"] == record["species_guid"]:
                    info = {
                        "accepted_specify_id": record["species_id"],
                        "accepted_name": species["canonicalName"],
                        "accepted_guid": species["taxonID"],
                    }
                    formatted.append(info)
        return formatted

    @staticmethod
    def format_synonyms(synonyms, records):
        formatted = []
        for synonym in synonyms:
            for record in records:
                if synonym["synonym_guid"] == record["species_guid"]:
                    accepted_ids = set([])
                    for _record in records:
                        if synonym["accepted_guid"] == _record["species_guid"]:
                            info = {
                                "synonym_specify_id": record["species_id"],
                                "species_treedef": record["treedef"],
                            }
                            info["accepted_specify_id"] = _record["species_id"]
                            info["treedef"] = _record["treedef"]
                            if record["treedef"] == _record["treedef"]:
                                formatted.append({**synonym, **info})
        return formatted

    def do_report(self, synonyms, accepted, records):
        filename = "{}/{}".format(os.path.abspath(os.getcwd()), self.REPORT_FILENAME)
        with open(filename, "w") as report:
            writer = csv.DictWriter(report, fieldnames=self.REPORT_FIELDNAMES)
            writer.writeheader()
            writer.writerows(SpecifySynonimize.format_synonyms(synonyms, records))

        accepted_filename = "{}/{}".format(
            os.path.abspath(os.getcwd()), self.REPORT_ACCEPTED_FILENAME
        )
        with open(accepted_filename, "w") as report:
            writer = csv.DictWriter(report, fieldnames=self.REPORT_ACCEPTED_FIELDNAMES)
            writer.writeheader()
            writer.writerows(SpecifySynonimize.format_accepted(accepted, records))

    def update_records(self, cursor, synonyms, accepted, records):
        accepted_query = "UPDATE taxon SET IsAccepted = %(accepted_val)s WHERE TaxonID = %(accepted_id)s"
        synonym_query = (
            "UPDATE taxon SET AcceptedID = %(accepted_id)s, IsAccepted = %(accepted_val)s, FullName = %(synonym_name)s WHERE TaxonID ="
            " %(synonym_id)s"
        )

        formatted_accepted = SpecifySynonimize.format_accepted(accepted, records)
        formatted_synonyms = SpecifySynonimize.format_synonyms(synonyms, records)
        total_accepted = len(formatted_accepted)
        total_synonyms = len(formatted_synonyms)

        print("Setting {} accepted records as accepted taxa".format(total_accepted))
        for _accepted in formatted_accepted:
            try:
                data = {
                    "accepted_id": _accepted["accepted_specify_id"],
                    "accepted_val": 1,
                }
                cursor.execute(accepted_query, data)
                self.connection.commit()
            except mysql.connector.Error as error:
                print(error)
                sys.exit()

        print("Synonymizing {} records".format(total_synonyms))

        for synonym in formatted_synonyms:
            try:
                data = {
                    "accepted_id": synonym["accepted_specify_id"],
                    "synonym_id": synonym["synonym_specify_id"],
                    "synonym_name": synonym["synonym"],
                    "accepted_val": 0,
                }
                cursor.execute(synonym_query, data)
                self.connection.commit()
            except mysql.connector.Error as error:
                print(error)
                sys.exit()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Uses a csv file to synonymize Specify Taxa"
    )
    parser.add_argument(
        "-i", "--input_file", help="input csv file", required=True, type=str
    )
    parser.add_argument(
        "-c",
        "--config",
        help="path to the db config file (json)",
        required=False,
        type=str,
    )
    parser.add_argument(
        "-d",
        "--dry_run",
        help="If selected, this will export a csv report listing all species with their accepted names",
        action="store_true",
        required=False,
        default=False,
    )

    args = vars(parser.parse_args())
    csvfile = args["input_file"]
    configfile = args["config"]
    dry_run = args["dry_run"]

    db_info = DBInfo(configfile)
    sp = SpecifySynonimize(db_info.config, csvfile)
    sp.connect()
    synonyms, accepted = sp.find_synonyms()
    sp.synonymize_records(synonyms, accepted, dry_run=dry_run)

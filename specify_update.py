from specify_base import SpecifyBase
from db_info import DBInfo


class SpecifyExampleUpdater(SpecifyBase):
    def do_select(self):
        catalog_ids = [row["AltCatalogNumber"] for row in self.reader]
        id_set = ",".join(["%s"] * len(catalog_ids))
        query = "SELECT * FROM collectionobject WHERE collectionobject.AltCatalogNumber = 'RGL085117'"
        results = self.run_select(query)
        for result in results:
            print(result)

    def do_update(self):
        # AltCatalogNumber and CountAmt
        params = [{**row} for row in self.reader]
        queries = (
            [
                ("UPDATE collectionobject "
                "INNER JOIN preparation ON collectionobject.CollectionObjectID = preparation.CollectionObjectID "
                "SET preparation.CountAmt = %(CountAmt)s "
                "WHERE collectionobject.AltCatalogNumber = %(AltCatalogNumber)s")
            ]
            * len(params)
        )
        results = self.run_command(queries, tuple(params))
        len_rows = len(params)
        len_results = 0
        for res in results:
            len_results += res
        print("csv rows {} : db rows effected {}".format(len_rows, len_results))

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Uses a csv file to bulk update a Specify Database"
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

    args = vars(parser.parse_args())
    csvfile = args["input_file"]
    configfile = args["config"]

    db_info = DBInfo(configfile)
    updater = SpecifyExampleUpdater(db_info.config, csvfile)
    updater.connect()
    updater.do_update()

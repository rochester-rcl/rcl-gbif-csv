from csv_generator import CSVReaderGenerator
import mysql.connector
from mysql.connector import errorcode
import json
import sys
import os
from abc import ABC
from contextlib import contextmanager


class DBResultIterator:
    def __init__(self, cursor, result_size):
        self.cursor = cursor
        self.result_size = result_size

    def __iter__(self):
        all_results_fetched = False
        while not all_results_fetched:
            res = self.cursor.fetchmany(self.result_size)
            if len(res) > 0:
                yield res
            else:
                all_results_fetched = True


class SpecifyBase(ABC):
    class SpecifyToolException(Exception):
        pass

    DEFAULT_RESULT_SIZE = 10

    def __init__(self, db_config, input_csv):
        self.db_config = db_config
        self.connection = None
        self.input_csv = input_csv
        self._reader = None

    def connect(self):
        try:
            self.connection = mysql.connector.connect(**self.db_config)
            print(
                "successfully connected to database {}".format(
                    self.db_config["database"]
                )
            )
            return self
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Invalid username or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")

    @property
    def reader(self):
        if not self._reader:
            self._reader = CSVReaderGenerator(self.input_csv)
        return self._reader

    @contextmanager
    def get_cursor(self, **kwargs):
        cursor = self.connection.cursor(**kwargs)
        try:
            yield cursor
        finally:
            status = cursor.close()
            if status:
                print(
                    "successfully disconnected from database {}".format(
                        self.db_config["database"]
                    )
                )
            else:
                print("att")

    def run_select(self, query, params=None, **kwargs):
        if "SELECT" not in query:
            raise self.SpecifyToolException(
                "query parameter must be a SELECT statement"
            )
        with self.get_cursor(dictionary=True) as cursor:
            cursor.execute(query, params)
            for results in DBResultIterator(cursor, 20):
                yield results

    def run_command(self, cmd, params=None):
        err_msg = "cmd parameter cannot be a SELECT statement. Use the run_select method for queries"
        multi = True

        try:
            iter(cmd)
        except TypeError:
            multi = False

        if multi:
            if len([c for c in cmd if "SELECT" in c]) > 0:
                raise self.SpecifyToolException(err_msg)
            if params is not None and len(params) != len(cmd):
                raise self.SpecifyToolException(
                    "The number of parameters must be equal to the length of commands, or parameters must be None for multi-command execution"
                )
        else:
            if "SELECT" in cmd:
                raise self.SpecifyToolException(err_msg)
        #TODO batch commits for transactions in chunks?
        with self.get_cursor(dictionary=True) as cursor:
            if multi:
                for c, p in zip(cmd, params):
                    cursor.execute(c, p)
                    self.connection.commit()
                    yield cursor.rowcount
            else:
                cursor.execute(cmd, params)
                self.connection.commit()
                yield cursor.rowcount

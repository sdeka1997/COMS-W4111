# I write and test methods one at a time.
# This file contains unit tests of individual methods.

from src.CSVDataTable import CSVDataTable
import logging
import os
import time
import json

# The logging level to use should be an environment variable, not hard coded.
logging.basicConfig(level=logging.DEBUG)

# Also, the 'name' of the logger to use should be an environment variable.
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# This should also be an environment variable.
# Also not the using '/' is OS dependent, and windows might need `\\`
data_dir = os.path.abspath("../Data/Baseball")


def test_project():
    template = {
        "playerID": "worthal01",
        "teamID": "BOS",
        "yearID": "1960",
        "AB": "1",
        "H": "0",
        "HR": "0",
        "RBI": "0"
    }

    query = ["playerID", "teamID", "yearID"]

    print("Select = ", CSVDataTable.project(template, query))




def test_matches():
    template = {
        "playerID": "worthal01",
        "teamID": "BOS",
        "yearID": "1960",
        "AB": "1",
        "H": "0",
        "HR": "0",
        "RBI": "0"
    }

    match = {"teamID": "BOS"}

    print("Matches = ", CSVDataTable.matches_template(template, match))


def test_load():
    connect_info = {
        "directory": data_dir,
        "file_name": "Batting.csv"
    }

    csv_tbl = CSVDataTable("people", connect_info, key_columns=['playerID'])

    print("Created table = " + str(csv_tbl))


def test_find_by_template():
    connect_info = {
        "directory": data_dir,
        "file_name": "_Small.csv"
    }

    key_cols = ['playerID', 'teamID', 'yearID', 'stint']
    fields = ['playerID', 'teamID', 'yearID', 'AB', 'H', 'HR', 'RBI']
    template = {'teamID': 'CL1', 'yearID': '1871', "stint": "1"}

    csv_tbl = CSVDataTable("batting", connect_info, key_columns=key_cols)

    output = csv_tbl.find_by_template(template=template, field_list=None)

    print("Query Result: \n", json.dumps(output, indent=2))


def test_find_by_primary_key():
    connect_info = {
        "directory": data_dir,
        "file_name": "_Small.csv"
    }

    key_cols = ['yearID', 'stint', 'teamID']
    key_values = ["1871", "1", "CL1"]

    csv_tbl = CSVDataTable("batting", connect_info, key_columns=key_cols)

    output = csv_tbl.find_by_primary_key(key_fields=key_values)

    print("Query Result: \n", json.dumps(output, indent=2))


def test_delete_by_template():
    connect_info = {
        "directory": data_dir,
        "file_name": "_Small.csv"
    }

    key_cols = ['playerID', 'yearID', 'stint', 'teamID']
    template = {'teamID': 'CL1', 'yearID': '1871'}

    csv_tbl = CSVDataTable("batting", connect_info, key_columns=key_cols)

    output = csv_tbl.delete_by_template(template=template)

    print("Query Result: \n", json.dumps(output, indent=2))


def test_delete_by_key():
    connect_info = {
        "directory": data_dir,
        "file_name": "_Small.csv"
    }

    key_cols = ['playerID', 'yearID', 'stint', 'teamID']
    key_values = ["battijo01", '1871', '1', 'CL1']

    csv_tbl = CSVDataTable("batting", connect_info, key_columns=key_cols)

    output = csv_tbl.delete_by_key(key_fields=key_values)

    print("Query Result: \n", json.dumps(output, indent=2))


def test_update_by_template():
    connect_info = {
        "directory": data_dir,
        "file_name": "_Small.csv"
    }

    key_cols = ['playerID', 'yearID', 'stint', 'teamID']
    template = {'teamID': 'CL1', 'yearID': '1871'}
    new_values = {"playerID": "battijo01", 'G': '6', 'AB': '20'}

    csv_tbl = CSVDataTable("batting", connect_info, key_columns=key_cols)

    output = csv_tbl.update_by_template(template=template, new_values=new_values)

    print("Query Result: \n", json.dumps(output, indent=2))


def test_update_by_key():
    connect_info = {
        "directory": data_dir,
        "file_name": "_Small.csv"
    }

    key_cols = ['playerID', 'yearID', 'stint', 'teamID']
    key_values = ["battijo01", '1871', '1', 'CL1']
    new_values = {"playerID": "swapdog", 'G': '6', 'AB': '20'}

    csv_tbl = CSVDataTable("batting", connect_info, key_columns=key_cols)

    output = csv_tbl.update_by_key(key_fields=key_values, new_values=new_values)

    print("Query Result: \n", json.dumps(output, indent=2))


def test_insert():
    connect_info = {
        "directory": data_dir,
        "file_name": "_Small.csv"
    }

    key_cols = ['playerID', 'yearID', 'stint', 'teamID']
    row = {"playerID": "swapdog", "yearID": '2019', "stint": '1', 'teamID': 'TEX',
           'G': '6', 'AB': '20', 'R': '20', 'H': '20', '2B': '20', '3B': '20', 'HR': '20',
           'RBI': '20', 'SB': '20', 'CS': '20', 'BB': '20', 'SO': '20', 'IBB': '20', 'HBP': '20',
           'SH': '20', 'SF': '20', 'GIDP': '20'}

    csv_tbl = CSVDataTable("batting", connect_info, key_columns=key_cols)

    output = csv_tbl.insert(new_record=row)

    print("Query Result: \n", json.dumps(output, indent=2))


test_project()
test_matches()
test_load()
test_find_by_template()
test_find_by_primary_key()
test_delete_by_template()
test_delete_by_key()
test_update_by_template()
test_update_by_key()
test_insert()
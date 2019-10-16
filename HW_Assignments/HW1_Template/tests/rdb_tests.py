from src.RDBDataTable import RDBDataTable
import pymysql
import logging
import json
import src.dbutils as dbutils

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


def t1():
    c_info = {
        "host": "localhost",
        "port": 3306,
        "user": "root",
        "password": "riceowls",
        "db": "lahman2019raw",
        "cursorclass": pymysql.cursors.DictCursor
    }

    r_dbt = RDBDataTable("People", connect_info=c_info, key_columns=['playerID'])
    print("RDB table = ", r_dbt)


def test_rdb_find_by_template():
    c_info = {
        "host": "localhost",
        "port": 3306,
        "user": "root",
        "password": "riceowls",
        "db": "lahman2019raw",
        "cursorclass": pymysql.cursors.DictCursor
    }

    r_dbt = RDBDataTable("People", connect_info=c_info, key_columns=['playerID'])
    res = r_dbt.find_by_template({"birthCity": "Plano"},
                                 field_list=['playerID', 'nameLast', 'nameFirst', 'birthCity', 'birthYear'])

    print("Res = ", json.dumps(res, indent=2))


def test_rdb_find_by_key():
    c_info = {
        "host": "localhost",
        "port": 3306,
        "user": "root",
        "password": "riceowls",
        "db": "lahman2019raw",
        "cursorclass": pymysql.cursors.DictCursor
    }

    r_dbt = RDBDataTable("Batting", connect_info=c_info, key_columns=['playerID', 'teamID', 'yearID', 'stint'])
    res = r_dbt.find_by_primary_key(['willite01', 'BOS', '1960', '1'],
                                    field_list=['playerID', 'teamID', 'yearID', 'stint', 'H', 'AB'])

    print("Res = ", json.dumps(res, indent=2))


def test_create_insert():
    new_row = {"playerID": "willite01", "teamID": "BOS", "USMC": "Cool"}
    sql, args = dbutils.create_insert("People", new_row=new_row)

    print("SQL = ", sql)
    print("args = ", args)


def test_insert_delete():
    print("Testing insert and delete.")

    new_row = {"playerID": "sdeka1997", "nameLast": "Deka", "nameFirst": "Swapnav", 'weight': '184'}
    print("Row to insert = ", new_row)

    c_info = {
        "host": "localhost",
        "port": 3306,
        "user": "root",
        "password": "riceowls",
        "db": "lahman2019raw",
        "cursorclass": pymysql.cursors.DictCursor
    }

    r_dbt = RDBDataTable("People", connect_info=c_info, key_columns=['playerID'])

    print("Testing find...")
    res = r_dbt.find_by_template({"playerID": "sdeka1997"})
    print("Result of find = ", res)

    print("Doing insert...")
    res = r_dbt.insert(new_row)

    print("Testing find again...")
    res = r_dbt.find_by_template({"playerID": "sdeka1997"})
    print("Result of delete = ", res)

    print("Testing delete...")
    res = r_dbt.delete_by_template({"playerID": "sdeka1997"})
    print("Result of find = ", res)

    print("Testing find again...")
    res = r_dbt.find_by_template({"playerID": "sdeka1997"})
    print("Result of find = ", res)


def test_create_update():
    new_cols = {"playerID": "sdeka1997", "birthCountry": "Plano"}
    template = {"playerID": "willite01"}

    sql, args = dbutils.create_update('People', template=template, changed_cols=new_cols)
    print("SQL = ", sql)
    print("args = ", args)


def test_all():
    print("Testing insert, update, and delete.")

    new_row = {"playerID": "sdeka1997", "nameLast": "Deka", "nameFirst": "Swapnav", 'weight': '184'}
    print("Row to insert = ", new_row)

    c_info = {
        "host": "localhost",
        "port": 3306,
        "user": "root",
        "password": "riceowls",
        "db": "lahman2019raw",
        "cursorclass": pymysql.cursors.DictCursor
    }

    r_dbt = RDBDataTable("People", connect_info=c_info, key_columns=['playerID'])

    print("Testing find...")
    res = r_dbt.find_by_template({"playerID": "sdeka1997"})
    print("Result of find = ", res)

    print("Doing insert...")
    res = r_dbt.insert(new_row)

    print("Testing find again...")
    res = r_dbt.find_by_template({"playerID": "sdeka1997"})
    print("Result of find = ", res)

    new_cols = {"nameLast": "Deka", "birthCountry": "Plano"}

    print("Testing update...new = ", new_cols)
    res = r_dbt.update_by_template({"playerID": "sdeka1997"}, new_values=new_cols)
    print("Result of update = ", res)

    print("Testing find again...")
    res = r_dbt.find_by_template({"playerID": "sdeka1997"})
    print("Result of find = ", res)

    print("Testing delete...")
    res = r_dbt.delete_by_template({"playerID": "sdeka1997"})
    print("Result of delete = ", res)

    print("Testing find again...")
    res = r_dbt.find_by_template({"playerID": "sdeka1997"})
    print("Result of find = ", res)


def test_all_key():
    print("Testing insert, update, and delete.")

    new_row = {"playerID": "sdeka1997", "nameLast": "Deka", "nameFirst": "Swapnav", 'weight': '184'}
    print("Row to insert = ", new_row)

    c_info = {
        "host": "localhost",
        "port": 3306,
        "user": "root",
        "password": "riceowls",
        "db": "lahman2019raw",
        "cursorclass": pymysql.cursors.DictCursor
    }

    r_dbt = RDBDataTable("People", connect_info=c_info, key_columns=['playerID'])

    print("Testing find...")
    res = r_dbt.find_by_primary_key(['sdeka1997'])
    print("Result of find = ", res)

    print("Doing insert...")
    res = r_dbt.insert(new_row)

    print("Testing find again...")
    res = r_dbt.find_by_primary_key(['sdeka1997'])
    print("Result of find = ", res)

    new_cols = {"nameLast": "Deka", "birthCountry": "Plano"}

    print("Testing update...new = ", new_cols)
    res = r_dbt.update_by_key(['sdeka1997'], new_values=new_cols)
    print("Result of update = ", res)

    print("Testing find again...")
    res = r_dbt.find_by_primary_key(['sdeka1997'])
    print("Result of find = ", res)

    print("Testing delete...")
    res = r_dbt.delete_by_key(["sdeka1997"])
    print("Result of delete = ", res)

    print("Testing find again...")
    res = r_dbt.find_by_primary_key(['sdeka1997'])
    print("Result of find = ", res)


t1()
test_rdb_find_by_template()
test_rdb_find_by_key()
test_create_insert()
test_insert_delete()
test_create_update()
test_all()
test_all_key()

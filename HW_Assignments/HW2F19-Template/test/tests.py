import src.data_service.data_table_adaptor as dta
import json


def test1():
    t = dta.get_rdb_table("people", "lahman2019")
    print(t)


def test2():
    d = dta.get_databases()
    print("databases test: ", json.dumps(d, indent=2))


def test3(name):
    d = dta.get_tables(name)
    print("tables test: ", json.dumps(d, indent=2))

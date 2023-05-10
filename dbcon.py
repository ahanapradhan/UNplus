import psycopg2

import reveal_globals


def getconn():
    # change port
    conn = psycopg2.connect(
        database=reveal_globals.database_in_use, user='postgres', password='postgres', host='localhost', port='5432')
    return conn


def establishConnection():
    print("inside------reveal_support.establishConnection")
    reveal_globals.global_db_engine = 'PostgreSQL'
    reveal_globals.global_conn = getconn()
    if reveal_globals.global_conn is None:
        return False
    print("connected...")
    return True

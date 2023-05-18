import psycopg2

from UNplus import reveal_globals


def getconn():
    # change port
    conn = psycopg2.connect(
            database=reveal_globals.database_in_use, user='postgres', password='postgres', host='localhost',
            port='5432')
    return conn


def establishConnection(reveal_globals):
    print("inside------reveal_support.establishConnection")
    reveal_globals.global_db_engine = 'PostgreSQL'
    reveal_globals.global_conn = getconn()
    if reveal_globals.global_conn is None:
        return False, reveal_globals
    print("connected...")
    return True, reveal_globals


def execute_sql(sqls):
    cur = reveal_globals.global_conn.cursor()
    print(cur)
    for sql in sqls:
        cur.execute(sql)
    cur.close()


def execute_sql_fetchone(sql):
    cur = reveal_globals.global_conn.cursor()
    cur.execute(sql)
    prev = cur.fetchone()
    prev = prev[0]
    cur.close()
    return prev


conn = None

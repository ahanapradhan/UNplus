import reveal_globals
import psycopg2
def getconn() :
    #change port 
    conn = psycopg2.connect(
   database=reveal_globals.database_in_use, user='postgres', password='root', host='localhost', port= '5432'
)
    return conn


def establishConnection():
    print("inside------reveal_support.establishConnection")
    # reveal_globals.global_connection_string = str('0.0.0.0,5432,' + reveal_globals.global_db_instance + ',,,')
    # arg = reveal_globals.global_connection_string.split(',')
    reveal_globals.global_db_engine = 'PostgreSQL'
    conn=getconn()
    reveal_globals.global_conn = conn
    print("connected...")
    return True
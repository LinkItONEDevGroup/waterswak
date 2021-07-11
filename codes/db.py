# @file db.py
# @brief database related library
# @author wuulong@gmail.com
import psycopg2
from configparser import ConfigParser
import pandas as pd

def config(filename='include/database.ini', section='postgresql'):
    """ generate db parameters from ini file"""
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)

    # get section, default to postgresql
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    return db

def connect_test():
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # read connection parameters
        params = config()

        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)

        # create a cursor
        cur = conn.cursor()

        # execute a statement
        print('PostgreSQL database version:')
        cur.execute('SELECT version()')

        # display the PostgreSQL database server version
        db_version = cur.fetchone()
        print(db_version)

        # close the communication with the PostgreSQL
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')

def connect_db():
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # read connection parameters
        params = config()

        # connect to the PostgreSQL server

        conn = psycopg2.connect(**params)
        print('Connected to the PostgreSQL database...')

        return conn
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        return None

def close_db(conn):
    if conn is not None:
        conn.close()
        print('Database connection closed.')
        return None

def sql_exec(conn, sql):

    try:
        # create a cursor
        cur = conn.cursor()

        # execute a statement
        cur.execute(sql)
        conn.commit()
        # close the communication with the PostgreSQL
        cur.close()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    # execute a statement
    #print('PostgreSQL database version:')
    #cur.execute('SELECT version()')

def sql_to_df(conn, sql):
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        #print("pass conn")
        #sql = "select * from rivercode order by river_id"
        df = pd.read_sql(sql, conn)
        return df
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

def df_to_db(table_name,df,action_str='replace'):
    """
    action_str: df.to_sql if_exists parameter: replace, append, ..
    """
    import psycopg2
    import pandas as pds
    from sqlalchemy import create_engine

    dataFrame = df
    # Create an engine instance
    alchemyEngine   = create_engine('postgresql+psycopg2://postgres:your_password@127.0.0.1:5431/postgis', pool_recycle=3600);
    postgreSQLConnection    = alchemyEngine.connect();
    postgreSQLTable         = table_name

    try:
        frame           = dataFrame.to_sql(postgreSQLTable, postgreSQLConnection, if_exists=action_str,index=False);
    except ValueError as vx:
        print(vx)
    except Exception as ex:
        print(ex)
    else:
        print("PostgreSQL Table %s has been created successfully."%postgreSQLTable);
    finally:
        postgreSQLConnection.close();
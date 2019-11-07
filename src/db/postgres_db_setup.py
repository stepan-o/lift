import pandas as pd
import json
import psycopg2
from pandas.io.json import json_normalize
from psycopg2 import Error, DatabaseError
from time import time
pd.set_option('display.max_columns', 500)


def db_check_connection(conn, cur):
    try:
        # display PostgreSQL connection properties
        print("PostgreSQL connection properties:\n{0}\n".format(conn.get_dsn_parameters()))

        # display server version
        query = 'SELECT version();'
        cur.execute(query)
        server_version = cur.fetchone()
        print("-- You are connected to {0}\n".format(server_version))

        # display tables in the current database
        query = 'SELECT * FROM yelp.pg_catalog.pg_stat_user_tables;'
        cur.execute(query)
        tables = pd.DataFrame(cur.fetchall())
        tables = tables[[1, 2]]
        tables.columns = ['schema', 'table_name']
        print("Tables in the current schema:\n{0}\n".format(tables))

    except (Exception, Error) as conn_error:
        print("Error while connecting to PostgreSQL", conn_error)
    return


def db_execute_queries(conn, cur, queries, verbose=False):
    try:
        # execute provided queries
        print("------ Executing {0} queries".format(len(queries)))
        query_count = 0
        for query in queries:
            query_count += 1
            if verbose:
                print("-- Executing query {0}:\n{1}".format(query_count, query))
            else:
                print("-- Executing query {0}: {1}...".format(query_count, query[:30]))
            t = time()
            cur.execute(query)
            elapsed = time() - t
            print("Query executed, took {0:,.2f} seconds ({1:,.2f} minutes)\n"
                  .format(elapsed, elapsed / 60))

        # persist changes to the database
        print("------ All queries executed successfully, persisting changes to the database...")
        conn.commit()
        print("{0} Changes persisted\n".format('-' * 15))

    except (Exception, Error, DatabaseError) as ddl_error:
        # in case of an error, roll back changes
        print("\n{0} Error while executing query:\n{1}".format('-' * 15, ddl_error))
        print("------ Reverting changes from the transaction...")
        conn.rollback()
        print("{0} Changes rolled back\n".format('-' * 15))


def db_copy_from(conn, cur, file_path, target_table, header=True, sep=',', null='\\N'):
    try:
        # copy data from file into the table
        print("-- Copying data into '{0}' table from file {1}...".format(target_table, file_path))
        t = time()
        with open(file_path, 'r') as f_in:
            if header:
                next(f_in)
            cur.copy_from(f_in, target_table, sep=sep, null=null)
        elapsed = time() - t
        # persist changes to the database
        print("Data copied, took {0:,.2f} seconds ({1:,.2f} minutes, persisting changes to the database..."
              .format(elapsed, elapsed / 60))
        conn.commit()
        print("{0} Changes persisted\n".format('-' * 15))

    except (Exception, Error, DatabaseError) as copy_error:
        # in case of an error, roll back changes
        print("\n{0} Error while executing query:\n{1}".format('-' * 15, copy_error))
        print("------ Reverting changes from the transaction...")
        conn.rollback()
        print("{0} Changes rolled back\n".format('-' * 15))


def db_insert_json(conn, cur, json_path, target_table, batch_size):
    try:
        # copy data from file into the table
        print("-- Insert data into '{0}' table from file {1}, batch size = {2:,}..."
              .format(target_table, json_path, batch_size))
        t = time()
        with open(json_path, 'r') as f_in:
            lines = f_in.readlines()
            row = 0
            json_df = pd.DataFrame()
            for line in lines:
                if row % 10000 == 0:
                    elpsd = time() - t
                    print("Inserted {0:,.0f} rows, so far took {1:,.2f} seconds ({2:,.2f} minutes)"
                          .format(row, elpsd, elpsd / 60))
                row += 1
                line_json = json.loads(line)
                line_df = json_normalize(line_json)
                json_df = json_df.append(line_df, sort=True)
                if len(json_df) == batch_size:
                    json_df = json_df.fillna('NULL').replace('None', 'NULL')
                    json_tuple = [tuple(x) for x in json_df.values]
                    insert_query_string = 'INSERT INTO {0} ( '.format(target_table)
                    i = 0
                    for col in json_df.columns:
                        if '.' in col:
                            col = '"' + col + '"'
                        insert_query_string = insert_query_string + col
                        i += 1
                        if i < len(json_df.columns):
                            insert_query_string = insert_query_string + ', '
                    insert_query_string = insert_query_string + ' ) VALUES ({0})'\
                        .format(','.join(['%s'] * len(json_df.columns)))
                    cur.executemany(insert_query_string, json_tuple)
                    json_df = pd.DataFrame()

        elapsed = time() - t
        # persist changes to the database
        print("Data inserted, took {0:,.2f} seconds ({1:,.2f} minutes, persisting changes to the database..."
              .format(elapsed, elapsed / 60))
        conn.commit()
        count = cur.rowcount
        print("{0} Changes persisted, {1} rows inserted\n".format('-' * 15, count))

    except (Exception, Error, DatabaseError) as insert_error:
        # in case of an error, roll back changes
        print("\n{0} Error while executing query:\n{1}".format('-' * 15, insert_error))
        print("------ Reverting changes from the transaction...")
        conn.rollback()
        print("{0} Changes rolled back\n".format('-' * 15))


def db_setup(connection_params, ddl_queries=None, ingest_list=None):
    print("------ Connecting to database '{0}'...".format(connection_params['database']))
    tt = time()
    with psycopg2.connect(**connection_params) as conn:
        with conn.cursor() as cur:
            conn.autocommit = False
            db_check_connection(conn, cur)
            batch_size = input("Specify batch size (integer) for json INSERT : ")
            if ddl_queries:
                print("------ Executing DDL queries")
                db_execute_queries(conn, cur, ddl_queries)
                print("All DDL queries executed\n")
            else:
                print("------ No DDL queries to execute")
            if ingest_list:
                print("------ Executing DML queries. {0} sources provided for ingestion"
                      .format(len(ingest_list)))
                for source in ingest_list:
                    db_insert_json(conn, cur, source['file_path'], source['target_table'], batch_size=batch_size)

    # close connection to the database
    if cur:
        cur.close()
    if conn:
        conn.close()
    t_elapsed = time() - tt
    print("--- Database connection closed, total time {0:,.2f} seconds ({1:,.2f} minutes)."
          .format(t_elapsed, t_elapsed / 60))
    return

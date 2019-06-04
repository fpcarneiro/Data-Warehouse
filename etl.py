import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    This function loads the staging tables through execution of COPY comands defined in 'sql_queries' module.
    This is the first step of the pipeline in AWS where we transfer the records from JSON files in Amazon S3 to Amazon Redshift cluster.
    
    Args:
        cur (:obj:`cursor`): One object of the class cursor. 
            It allows the execution of PostgreSQL commands in a database session. 
            All the commands are executed in the context of the database session wrapped by the connection.
        conn (:obj:`connection`): One object of the class connection that encapsulates a database session. 
            It handles the connection to a PostgreSQL database instance. 
            Since Redshift is compatible with PostgreSQL we can use the same library to connect and manipulate Amazon Redshift.
    """
    
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    This function insert the records in the data warehouse in Redshift tables. The data is read from staging tables using SQL statements defined in 'sql_queries' module.
    This is the second step of the pipeline in AWS.
    
    Args:
        cur (:obj:`cursor`): One object of the class cursor. 
            It allows the execution of PostgreSQL commands in a database session. 
            All the commands are executed in the context of the database session wrapped by the connection.
        conn (:obj:`connection`): One object of the class connection that encapsulates a database session. 
            It handles the connection to a PostgreSQL database instance. 
            Since Redshift is compatible with PostgreSQL we can use the same library to connect and manipulate Amazon Redshift.
    """
    
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

def main():
    """
    Main function, used to connect to the data warehouse in Amazon Redshift and execute the data pipeline by calling 'load_staging_tables' and 'insert_tables' functions.
    """
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    
    conn.close()


if __name__ == "__main__":
    main()
import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    This function drops the tables in the data warehouse in Amazon Redshift. The commands to do so are read from 'sql_queries' module.
    This way, we can run create_tables.py whenever needed to reset the database and test the ETL pipeline.
    
    Args:
        cur (:obj:`cursor`): One object of the class cursor. 
            It allows the execution of PostgreSQL commands in a database session. 
            All the commands are executed in the context of the database session wrapped by the connection.
        conn (:obj:`connection`): One object of the class connection that encapsulates a database session. 
            It handles the connection to a PostgreSQL database instance. 
            Since Redshift is compatible with PostgreSQL we can use the same library to connect and manipulate Amazon Redshift.
    """
    
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    This function creates the tables in the data warehouse in Amazon Redshift. The commands to do so are read from 'sql_queries' module.
    
    Args:
        cur (:obj:`cursor`): One object of the class cursor. 
            It allows the execution of PostgreSQL commands in a database session. 
            All the commands are executed in the context of the database session wrapped by the connection.
        conn (:obj:`connection`): One object of the class connection that encapsulates a database session. 
            It handles the connection to a PostgreSQL database instance. 
            Since Redshift is compatible with PostgreSQL we can use the same library to connect and manipulate Amazon Redshift.
    """    
    
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Main function, used to connect to the data warehouse in Amazon Redshift and the to drop and create the tables by calling 'drop_tables' and 'create_tables' functions.
    """
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
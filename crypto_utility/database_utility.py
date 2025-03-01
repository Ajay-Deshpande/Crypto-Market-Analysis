class DatabaseUtility:
    def __init__(self, username=None, password=None, host="localhost", port=5432):
        import os
        if not username or not password:
            self.username = os.environ.get("POSTGRES_USER", "postgres")
            self.password = os.environ.get("POSTGRES_USER", "postgres")
        self.host = host
        self.port = port
        self.conn = None
        self.conn = self.connect()
        

    def connect(self):
        import psycopg2
        
        conn = psycopg2.connect(username=self.username, 
                                     password=self.password,
                                     host=self.host,
                                     port=self.port)
        conn.autocommit = True
        return conn
    
    def create_database(self, database_name):
        if not self.conn:
            self.conn = self.connect()
        try:
            cursor = self.conn.cursor()
            cursor.execute(f"SELECT * FROM pg_catalog.pg_database WHERE datname = '{database_name}';")
            if not cursor.fetchone():
                cursor.execute(f'CREATE DATABASE {database_name}')
        except Exception as e:
            print("Couldn't create database")
            raise e
        finally:
            self.conn.close()

    def create_table(self, process_name):
        if not self.conn:
            self.conn = self.connect()
        try:
            cursor = self.conn.cursor()
            cursor.execute(f"SELECT * FROM pg_catalog.pg_database WHERE datname = '{database_name}';")
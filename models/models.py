import snowflake.connector as snowflake


class SnowflakeConnector:
    def __init__(self, user, password, account, warehouse=None, database=None, schema='PUBLIC'):
        self.user = user
        self.password = password
        self.account = account
        self.warehouse = warehouse
        self.database = database
        self.schema = schema
        self.connection = None

    def connect(self):
        try:
            self.connection = snowflake.connect(
                user=self.user,
                password=self.password,
                account=self.account,
                warehouse=self.warehouse,
                database=self.database,
                schema=self.schema
            )
        except snowflake.Error as e:
            print(f'[ERROR]: {e}')
            return e

    def close(self):
        if self.connection:
            print('[INFO]: connection was closed')
            self.connection.close()
            self.connection = None

    def execute_query(self, query):
        cursor = self.connection.cursor()
        try:
            cursor.execute(query)
            self.connection.commit()
            cursor.close()
        except snowflake.Error as e:
            self.connection.rollback()
            print(f'[ERROR]: {e}')
            return e

    def fetch_one(self, query):
        cursor = self.connection.cursor()
        try:
            cursor.execute(query)
            result = cursor.fetchone()
            cursor.close()
            return result
        except snowflake.Error as e:
            print(f'[ERROR]: {e}')
            return e

    def fetch_all(self, query):
        cursor = self.connection.cursor()
        try:
            cursor.execute(query)
            result = cursor.fetchall()
            cursor.close()
            return result
        except snowflake.Error as e:
            print(f'[ERROR]: {e}')
            return e

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


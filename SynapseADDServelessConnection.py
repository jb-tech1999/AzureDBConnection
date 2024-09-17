import struct
import pyodbc
from azure.identity import AzureCliCredential

class SynapseDB:
    """
    A class representing a connection to an Azure Synapse database.
    Args:
        server (str): The server name or IP address.
        database (str): The name of the database.
        driver (str): The ODBC driver version.
    Attributes:
        server (str): The server name or IP address.
        database (str): The name of the database.
        driver (str): The ODBC driver version.
        conn (pyodbc.Connection): The connection object to the database.
    Methods:
        connect_to_db(): Connects to the Azure Synapse database using the provided credentials.
        count_rows(table_name: str) -> int: Returns the number of rows in the specified table.
        close_connection(): Closes the connection to the database.
    """

    def __init__(self, server, database, driver):
        self.server = server
        self.database = database
        self.driver = driver
        self.connect_to_db()

    def connect_to_db(self):
        """
        Connects to the Azure SQL Database using the provided credentials.
        Returns:
            pyodbc.Connection: The connection object to the Azure SQL Database.
        """
        credential = AzureCliCredential()
        database_token = credential.get_token('https://database.windows.net/')

        # get bytes from token obtained
        tokenb = bytes(database_token[0], "UTF-8")
        exptoken = b''
        for i in tokenb:
            exptoken += bytes({i})
            exptoken += bytes(1)
            tokenstruct = struct.pack("=i", len(exptoken)) + exptoken

        # build connection string using acquired token
        conn_string = "Driver={ODBC Driver "+str(
            self.driver)+" for SQL Server};SERVER="+self.server+";DATABASE="+self.database+""
        SQL_COPT_SS_ACCESS_TOKEN = 1256
        conn = pyodbc.connect(conn_string, attrs_before={
                              SQL_COPT_SS_ACCESS_TOKEN: tokenstruct})
        self.conn = conn

    def count_rows(self, table_name) -> int:
        """
        Counts the number of rows in the specified table.

        Parameters:
            table_name (str): The name of the table.

        Returns:
            int: The number of rows in the table.
        """
        cursor = self.conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        row_count = cursor.fetchone()[0]
        return row_count

    def close_connection(self):
        """
        Closes the connection to the Azure database.
        """
        self.conn.close()

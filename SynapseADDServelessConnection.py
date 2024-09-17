from azure.identity import AzureCliCredential
import struct
import pyodbc



class SynapseDB:

    def __init__(self, server, database, driver):
        self.server = server
        self.database = database
        self.driver = driver
        self.connect_to_db()

    def connect_to_db(self):
        credential = AzureCliCredential()
        databaseToken = credential.get_token('https://database.windows.net/')

        # get bytes from token obtained
        tokenb = bytes(databaseToken[0], "UTF-8")
        exptoken = b'';
        for i in tokenb:
            exptoken += bytes({i});
            exptoken += bytes(1);
            tokenstruct = struct.pack("=i", len(exptoken)) + exptoken;

        # build connection string using acquired token
        connString = "Driver={ODBC Driver "+str(self.driver)+" for SQL Server};SERVER="+self.server+";DATABASE="+self.database+""
        SQL_COPT_SS_ACCESS_TOKEN = 1256 
        conn = pyodbc.connect(connString, attrs_before = {SQL_COPT_SS_ACCESS_TOKEN:tokenstruct});
        self.conn = conn
      
    def close_connection(self):
        self.conn.close()

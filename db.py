from azure.identity import AzureCliCredential
import struct
import pyodbc 

def connect_to_db(server, database, driver):
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
    connString = "Driver={ODBC Driver "+str(driver)+" for SQL Server};SERVER="+server+";DATABASE="+database+""
    SQL_COPT_SS_ACCESS_TOKEN = 1256 
    conn = pyodbc.connect(connString, attrs_before = {SQL_COPT_SS_ACCESS_TOKEN:tokenstruct});
    return conn


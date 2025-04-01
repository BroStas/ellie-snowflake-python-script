import pyodbc
import struct
from itertools import chain, repeat
from azure.identity import InteractiveBrowserCredential
import os
from dotenv import load_dotenv

load_dotenv()

def main():
    """
    A working example code for authenticating with Azure browser integration.
    This will ask the user to login to Microsofdt Entra ID to get the token
    to connect with.
    """
    database_server_name = os.environ["DB_SERVER"]
    database_name = os.environ["DB_NAME"]

    try:
        credential = InteractiveBrowserCredential()

        SQL_COPT_SS_ACCESS_TOKEN = 1256  # This connection option is defined by Microsoft in msodbcsql.h

        token_bytes = credential.get_token("https://database.windows.net//.default").token.encode("UTF-16-LE")
        token_struct = { SQL_COPT_SS_ACCESS_TOKEN: struct.pack(f'<I{len(token_bytes)}s', len(token_bytes), token_bytes) }

        # token_object = credential.get_token("https://database.windows.net//.default") # Retrieve an access token valid to connect to SQL databases
        # token_as_bytes = bytes(token_object.token, "UTF-8") # Convert the token to a UTF-8 byte string
        # encoded_bytes = bytes(chain.from_iterable(zip(token_as_bytes, repeat(0)))) # Encode the bytes to a Windows byte string
        # token_bytes = struct.pack("<i", len(encoded_bytes)) + encoded_bytes # Package the token into a bytes object
        # token_struct = {SQL_COPT_SS_ACCESS_TOKEN: token_bytes}  # Attribute pointing to SQL_COPT_SS_ACCESS_TOKEN to pass access token to the driver

        conn_str = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={database_server_name};"
            f"DATABASE={database_name};"
            f"ENCRYPT=yes"
        )

        # See connection string options from documentation:
        # https://learn.microsoft.com/en-us/sql/connect/odbc/using-azure-active-directory?view=sql-server-ver16

        conn = pyodbc.connect(conn_str, attrs_before=token_struct)
        # conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        cursor.execute("SELECT 1 AS number")
        row = cursor.fetchone()
        print(f"Query result: {row[0]}")
        
    except Exception as e:
        print(f"Error: {e}")
        # print(f"")

if __name__ == "__main__":
    main()
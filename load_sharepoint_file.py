from shareplum import Site, Office365
from shareplum.site import Version
import pandas as pd
import io
from datetime import datetime
from sqlalchemy import create_engine, text
import urllib
import logging
from logging.handlers import RotatingFileHandler
import keyring #allows password retrieval from windows credential manager - use this module for password management
from msal import ConfidentialClientApplication
import requests



#functions
def get_token():
    # Values provided by IT when they create the app. The app is an object in Azure AD called XXX. 
    # the app needs Sites.Read.All and Sites.Selected 
    tenant_id = "XXXX"
    client_id = "XXXX" 

    client_secret = keyring.get_password("DBSPython.Service", client_id) #gets password for stored username. The arguments here are essential for retrieving the password and it can only be obtained by logging in as this user
    if not client_secret:
        logger.error("No client_secret found in Windows Credential Manager for SharePoint")
        raise RuntimeError("Missing SharePoint password")    

    authority = f"https://login.microsoftonline.com/{tenant_id}"
    scope = ["https://graph.microsoft.com/.default"]
    app = ConfidentialClientApplication(
        client_id,
        authority=authority,
        client_credential=client_secret
    )

    result = app.acquire_token_for_client(scopes=scope) #get the token

    if "access_token" in result:
        #print("Access token acquired")        
        token = result["access_token"]
    else:
        #print("Error:", result.get("error_description"))
        logger.exception("error acquiring access token")
        
    return token


def get_file(token):
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json;"
    }

    site_id = "shknhs.sharepoint.com,XXXX" #GET https://graph.microsoft.com/v1.0/sites/XXX.sharepoint.com:/sites/XXX
    drive_id = "XXXX" #GET https://graph.microsoft.com/v1.0/sites/{site_id}/drives 
    filepath = "KPI Reporting/Dashboard KPI Data.xlsx"    

    return requests.get(f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/root:/{filepath}:/content", headers=headers)



def setup_logger():
    # Set up rotating log handler directly
    logfile = 'Log.txt'

    logger = logging.getLogger("CommunityKPI_ETL")
    logger.setLevel(logging.INFO)

    handler = RotatingFileHandler(
        logfile,
        maxBytes=50000,
        backupCount=3
    )

    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.propagate = False  # Prevent messages from being passed to root logger
    return logger



def excel_to_df(file_content):
    # Load Excel file from SharePoint (already in memory as `file_content`)
    xls = pd.ExcelFile(io.BytesIO(file_content), engine='openpyxl')

    # Capture the start time of the process
    process_timestamp = datetime.now()

    # List to hold all DataFrames
    all_data = []

    for sheet_name in xls.sheet_names:
        try:
            df = pd.read_excel(xls, sheet_name=sheet_name, engine='openpyxl')

            # Skip empty sheets
            if df.empty:
                continue

            # Add metadata columns
            df['SheetName'] = sheet_name
            df['LoadTimestamp'] = process_timestamp

            all_data.append(df)

        except Exception as e:
            logger.exception(f"Error reading sheet {sheet_name}: {e}")


    # Combine all into one DataFrame
    return pd.concat(all_data, ignore_index=True)



def truncate_table(engine, table_name):
    with engine.begin() as conn:
        sqlcmd_truncate = text(f"TRUNCATE TABLE {table_name}")
        conn.execute(sqlcmd_truncate)



def execute_procedure(engine, procedure_name):
    with engine.begin() as conn:
        sqlcmd_load = text(f"exec {procedure_name}")
        conn.execute(sqlcmd_load)



#----Start---

#setup logging to auto manage files (creates x log files up to filesize y and then recycles as they fill up)
logger = setup_logger() 

#wrap all the work done in a try-catch block to record the error
try:
   
    logger.info("Starting ETL process")

    #get a token to get authorisation to get the file via API call. It contains who the token is for, what roles the application has and the expiry
    token = get_token()

    #get the file via ms graph using token above
    file_content = get_file(token).content
    
    #Combine excel sheets to a dataframe object
    combined_df = excel_to_df(file_content)

    #SQL connection details
    #################################################################
    server = 'Server'
    database = 'Database'

    params = urllib.parse.quote_plus(
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={server};"
        f"DATABASE={database};"
        "Trusted_Connection=yes;"
        "TrustServerCertificate=yes;"
    )

    connection_string = f"mssql+pyodbc:///?odbc_connect={params}"

    staging_table_name = 'Dashboard_KPI_Data_Staging'
    #################################################################

    #Create engine for SQL Server tasks
    engine = create_engine(connection_string)

    #truncate staging table
    truncate_table(engine, f"[dbo].[{staging_table_name}]")

    #Write df to staging table
    combined_df.to_sql(staging_table_name, con=engine, schema='dbo', if_exists='append', index=False)

    #Load data into main table
    execute_procedure(engine, 'dbo.sp_Dashboard_KPI_Data')

    logger.info("Finished ETL process")

except Exception as e:
    logger.exception("ETL process failed:")

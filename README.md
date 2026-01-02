# SharePoint to SQL Server ETL (Python)

## Overview

- This project automates the ingestion of Excel-based KPI data from SharePoint into SQL Server for downstream reporting.
- The solution replaces a manual extraction process with a scheduled, auditable Python ETL that integrates with Microsoft Graph, performs light transformation, and loads data into a SQL Server staging table before applying business logic via a stored procedure.

## High-Level Flow

- Authenticate to Microsoft Graph using Azure AD application credentials
- Download an Excel file from SharePoint via the Graph API
- Read and combine all worksheets into a single dataset
- Add load metadata (sheet name, load timestamp)
- Truncate and reload a SQL Server staging table
- Execute a stored procedure to populate reporting tables

## Key Technologies

- Python
- Microsoft Graph API
- MSAL (Azure AD authentication)
- pandas
- SQLAlchemy / pyodbc
- SQL Server
- Windows Credential Manager (via keyring)
- Rotating file logging

## Design Notes

- Credentials are retrieved securely from Windows Credential Manager; no secrets are stored in code
- Logging is implemented using rotating log files to support unattended execution
- Excel ingestion is performed in-memory (no intermediate files written to disk)
- SQL loading uses a staging table followed by a stored procedure to separate ingestion from business logic
- Identifiers, secrets, and business-specific details have been anonymised

## Intended Usage

- This script is designed to be run as part of a scheduled process (e.g. Task Scheduler or SQL Server Agent) to support regular reporting refreshes.

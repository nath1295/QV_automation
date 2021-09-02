# This python file is a sample of cred.py which contains all the credentials for the project
# For credentials

gs_api_key = "api key"
gs_api_secret = "api secret"
gs_log_sheet = 'https://docs.google.com/spreadsheets/d/sample_id1234'
gs_setup_sheet_id = 'sample_id_2'
gs_interactor = 'maintainer@email.com'

# ftp_credentials
import json
ftps = {
    "ftphost1": {
        "user":"user1",
        "port":22,
        "password":"pw1",
        "sftp":True
    },
    "ftphost2": {
        "user":"user2",
        "port":22,
        "password":"pw2",
        "sftp":True
    },
    "ftphost3": {
        "user": "user3",
        "port": 22,
        "password": "pw3",
        "sftp": True
    }
}

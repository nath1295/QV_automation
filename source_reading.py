import pandas as pd
import gspread
from oauth2client.client import OAuth2WebServerFlow
from oauth2client.file import Storage
from oauth2client.tools import run_flow
import httplib2
from cred import gs_api_key,gs_api_secret,gs_setup_sheet_id

# Start the OAuth flow to retrieve credentials
def authorize_credentials():
    SCOPE = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]
    STORAGE = Storage('credentials.storage')
# Fetch credentials from storage
    credentials = STORAGE.get()
# If the credentials doesn't exist in the storage location then run the flow
    if credentials is None or credentials.invalid:
        flow = OAuth2WebServerFlow(client_id=gs_api_key,
                                        client_secret=gs_api_secret,
                                        scope=SCOPE)
        http = httplib2.Http()
        credentials = run_flow(flow, STORAGE, http=http)
    return credentials

# Global variables
GGSS_ID = gs_setup_sheet_id
gc = gspread.authorize(authorize_credentials())
ss = gc.open_by_key(GGSS_ID)

# get all projects for a user
def get_projects(user):
    sheet = ss.worksheet('User Register')
    snames = ss.worksheets()
    snames = list(map(lambda x: x.title,snames))
    snames.remove('User Register')
    info = pd.DataFrame(sheet.get_all_records(), dtype=str)
    info = info[info['User']==user]

    pjs = list(info['Project'])
    if len(pjs)==0:
        return 'no project',300
    elif pjs[0] not in snames:
        return 'project does not exist',pjs[0]
    else:
        return pjs[0],info[info['Project']==pjs[0]]['tool interval (min)'].iloc[0],info[info['Project']==pjs[0]]['Project Log ggss'].iloc[0]

def tl_data(sheet_name):
    # get project sheet
    sheet = ss.worksheet(sheet_name)
    users = pd.read_csv('ftp_credentials.csv', index_col='host')['user'].unique()
    tool_data = pd.DataFrame(sheet.get_all_records(),dtype=str)
    tool_data = tool_data[tool_data['owner'].isin(users)]
    tool_data['download_to'] = tool_data['download_to'].apply(lambda x: x[:-1] if x[-1]=='\\' else x)
    tool_data['upload_from'] = tool_data['upload_from'].apply(lambda x: x[:-1] if x[-1]=='\\' else x)
    tool_data['download_from'] = tool_data['download_from'].apply(lambda x: x[:-1] if x[-1]=='/' else x)
    tool_data['upload_to'] = tool_data['upload_to'].apply(lambda x: x[:-1] if x[-1]=='/' else x)
    tool_data  = tool_data.fillna('')
    tool_data['day_shifting'] = tool_data['day_shifting'].apply(lambda x: '0' if x=='' else x)
    tool_data['download_dir_clear'] = tool_data['download_dir_clear'].apply(lambda x: 'Yes' if x=='' else x)
    tool_data['report_range'] = tool_data.apply(report_range, axis=1)
    return tool_data

def report_range(x):
    if (x['reporting_day_start']=='') or (x['reporting_day_end']==''):
        rpt_range = 'mon-sun'
    else:
        rpt_range = x['reporting_day_start']+'-'+x['reporting_day_end']
    return rpt_range



def out_num_check(x):
    err = []
    if len(x['upload_filename_pattern'].split('|'))!=int(x['upload_file_number']):
        err.append('"upload_file_number" and number of "upload_filename_pattern" don\'t match')
    if (x['output_filename']!='') and len(x['output_filename'].split('|'))!=int(x['upload_file_number']):
        err.append('"upload_file_number" and number of "output_filename" don\'t match')
    err = ','.join(err)
    return err

def tool_key(x):
    return x['tool_dir']+'\\'+x['tool_name']

def duplicates(df):
    tool_list = []
    dub_list = []
    check_list = df['tool_key'].values
    for tool in check_list:
        if tool not in tool_list:
            tool_list.append(tool)
        else:
            dub_list.append(tool)
    return dub_list

def check_dup(x,dup_list):
    err = x['errors'].split(',')
    if x['tool_key'] in dup_list:
        err.append('There are duplicate(s) of this tool in the spreadsheet')
    err = ','.join(err)
    if (len(err)!=0) and (err[0]==','):
        err=err[1:]
    return err

def check(tool_data):
    checker = tool_data.copy()
    checker['errors'] = checker.apply(out_num_check,axis=1)
    checker['tool_key'] = checker.apply(tool_key,axis=1)
    duplicate = duplicates(checker)
    checker['errors'] = checker.apply(lambda x: check_dup(x,duplicate),axis=1)
    checker = checker[checker['errors']!='']
    checker = checker[['source_name','tool_name','errors']]
    checker['errors'] = checker.apply(lambda x: f"{x['source_name']} [{x['tool_name']}]: {x['errors']}",axis=1)
    return checker



if __name__=='__main__':
    tool_data = tl_data('Lenovo West Retail')
    proj = get_projects('ntam')
    print('done')


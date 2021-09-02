from source_reading import authorize_credentials
import gspread
import pandas as pd
import math
from datetime import datetime as dt
from cred import gs_log_sheet, gs_interactor



class GsLogger:
    def __init__(self):
        self.__ggss = gs_log_sheet
        self.__gc = gspread.authorize(authorize_credentials())
        self.__ss = self.__gc.open_by_url(self.__ggss)
        self.__log = self.__ss.worksheet('Log')
        self.__time = self.__ss.worksheet('Time')

    def log(self,log_info):
        #  Input Path, Input File, mod timestamp, Output File, FTP, Tool, Time, Status, Project, run status, runtime id
        keys = ["path","infile","modtime","outfile","ftp","tool","Time","status","pj","run_status","run_id"]
        log_info = list(map(lambda x: [x[k] for k in keys],log_info))
        self.__log.append_rows(log_info)

    def log_refresh(self,resfresh_info):
        self.__time.append_rows(resfresh_info,value_input_option='RAW')

    def last_refresh(self,project,ftp_host):
        # '12/04/2021 10:50:37'
        data = pd.DataFrame(self.__time.get_all_records())
        data = data[(data['ftp_host']==ftp_host) & (data['Project']==project)]
        data = data[data['mode']=='live']
        time = data['timestamp'].max()
        if math.isnan(time):
            time  = dt.today().strftime('%Y%m%d')
            time  = dt.strptime(time,'%Y%m%d').timestamp()
        return time

    def get_history(self,project):
        data = self.__log.get_all_records()
        data = pd.DataFrame(data)
        data = data[data['Project']==project]
        data = data[data['Run Status']=='live']
        data['mod_timestamp'] = data['mod_timestamp'].astype(int)
        if len(data)!=0:
            data['filekey'] = data.apply(lambda x: f"{x['Input Path']}/{x['Input File']}{x['mod_timestamp']}",axis=1)
            return data['filekey'].unique()
        else:
            return []

    # ['file', 'time', 'type', 'path']
    def newfiles(self,data,project):
        history = self.get_history(project)
        if len(data)!=0:
            data['filekey'] = data.apply(lambda x: f"{x['path']}/{x['file']}{int(x['time'].timestamp())}",axis=1)
            data['del'] = data['filekey'].apply(lambda x: 'yes' if x in history else 'no')
            data = data[data['del']=='no']
        data = data[['file', 'time', 'type', 'path']]
        return data

    def create_private_log(self,ggss_url):
        try:
            cc = self.__gc.open_by_url(ggss_url)
            sheets = cc.worksheets()
            sheets = list(map(lambda x: x.title,sheets))
            if 'QV automator log' not in sheets:
                cc.add_worksheet('QV automator log',1,8)
                header = ["Input Path,Input File,mod_timestamp,Output File,FTP,Tool,Time,Status,Run Status,Runtime ID".split(',')]
                private = cc.worksheet('QV automator log')
                private.insert_rows(header)
            else:
                private = cc.worksheet('QV automator log')
            self.__private = private
        except:
            print(f'Please add "{gs_interactor}" as an editor in the log viewing google spreadsheet [{ggss_url}].')
            self.__private = 'NA'

    def private_log(self,log_info):
        if self.__private!='NA':
            keys = ["path", "infile", "modtime", "outfile", "ftp", "tool", "Time", "status", "run_status","run_id"]
            log_info = list(map(lambda x: [x[k] for k in keys], log_info))
            self.__private.insert_rows(log_info,2)

    def log_others(self,file_data):
        # [df,ftp,user]
        dfs = []
        for inst in file_data:
            inst[0]['ftp'] = inst[1]
            inst[0]['user'] = inst[2]
            dfs.append(inst[0])
        final = pd.concat(dfs,ignore_index=True)
        final.drop_duplicates(inplace=True,ignore_index=True)
        compare = pd.DataFrame(self.__private.get_all_records())
        if len(final)!=0:
            final['delete'] = final.apply(lambda x: 'y' if x['file'] in compare['Output File'].values else 'n',axis=1)
            final = final[final['delete']=='n']
            final['file'] = final.apply(lambda x: x['path']+'/'+x['file'],axis=1)
            now = dt.today().strftime('%d/%m/%Y %H:%M:%S')
            final['log'] = final.apply(lambda x: [x['file'],str(int(x['time'].timestamp())),'',
                                                  x['ftp'],'',now,'not processed',x['user']],axis=1)
            logs = final['log'].values.tolist()
            self.__private.insert_rows(logs,2)




import os
import subprocess
import time
import warnings
from datetime import datetime as dt, timedelta as delt
from pathlib import Path
from zipfile import ZipFile
import pandas as pd
import shutil

# Project modules
from central_log import GsLogger
from filename_pattern import DatedFile
from ftp import Ftp
from source_reading import tl_data, check, get_projects,project_info
from cred import ftps
from script_compile import run_script

warnings.filterwarnings('ignore')

def f_rename(src,dst):
    try:
        os.rename(src, dst)
    except:
        os.remove(dst)
        os.rename(src, dst)


class ToolService:
    '''Manager of a tool in a project'''
    def __init__(self,project,tool,newfiles,last_refresh,runtime_id):
        self.__project = project
        self.__tool = tool
        self.__newfiles = newfiles
        self.__gslogger = GsLogger()
        self.unprocessed,self.loaded_log = self.__run_flow(last_refresh,runtime_id)

    def __run_flow(self,last_refresh,runtime_id):
        task = self.__tool
        # Search and download from ftp
        pattern = task['download_filename']
        host = task['ftp_host']
        file_format = pattern.split('.')[-1]
        dayshift = task['day_shifting']
        report_range = task['report_range']
        self.__ftp = Ftp(host,ftps[host]['port'],ftps[host]['sftp'],ftps[host]['user'],ftps[host]['password'])
        self.__ftp.chdir(task['download_from'])
        scan = self.__newfiles[self.__newfiles['path'] == task['download_from']]
        scan['filedate_obj'] = scan['file'].apply(lambda x: DatedFile(x, pattern, dayshift, report_range))
        scan['filedate'] = scan['filedate_obj'].apply(lambda x: x.file_date)
        scan = scan[scan['filedate'] != 'Filename/pattern not matching']
        scan = scan.sort_values(['time'], ascending=True, ignore_index=True)
        logdict = {'ftp': task['ftp_host'], 'tool': f'{task["tool_dir"]}\\{task["tool_name"]}'}
        logdict['pj'] = self.__project
        logdict['tool'] = task['tool_dir']+'\\'+task['tool_name']
        logdict['run_status'] = last_refresh
        run_confirm = True
        try:
            self.__create_temp_folder(task['download_to'],file_format,task['download_dir_clear'])
        except Exception as e:
            print(e)
            run_confirm = False

        logs = []
        if run_confirm:
            for i in range(len(scan)):
                row = scan.iloc[i]
                try:
                    logs.append(self.__process_file(row,task,logdict,last_refresh,runtime_id))
                except Exception as e:
                    print(e)
            self.__delete_temp_folder(task['download_to'],task['download_dir_clear'])

        self.__gslogger.log(logs)
        processed_files = list(map(lambda x: f"{x['path']}/{x['infile']}{x['modtime']}",logs))
        return processed_files,logs

    def __process_file(self,scan,task,logdict,last_refresh,runtime_id):
        file = scan['file']
        log = logdict
        # download the file
        self.__ftp.chdir(task['download_from'])
        self.__ftp.download(file,task['download_to'])
        # run transformation tool
        if task['tool_type']=='QV':
            # For QV
            with open('temp.txt', 'w') as temp:
                temp.write(f"{task['tool_name']},{task['tool_dir']}")
            run_time = dt.now()
            subprocess.call(['dist\\qlikview.exe'])
            with open('return.txt', 'r') as temp:
                success = temp.read()
            if success=='t':
                success=True
            else:
                success=False
            os.remove('return.txt')
            os.remove('temp.txt')
        elif task['tool_type']=='Python':
            run_time = dt.now()
            try:
                success = run_script(task['tool_name'],task['tool_dir'])
            except:
                success = False
        elif task['tool_type']=='Renaming':
            run_time = dt.now()
            time.sleep(0.5)
            success = True
            try:
                new_file = scan['filedate_obj'].str_date(task['upload_filename_pattern'])
                print(f'Renaming file "{file}" to "{new_file}"')
                shutil.copyfile(f"{task['download_to']}\\{file}",f"{task['upload_from']}\\{new_file}")
            except:
                success = False

        else:
            # For other transformation method under development
            run_time = dt.now()
            success=False
        log['infile'] = file
        log['path'] = task['download_from']
        log['modtime'] = str(int(scan['time'].timestamp()))
        log['Time'] = run_time.strftime("%d/%m/%Y %H:%M:%S")
        log['run_id'] = str(runtime_id)
        # backup the file if neccessary and clean the local directory
        if task['download_file_backup']:
            Path(task['download_to'] + '\\AutoQV_backup').mkdir(parents=True, exist_ok=True)
            zipname = '.'.join(file.split('.')[:-1])
            with ZipFile(f"{task['download_to']}\\AutoQV_backup\\{zipname}_{dt.today().strftime('%Y%m%d%H%M%S')}.zip",
                         'w') as zip:
                zip.write(f"{task['download_to']}\\{file}", file)
        if task['download_dir_clear']=='Yes':
            os.remove(f"{task['download_to']}\\{file}")
        # rename output file if neccesary
        if success:
            transformed_files = os.listdir(task['upload_from'])
            transformed_files = list(map(lambda x: [x,
                                                    dt.fromtimestamp(
                                                        Path(f"{task['upload_from']}\\{x}").stat().st_mtime)],
                                         transformed_files))
            transformed_files = pd.DataFrame(transformed_files, columns=['file', 'mod_time'])
            transformed_files = transformed_files[transformed_files['mod_time'] > run_time]
            transformed_files = list(transformed_files['file'].values)
            rename_patterns = task['upload_filename_pattern'].split('|')
            # rename output file if neccesary
            if task['output_filename']!='':
                static_names = task['output_filename'].split('|')
                if set(transformed_files)==set(static_names):
                    renamed_files = list(map(lambda x: scan['filedate_obj'].str_date(x),rename_patterns))
                    upload_files = []
                    for i in range(len(static_names)):
                        try:
                            f_rename(f"{task['upload_from']}\\{static_names[i]}",f"{task['upload_from']}\\{renamed_files[i]}")
                            upload_files.append(renamed_files[i])
                        except:
                            print(f'failed renaming "{static_names[i]}" to "{renamed_files[i]}"')
                    transformed_files = upload_files.copy()
                else:
                    upload_files = []
            # upload straigtaway
            else:
                if scan['filedate_obj'].file_date.strftime('%Y%m%d')=='19700101':
                    upload_files = []
                    for file in transformed_files:
                        for pattern in rename_patterns:
                            if (DatedFile(file,pattern).file_date!='Filename/pattern not matching') & (file not in upload_files):
                                upload_files.append(file)
                else:
                    # upload_files = list(map(lambda x: scan['filedate_obj'].str_date(x), rename_patterns))
                    upload_files = []
                    for file in transformed_files:
                        for pattern in rename_patterns:
                            if (scan['filedate_obj'].output_matching(file,pattern)) & (file not in upload_files):
                                upload_files.append(file)
            # Actual uploading
            self.__ftp.chdir(task['upload_to'])
            failed_files = []
            for file in upload_files:
                try:
                    if file not in transformed_files:
                        raise RuntimeError(f'"{file}" not in the transformed list')
                    if last_refresh=='live':
                        if task['status']=='live':
                            self.__ftp.upload(file,task['upload_from'])
                except:
                    failed_files.append(file)

            if ((len(upload_files)==0)|(len(failed_files)!=0)):
                log['status'] = 'success/ file(s) not uploaded'
            elif task['status']=='live/no upload':
                log['status'] = 'success/ no upload'
            else:
                log['status'] = 'success'
            log['outfile'] = '|'.join(transformed_files)
        else:
            log['status'] = 'fail'
            log['outfile'] = ''
        # for k in log.keys():
        #     print(f'{k}: {log[k]}')
        return log.copy()


    def __create_temp_folder(self,path,file_format,clear_path):
        inv = os.listdir(path)
        inv = list(filter(lambda x: x.split('.')[-1] == file_format, inv))
        if clear_path == 'Yes':
            Path(path + '\\temp_for_qv_trans').mkdir(parents=True, exist_ok=True)
            for file in inv:
                try:
                    os.rename(f"{path}\\{file}", f"{path}\\temp_for_qv_trans\\{file}")
                except:
                    raise RuntimeError(f'{file} already exist in temporary folder.')

    def __delete_temp_folder(self,path,clear_path):
        if clear_path == 'Yes':
            inv = os.listdir(path+'\\temp_for_qv_trans')
            for file in inv:
                try:
                    os.rename(f"{path}\\temp_for_qv_trans\\{file}",f"{path}\\{file}")
                except:
                    print(f'{file} already copied.')
            shutil.rmtree(f"{path}\\temp_for_qv_trans")



class ProjectManager:
    '''Manage a project'''
    def __init__(self,project):
        self.__project = project
        self.runtime_id = self.get_rumtime_id()
        self.__tools = tl_data(project)
        checker = check(self.__tools)
        if len(checker) != 0:
            raise RuntimeError('please correct the setting spreadsheet before running the QV automator.')
        if len(self.__tools) == 0:
            raise RuntimeError(f'There are no active tools for [{project}] on QV automator.')
        self.__interval,self.__privategs = project_info(self.__project)

        self.__gslog = GsLogger()
        try:
            if self.__privategs != '':
                self.__gslog.create_private_log(self.__privategs)
                self.__private_log = True
            else:
                self.__private_log = False
        except:
            raise RuntimeError(
                'There is an issue with the log ggss link you provided, please check [Project register] tab in the set up')


    def tools_manage(self,last_refresh='live'):
        runtime_id = self.runtime_id
        # find new files
        scans = []
        time_log_list = list()
        file_log_list = list()
        for host in self.__tools['ftp_host'].unique():
            ftp = Ftp(host,ftps[host]['port'],ftps[host]['sftp'],ftps[host]['user'],ftps[host]['password'])
            tools = self.__tools[self.__tools
                                 ['ftp_host']==host]
            paths = tools['download_from'].unique()
            if last_refresh=='live':
                last_op = self.__gslog.last_refresh(self.__project,host)
                last_op = dt.fromtimestamp(last_op)
                print(f'last running time for [{self.__project}] [{host}]: {last_op}')
            else:
                default_day = dt.today() - delt(days=7)
                last_op = input(f'Files start from [{default_day.strftime("%Y-%m-%d")}]:')
                try:
                    last_op = dt.strptime(last_op,"%Y-%m-%d")
                except:
                    last_op = default_day
            newfiles = list()
            c_time = dt.now()
            new_time_log = [host,self.__project,c_time.strftime('%d/%m/%Y %H:%M:%S'),str(c_time.timestamp()),last_refresh]
            # self.__gslog.log_refresh(new_time_log,'live')
            for path in paths:
                ftp.chdir(path)
                all_files = pd.DataFrame(ftp.list_dir(), columns=['file', 'time', 'type'])
                all_files = all_files[all_files['type'] == '-']
                all_files = all_files[all_files['time'] >= last_op]
                all_files['path'] = path
                newfiles.append(all_files)
            ftp.close()
            newfiles = pd.concat(newfiles)
            newfiles = newfiles.sort_values(by=['time'])
            if last_refresh=='live':
                newfiles = self.__gslog.newfiles(newfiles,self.__project)


            tool_logs = list()
            proced = list()
            for row in range(len(tools)):
                tool = tools.iloc[row]
                toolflow = ToolService(self.__project,tool,newfiles,last_refresh,runtime_id)
                tool_logs = tool_logs + toolflow.loaded_log
                proced = proced + toolflow.unprocessed
            time_log_list.append(new_time_log)
            file_log_list = file_log_list + tool_logs.copy()
            details = newfiles
            details['pj'] = self.__project
            details['run_status'] = last_refresh
            details['ftp'] = host
            details['modtime'] = details['time'].apply(lambda x: str(int(x.timestamp())))
            details['run_id'] = str(runtime_id)
            scans.append(details)
        self.__gslog.log_refresh(time_log_list)
        scans = pd.concat(scans)
        private_logs = file_log_list.copy()
        pro_files = list(map(lambda x: f'{x["infile"]}{x["modtime"]}{x["path"]}{x["ftp"]}',private_logs))
        pro_files = list(set(pro_files))
        if len(scans)!=0:
            scans['keys'] = scans.apply(lambda y: f"{y['path']}/{y['file']}{int(y['time'].timestamp())}",axis=1)
            scans['processed'] = scans['keys'].apply(lambda x: x in proced)
            unpro = scans[scans['processed']==False]
            if len(unpro)!=0:
                unpro['Time'] = dt.now().strftime('%d/%m/%Y %H:%M:%S')
                unpro['log'] = unpro.apply(file_log_creation,axis=1)
                unpro = list(unpro['log'].values)
                self.__gslog.log(unpro)
                private_logs = private_logs + unpro
        if self.__private_log:
            self.__gslog.private_log(private_logs)
        print(f"{self.__project}: {len(scans)} new files; {len(pro_files)} files attempted proccessing.")
        return scans

    def get_rumtime_id(self):
        project = self.__project
        df = pd.read_parquet('runtime_summary.parquet')
        max = df['runtime_id'].max()
        id = max + 1
        new = pd.DataFrame([[id, project]], columns=['runtime_id', 'project'])
        df = df.append(new, ignore_index=True)
        df.to_parquet('runtime_summary.parquet')
        return id

def file_log_creation(x):
    log = dict()
    structured = ["path","modtime", "ftp", "Time", "pj", "run_status","run_id"]
    for i in structured:
        log[i] = x[i]
    log['infile'] = x['file']
    log['status'] = 'not proccessed'
    log['outfile'] = ''
    log['tool'] = ''
    return log.copy()

class Scheduler:
    '''Schedule projects run time'''
    def __init__(self):
        self.__gslog = GsLogger()
        self.__projects = get_projects()
        self.__pj_info = pd.DataFrame(list(map(lambda x: [x]+list(project_info(x)),self.__projects)),
                                      columns = ['project','interval','link'])
        self.__start_ref = '060000' # hour (a.m.)
        self.create()
        while True:
            self.__sleeper()

    def create(self):
        if self.__projects!=get_projects():
            self.__projects = get_projects()
            self.__pj_info = pd.DataFrame(list(map(lambda x: [x] + list(project_info(x)), self.__projects)),
                                          columns=['project', 'interval', 'link'])
        ref = dt.now().strftime(f'%Y%m%d{self.__start_ref}')
        ref = dt.strptime(ref,'%Y%m%d%H%M%S')
        full_list = []
        for pj in self.__projects:
            interval = int(self.__pj_info[self.__pj_info['project']==pj]['interval'].iloc[0])
            timetable = []
            count = 0
            while count<1440:
                count+=interval
                timetable.append(count)
            timetable = list(map(lambda x:[pj,(ref+delt(minutes=x))],timetable))
            full_list = full_list+timetable
        full_list = pd.DataFrame(full_list,columns=['project','timestamp'])
        now = dt.now() + delt(minutes=1)
        full_list = full_list[full_list['timestamp'] > now]
        full_list['timestamp'] = full_list['timestamp'].apply(lambda x: x.timestamp())
        full_list = full_list.sort_values(['timestamp'],ascending=True,ignore_index=True)
        full_list.to_csv('schedule.csv',index=False)
        try:
            full_list.to_csv('schedule_copy.csv', index=False)
        except:
            pass

    def __sleeper(self):
        schd = pd.read_csv('schedule.csv')
        if len(schd)==0:
            self.create()
            schd = pd.read_csv('schedule.csv')
        next = schd.iloc[0]
        pj = next['project']
        time_dif = next['timestamp']-dt.now().timestamp()
        if time_dif<=0:
            print(f'next project [{pj}] running in 0 seconds.')
        else:
            print(f'next project [{pj}] running in {time_dif} seconds.')
            time.sleep(time_dif)
        run_status = 'live'
        try:
            start = time.perf_counter()
            project = ProjectManager(pj)
            files = project.tools_manage(last_refresh=run_status)
            end = time.perf_counter()-start
            print(f'Time taken: {round(end,2)} seconds')
        except Exception as e:
            import traceback
            print(f'[{pj}] failed.')
            error = traceback.format_exc()
            Path('errors').mkdir(parents=True, exist_ok=True)
            now = dt.today()
            name = pj.replace(' ','_')
            err_ts = now.strftime(f"{name}_%Y%m%d%H%M%S_err.txt")
            with open(f"errors\\{err_ts}", 'w') as err_log:
                err_log.write(error)
            print(error)
            run_id = pd.read_parquet('runtime_summary.parquet')
            run_id  = run_id[run_id['project']==pj]
            run_id = run_id['runtime_id'].max()
            err_log = {
            "path":'root\\error',
            "infile": err_ts,
            "modtime":str(int(now.timestamp())),
            "outfile":err_ts,
            "ftp":str(e),
            "tool":"error",
            "Time":now.strftime("%d/%m/%Y %H:%M:%S"),
            "status":"QVAUTO ERROR",
            "pj": pj,
            "run_status": run_status,
            "run_id":str(run_id)
            }
            self.__gslog.log([err_log])
        print(f'[{pj}] done. {dt.now()}')
        schd = schd.iloc[1:]
        schd.to_csv('schedule.csv',index=False)
        try:
            schd.to_csv('schedule_copy.csv', index=False)
        except:
            pass

if __name__=='__main__':
    project = ProjectManager('Sample project')
    files = project.tools_manage(last_refresh='test')

    print('done')
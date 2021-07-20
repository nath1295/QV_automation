import os
import subprocess
import time
import warnings
from datetime import datetime as dt
from pathlib import Path
from zipfile import ZipFile
import logging
import pandas as pd

from central_log import GsLogger
from filename_pattern import DatedFile
from ftp import Ftp, ftp_config, get_ftp_user
from source_reading import tl_data, check, get_projects

warnings.filterwarnings('ignore')


def main(project,logging_gs):
    # Initialising
    tool_data = tl_data(project)
    cuser = tool_data['owner'].unique()
    checker = check(tool_data)
    if len(checker)!=0:
        for err in checker['errors']:
            print(err)
        print('please correct the setting spreadsheet before running the QV automator.')
        time.sleep(300)
        exit()
    if len(tool_data)==0:
        print(f'You are not the owner of any tools for [{project}] on QV automator.')
        time.sleep(300)
        exit()
    if len(cuser)!=1:
        print(f'You have more than one user registered on "ftp_credentials.csv". Please check the credentials again.')
        time.sleep(300)
        exit()
    else:
        cuser = cuser[0]
    gslog = GsLogger()
    try:
        if logging_gs!='':
            gslog.create_private_log(logging_gs)
            logging_gs = True
        else:
            logging_gs = False
    except:
        print('There is an issue with the log ggss link you provided, please check [User register] tab in the set up')
        time.sleep(300)
        exit()
    # Last operation scanning time
    last_op = gslog.last_refresh(project,cuser)
    last_op = dt.fromtimestamp(last_op)


    logging.basicConfig(format='%(levelname)s: %(message)s',filename='QVauto.log',filemode='a',level=logging.DEBUG)
    print('\nNew QV automator cycle started. Please don\'t exit QV automator until the current cycle is completed')

    # All available files for current scan
    scan_paths = tool_data[['ftp_host','download_from']].values
    files = []
    c_time = dt.today()
    rfslog = [tool_data['owner'].iloc[0],project,c_time.strftime('%d/%m/%Y %H:%M:%S'),str(c_time.timestamp())]
    gslog.log_refresh(rfslog)
    irre_files = []
    for path in scan_paths:
        ftp = Ftp(*ftp_config(path[0]))
        ftp.chdir(path[1])
        all_files = pd.DataFrame(ftp.list_dir(), columns=['file', 'time', 'type'])
        ftp.close()
        all_files = all_files[all_files['type'] == '-']
        all_files = all_files[all_files['time'] >= last_op]
        all_files['path'] = path[1]
        files.append(all_files)
    files = pd.concat(files)
    files = files.drop_duplicates()
    files = files.sort_values(by=['time'])
    files = gslog.newfiles(files,project)


    # loop though tools one by one
    for row in range(len(tool_data)):
        task = tool_data.iloc[row]
        # Search and download from ftp
        pattern = task['download_filename']
        file_format = pattern.split('.')[-1]
        dayshift = task['day_shifting']
        report_range = task['report_range']
        ftp = Ftp(*ftp_config(task['ftp_host']))
        ftp.chdir(task['download_from'])
        scan = files[files['path']==task['download_from']]
        scan['filedate_obj'] = scan['file'].apply(lambda x: DatedFile(x,pattern,dayshift,report_range))
        scan['filedate'] = scan['filedate_obj'].apply(lambda x: x.file_date)
        irre_files.append([scan[scan['filedate']=='Filename/pattern not matching'],task['ftp_host'],user])
        scan = scan[scan['filedate']!='Filename/pattern not matching']
        scan = scan.sort_values(['time'],ascending=True,ignore_index=True)
        logdict = {'ftp':task['ftp_host'],'tool':f'{task["tool_dir"]}\\{task["tool_name"]}'}
        logdict['pj'] = project
        logdict['user'] = cuser

        #moving files to temporary folder
        inv = os.listdir(task['download_to'])
        inv = list(filter(lambda x: x.split('.')[-1]==file_format,inv))
        if task['download_dir_clear']=='Yes':
            Path(task['download_to'] + '\\temp_for_qv_trans').mkdir(parents=True, exist_ok=True)
            for file in inv:
                os.rename(f"{task['download_to']}\\{file}",f"{task['download_to']}\\temp_for_qv_trans\\{file}")

        # loop through downloaded files one by one
        for ind in scan.index:
            file = scan.loc[ind,'file']
            logdict['infile'] = f'{task["download_from"]}/{file}'
            logdict['modts'] = str(int(scan.loc[ind,'time'].timestamp()))
            ftp.download(file,task['download_to'])
            logging.info('')
            logging.info(f'<{file}> downloaded to <{task["download_to"]}> at {dt.today().strftime("%Y%m%d %H:%M:%S")}')

            # Run QV
            print(f'transformation for {file} started')
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
            if success:
                current_ts = dt.today()
                logging.info(
                    f'<{file}> transformed successfully at {current_ts.strftime("%Y%m%d %H:%M:%S")}')
                logdict['status'] = 'success'
                logdict['time'] = current_ts.strftime("%d/%m/%Y %H:%M:%S")
            else:
                current_ts = dt.today()
                logging.error(
                    f'<{file}> transformation failed at {current_ts.strftime("%Y%m%d %H:%M:%S")}')
                logdict['status'] = 'fail'
                logdict['time'] = current_ts.strftime("%d/%m/%Y %H:%M:%S")


            # backup downloaded file
            Path(task['download_to']+'\\AutoQV_backup').mkdir(parents=True,exist_ok=True)
            zipname = '.'.join(file.split('.')[:-1])
            with ZipFile(f"{task['download_to']}\\AutoQV_backup\\{zipname}_{dt.today().strftime('%Y%m%d%H%M%S')}.zip",'w') as zip:
                zip.write(f"{task['download_to']}\\{file}",file)
            os.remove(f"{task['download_to']}\\{file}")
            logging.info(f'<{file}> compressed and moved to <{task["download_to"]}\\AutoQV_backup> at {dt.today().strftime("%Y%m%d %H:%M:%S")}')



            # file renaming and upload file
            if success:
                transformed_files = os.listdir(task['upload_from'])
                transformed_files = list(map(lambda x: [x,
                                                        dt.fromtimestamp(Path(f"{task['upload_from']}\\{x}").stat().st_mtime)],
                                             transformed_files))
                transformed_files = pd.DataFrame(transformed_files,columns=['file','mod_time'])
                transformed_files = transformed_files[transformed_files['mod_time']>run_time]
                transformed_files = transformed_files['file'].values

                if task['output_filename']=='':
                    output_file_names = task['upload_filename_pattern'].split('|')
                    output_file_names = list(
                        map(lambda x: scan.loc[ind, 'filedate_obj'].str_date(x), output_file_names))
                    output_file_names = list(filter(lambda x: x in transformed_files, output_file_names))
                    for file in transformed_files:
                        patterns = task['upload_filename_pattern'].split('|')
                        for patt in patterns:
                            if DatedFile(file,patt).file_date!='Filename/pattern not matching':
                                if file not in output_file_names:
                                    output_file_names.append(file)
                else:
                    output_file_names = task['upload_filename_pattern'].split('|')
                    output_file_names = list(map(lambda x: scan.loc[ind,'filedate_obj'].str_date(x),output_file_names))
                    if (task['output_filename']!='') or (task['output_filename']!=task['upload_filename_pattern']):
                        for i,out in enumerate(task['output_filename'].split('|')):
                            os.rename(f"{task['upload_from']}\\{out}",output_file_names[i])
                ftp.chdir(task['upload_to'])
                if  len(output_file_names)==0:
                    print(
                        'output file(s) not found, please check your set up for output filename and dayshifting configuration.')
                    logging.info(
                        f'Not enough files are uploaded. {dt.today().strftime("%Y%m%d %H:%M:%S")}')
                    logdict['status'] = 'success/ file(s) not uploaded'
                else:
                    for upload_file_name in output_file_names:
                        try:
                            file_check = Path(f"{task['upload_from']}\\{upload_file_name}")
                            if dt.fromtimestamp(file_check.stat().st_mtime)>run_time:
                                ftp.upload(upload_file_name,task['upload_from'])
                                logging.info(f'<{upload_file_name}> uploaded to <{task["upload_to"]}> at {dt.today().strftime("%Y%m%d %H:%M:%S")}')
                            else:
                                print(
                                    'output file(s) not found, please check your set up for output filename and dayshifting configuration.')
                                logging.info(
                                    f'<{upload_file_name}> not found in QV tool output folder, nothing is uploaded. {dt.today().strftime("%Y%m%d %H:%M:%S")}')
                                logdict['status'] = 'success/ file(s) not uploaded'
                            if len(output_file_names)<len(task['upload_filename_pattern'].split('|')):
                                logdict['status'] = 'success/ file(s) not uploaded'
                        except:
                            print('output file(s) not found, please check your set up for output filename and dayshifting configuration.')
                            logging.info(
                                f'<{upload_file_name}> not found in QV tool output folder, nothing is uploaded. {dt.today().strftime("%Y%m%d %H:%M:%S")}')
                            logdict['status'] = 'success/ file(s) not uploaded'
                logdict['outfile'] = '|'.join(output_file_names)
            else:
                logging.error(f'{file} transformation failed. Nothing uploaded. {dt.today().strftime("%Y%m%d %H:%M:%S")}')
                logdict['outfile'] = 'No output files'

            # log to central logger
            loglist=[]
            for key in 'infile,modts,outfile,ftp,tool,time,status,pj,user'.split(','):
                loglist.append(logdict[key])
            gslog.log(loglist)
            if logging_gs:
                gslog.private_log(loglist)



        # cleanup
        if task['download_dir_clear'] == 'Yes':
            for file in inv:
                os.rename(f"{task['download_to']}\\temp_for_qv_trans\\{file}",f"{task['download_to']}\\{file}")
            Path(f"{task['download_to']}\\temp_for_qv_trans").rmdir()
    if logging_gs:
        gslog.log_others(irre_files)
    print('Cycle completed. You can leave anytime now.')

if __name__=='__main__':
    user = get_ftp_user()
    project,interval,ggss = get_projects(user)
    if project=='no project':
        print('No project is registered under your user name.')
        time.sleep(300)
        exit()
    elif project=='project does not exist':
        print(f'Project [{project}] doesn\'t exist.')
        time.sleep(300)
        exit()
    else:
        print(f'QV autmator is working on {project} for {user} every {interval} minutes')
        while True:
            try:
                main(project,ggss)
                time.sleep(int(interval)*60)
            except Exception as e:
                import traceback
                error = traceback.format_exc()
                Path('errors').mkdir(parents=True,exist_ok=True)
                now = dt.today()
                err_ts = now.strftime("%Y%m%d%H%M%S_err.txt")
                with open(f"errors\\{err_ts}",'w') as err_log:
                    err_log.write(error)
                print(error)
                gs_log = GsLogger()
                err_log = [f'errors\\{err_ts}',str(int(now.timestamp())),err_ts,str(e),'error',
                           now.strftime("%d/%m/%Y %H:%M:%S"),"QVAUTO ERROR",project,user]
                gs_log.log(err_log)
                time.sleep(300)
                exit()



    print('Done')
import pandas as pd
from ftp import Ftp
from cred import ftps
import time

if __name__=="__main__":
    host_dict = {
        '1': "ftp.contextworld.com",
        '2': "ftp2.contextworld.com",
        '3': "ftp3.contextworld.com",
        '4': "ftp4.contextworld.com",
        '5': "ftp5.contextworld.com",
    }
    print("Available FTP host: ")
    for key in host_dict.keys():
        print(f"{key}: {host_dict[key]}")

    host_pass = True
    while host_pass:
        try:
            host = input('Please insert your FTP host (e.g. "1" for ftp.contextworld.com): ')
            ftp = Ftp(host_dict[host], **ftps[host_dict[host]])
            print(f"\nYou are in [{host_dict[host]}]")
            host_pass = False
        except:
            print(f'Please insert a number from {list(host_dict.keys())}')

    path_pass = True
    while path_pass:
        path = input('\nPlease insert your FTP path: ')
        org = path
        org = org.replace('/Noneu','/Non-EU/')
        ftp.chdir(org)
        if ftp.dir!='/':
            files = ftp.list_dir()
            files = pd.DataFrame(files,columns = ['Filename','Mod_time','File_type'])
            files = files[files['File_type']=='-']
            files = files.sort_values('Mod_time',axis=0,ascending=False,ignore_index=True).iloc[:10,:2]
            print(f'files in directory "{ftp.dir}":')
            print(files)
        else:
            print(f'Invalid path :{path}')
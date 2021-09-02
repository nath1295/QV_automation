import ftplib
import ftputil
import ftputil.session as session
import paramiko
import os

from datetime import datetime as dt
import pandas as pd


def get_ftp_user():
    df = pd.read_csv('ftp_credentials.csv', index_col='host')
    user = df['user'].unique()
    if len(user)>1:
        return 'nil'
    elif len(user)==0:
        return 'zero'
    else:
        return user[0]


def ftp_config(host):
    df = pd.read_csv('ftp_credentials.csv', index_col='host')
    ftp = list(df.loc[host])
    ftp = [host]+ftp
    ftp[1] = int(ftp[1])
    return ftp


class Ftp:
    '''
    FTP object which can perform basic ftp client operations
    '''

    def __init__(self,host, port,sftp,user,password):
        '''
        :param host: str
                FTP host
        :param port: int
                port for the FTP
        :param sftp: bool
                whether the FTP is an SFTP or a traditional FTP, True for SFTP
        '''
        self.sftp = sftp
        self.host = host
        try:
            if self.sftp:
                trans = paramiko.Transport((host,port))
                trans.connect(username=user,password=password)
                self.conn = paramiko.SFTPClient.from_transport(trans)
            else:
                self.conn = ftputil.FTPHost(host,user,password,session_factory = session.session_factory(base_class=ftplib.FTP,
                    port=port,
                    use_passive_mode=None,
                    encrypt_data_channel=True,
                    debug_level=None))
        except Exception as e:
            print(f'Ftp configuration of "{host}" seems incorrect, please check.')
        self.cwd = os.getcwd()
        self.dir = ''


    def chdir(self,dir):
        '''
        :param dir: str
                FTP directory to go
        '''
        try:
            try:
                if dir[-1] == '/':
                    dir = dir[:-1]
            except:
                pass
            self.conn.chdir(dir)
            self.dir = dir
        except:
            print(f'No such directory as "{dir}"')

    def close(self):
        '''
        Closing the FTP object connection
        '''
        self.conn.close()

    def list_dir(self):
        '''
        :return: list, a 2d list with 3 columns namely 'Filename/directory name',
        'Modification time',
        'Object type (d/-)' (d for directory, - for file)
        '''
        try:
            if self.sftp:
                details = self.conn.listdir_attr(self.dir)
                details = list(map(lambda x: [x.filename, dt.fromtimestamp(x.st_mtime),str(x)[0]],details))

                # details.sort(key=lambda x: x[1], reverse=True)
            else:
                details = list(map(lambda file: [file,
                                dt.fromtimestamp(self.conn.path.getmtime(file)),self.conn.path.isdir(file)],
                                self.conn.listdir(self.dir)))
                details = list(map(lambda x: [x[0],x[1],'d'] if x[2] else [x[0],x[1],'-'],details))
                details.sort(key=lambda x: x[1],reverse=True)
            details = list(map(lambda x: [f'/{x[0]}',x[1],x[2]] if x[2]=='d' else x,details))

            return details
        except Exception as e:
            print(e)
            return []

    def rename(self,source_name,tar_name):
        '''
        :param source_name: str, original name of the file within the current working directory of the Ftp object
        :param tar_name: str, renamed name of the file within the current working directory of the Ftp object
        '''
        try:
            self.conn.rename(f'{self.dir}/{source_name}',f'{self.dir}/{tar_name}')
        except Exception as e:
            print(e)

    def download(self,source_name, location):
        '''
        :param source_name: str, name of file or directory to download
        :param location: str, local machine directory to store the downloaded file/directory
        '''
        try:
            try:
                if location[-1]=='\\':
                    location = location[:-1]
            except:
                pass
            if self.sftp:
                self.conn.get(f'{self.dir}/{source_name}',f'{location}\\{source_name}')
            else:
                self.conn.download(f'{self.dir}/{source_name}',f'{location}\\{source_name}')
        except Exception as e:
            print(e)

    def upload(self,source_name, location):
        '''
        Uploading the specified file/directory to the cwd of the FTP object
        :param source_name: str, name of file or directory to upload
        :param location: str, local machine directory to upload the file/directory from
        '''
        try:
            try:
                if location[-1] == '\\':
                    location = location[:-1]
            except:
                pass
            if self.sftp:
                self.conn.put(f'{location}\\{source_name}',f'{self.dir}/{source_name}')
            else:
                self.conn.upload(f'{location}\\{source_name}',f'{self.dir}/{source_name}')
        except Exception as e:
            print(e)
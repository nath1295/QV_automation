import os
import subprocess
import time
import warnings
from datetime import datetime as dt
from pathlib import Path
from zipfile import ZipFile
import logging
import pandas as pd

# Project modules
from central_log import GsLogger
from filename_pattern import DatedFile
from ftp import Ftp, ftp_config, get_ftp_user
from source_reading import tl_data, check, get_projects

warnings.filterwarnings('ignore')

class ToolService:
    '''Manager of a tool in a project'''
    def __init__(self,project,tool):
        self.__project = project
        self.__tool = tool

class ProjectManager:
    '''Manage a project'''
    def __init__(self,project):
        self.__project = project
        self.__tools = tl_data(project)
        cuser = self.__tools['owner'].unique()
        checker = check(self.__tools)
        if len(checker) != 0:
            for err in checker['errors']:
                print(err)
            print('please correct the setting spreadsheet before running the QV automator.')
            time.sleep(300)
            exit()
        if len(self.__tools) == 0:
            print(f'You are not the owner of any tools for [{project}] on QV automator.')
            time.sleep(300)
            exit()
        if len(cuser) != 1:
            print(
                f'You have more than one user registered on "ftp_credentials.csv". Please check the credentials again.')
            time.sleep(300)
            exit()
        else:
            cuser = cuser[0]
        gslog = GsLogger()
        try:
            if logging_gs != '':
                gslog.create_private_log(logging_gs)
                logging_gs = True
            else:
                logging_gs = False
        except:
            print(
                'There is an issue with the log ggss link you provided, please check [User register] tab in the set up')
            time.sleep(300)
            exit()


class Scheduler:
    '''Schedule projects run time'''
    def __init__(self):
        pass


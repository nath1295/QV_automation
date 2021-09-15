from datetime import datetime as dt,timedelta
from dateutil.relativedelta import relativedelta
import re
import time



class DatedFile:
    '''
    File object with date information
    '''
    def __init__(self,filename,pattern,dayshift='0',report_range='mon-sun'):
        self.pattern = pattern
        self.filename = filename
        self.dayshift = dayshift
        self.report_range = report_range
        file_date = self.date_transformer(pattern,filename)
        self.file_date = self.dayshifting(file_date,dayshift,report_range)

    def regex_transformer(self,pattern):
        '''
        :param pattern: str, The input pattern from setup page
        :return: list, A pure regex pattern (str) and date groups in a list
        '''
        # Removing groups
        pattern = pattern.replace(r'\(', r'__bracketOPEN')
        pattern = pattern.replace(r'\)', r'bracketCLOSE__')
        pattern = pattern.replace(r'(', '')
        pattern = pattern.replace(r')', '')
        pattern = pattern.replace(r'__bracketOPEN',r'\(')
        pattern = pattern.replace(r'bracketCLOSE__',r'\)')

        # Replace datetime codes with regex groups

        date_code_dict = {'%W': '(\d{1,2})',
                          '%Y': '(\d{4})',
                          '%y': '(\d{2})',
                          '%m': '(\d{1,2})',
                          '%d': '(\d{1,2})',
                          '%b': '([A-Za-z]{3})',
                          '%B': '([A-Za-z]{3,9})',
                          '%j': '(\d{1,3})',
                          '%%': '(%)'}
        err_msg = 'Warning: Invalid pattern, datetime code not identified.'
        new_pattern = ''
        last_char = ''
        date_info = []
        keys = list(date_code_dict.keys())
        valid_date_char = list(map(lambda x: x[1],keys))
        for char in pattern:
            if char!='%' and last_char!='%':
                new_pattern += char
                last_char = char
            elif char=='%' and last_char!='%':
                last_char = char
            elif char!='%' and last_char=='%':
                if char in valid_date_char:
                    new_pattern += date_code_dict[last_char+char]
                    date_info.append(last_char + char)
                    last_char = char
                else:
                    new_pattern = err_msg
                    break
            elif char=='%' and last_char=='%':
                new_pattern += char
                last_char = ''
        return new_pattern, date_info

    def date_transformer(self,pattern,filename):
        '''
        :return: file_date (datetime object if valid, otherwise str)
        '''
        regex_pattern, date_code = self.regex_transformer(pattern)
        regex = re.compile(regex_pattern)
        matches = regex.match(filename)
        if matches:
            file_date_info = list(matches.groups())
            if len(date_code)==0:
                file_date = dt.strptime('1970-01-01', '%Y-%m-%d')
            elif ('%Y' not in date_code) and ('%y' not in date_code):
                date_code.append('%Y')
                file_date_info.append(dt.today().strftime('%Y'))
                file_date = self.pattern_sorting(date_code,file_date_info)
                if (file_date - dt.today()).days > 100:
                    year = str(int(file_date.strftime('%Y')) - 1)
                    date_code.append('%Y')
                    file_date_info.append(year)
                    file_date = self.pattern_sorting(date_code,file_date_info)
            elif ('%Y' in date_code) or ('%y' in date_code):
                file_date = self.pattern_sorting(date_code,file_date_info)

        else:
            file_date = 'Filename/pattern not matching'
        return file_date

    def pattern_sorting(self,date_code_,file_date_info_):
        date_code = date_code_
        file_date_info = file_date_info_
        month_sym = ['%m', '%b', '%B']
        day_sym = ['%d']
        if ('%W' in date_code):
            if (len(month_sym + date_code) > len(list(set(month_sym + date_code)))) \
                    and (len(day_sym + date_code) > len(list(set(day_sym + date_code)))):
                file_date = dt.strptime('-'.join(file_date_info), '-'.join(date_code))
            else:
                iso_var = 6
                date_dict = dict(zip(date_code,file_date_info))
                if '%Y' in date_dict.keys():
                    first_day = dt.strptime(date_dict['%Y']+'0101','%Y%m%d')
                    if first_day.weekday() <= 3:
                        iso_var = 13
                else:
                    first_day = dt.strptime(date_dict['%y'] + '0101', '%y%m%d')
                    if first_day.weekday() <= 3:
                        iso_var = 13
                date_code.append('%w')
                file_date_info.append('0')
                file_date = dt.strptime('-'.join(file_date_info), '-'.join(date_code)) - timedelta(
                    days=iso_var)
        elif (len(month_sym + date_code) > len(list(set(month_sym + date_code)))) \
                and (len(day_sym + date_code) == len(list(set(day_sym + date_code)))):
            date_code.append('%d')
            file_date_info.append('01')
            file_date = dt.strptime('-'.join(file_date_info), '-'.join(date_code))

        else:
            if len(date_code) not in [0, 1]:
                file_date = dt.strptime('-'.join(file_date_info), '-'.join(date_code))
            else:
                file_date = dt.strptime('1970-01-01', '%Y-%m-%d')
        return file_date

    def dayshifting(self,date,dayshift,report_range):
        file_date = date
        def date_back(wday,date):
            file_date = date
            if isinstance(file_date,str):
                return file_date
            else:
                while file_date.weekday()!=wday:
                    file_date = file_date - timedelta(days=1)
                return file_date

        special_shift = dict(month_end=lambda date: date+relativedelta(months=1,days=-1),
                             sun=lambda date: date_back(6,date),
                             sat=lambda date: date_back(5,date),
                             fri=lambda date: date_back(4,date),
                             thu=lambda date: date_back(3,date),
                             wed=lambda date: date_back(2,date),
                             tue=lambda date: date_back(1,date),
                             mon=lambda date: date_back(0,date))

        if (file_date != dt.strptime('1970-01-01','%Y-%m-%d')) and (file_date!='Filename/pattern not matching'):
            if dayshift[0]=='-':
                file_date = file_date - timedelta(days=int(dayshift[1:]))
            elif dayshift[0]=='+':
                file_date = file_date + timedelta(days=int(dayshift[1:]))
            elif dayshift in special_shift:
                file_date = special_shift[dayshift](file_date)
            elif dayshift[:12]=='business_day':
                try:
                    start,end = report_range.split('-',maxsplit=1)
                except:
                    start,end = ('mon','sun')
                week_days = ['mon','tue','wed','thu','fri','sat','sun']
                week_days_dict = {value:key for key, value in dict(enumerate(week_days)).items()}
                start = week_days_dict[start]
                end = week_days_dict[end]
                rge = []
                cont = True
                while cont:
                    if start>=7:
                        start-=7
                    rge.append(start)
                    if start==end:
                        cont = False
                    start+=1
                if start>end:
                    end+=7
                if len(dayshift)>12:
                    if dayshift[12]=='+':
                        shift = int(dayshift[13:])

                    elif dayshift[12]=='-':
                        shift = int(dayshift[13:])

                    else:
                        shift = 0
                else:
                    shift = 0
                count = 0
                while count!=shift:
                    if dayshift[12] == '-':
                        file_date = file_date - timedelta(days=1)
                    else:
                        file_date = file_date + timedelta(days=1)
                    if file_date.weekday() in rge:
                        count+=1
        return file_date

    def str_date(self,pattern,file_date='default'):
        if file_date == 'default':
            file_date = self.file_date
        if ('%W' in pattern):
            week = str(int(self.file_date.strftime('%W')))
            if len(week)==1:
                week = '0'+week
            if week=='00':
                week = '53'
            temp_pattern = pattern.replace('%%','__sign__escape__')
            temp_pattern = temp_pattern.replace('%W','$-----------$')
            temp_pattern = temp_pattern.replace('__sign__escape__','%%')
            str_filedate = file_date.strftime(temp_pattern)
            str_filedate = str_filedate.replace('$-----------$',week)
        else:
            str_filedate = file_date.strftime(pattern)
        return str_filedate

    def output_matching(self,filename,pattern):
        outdate = self.date_transformer(pattern,filename)
        status = self.str_date(pattern,outdate)==self.str_date(pattern)
        return status




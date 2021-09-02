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
        self.date_transformer()
        self.dayshifting()

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

    def date_transformer(self):
        '''
        :return: self.file_date (datetime object if valid, otherwise str)
        '''
        self.regex_pattern, self.date_code = self.regex_transformer(self.pattern)
        regex = re.compile(self.regex_pattern)
        matches = regex.match(self.filename)
        if matches:
            self.file_date_info = list(matches.groups())
            if len(self.date_code)==0:
                self.file_date = dt.strptime('1970-01-01', '%Y-%m-%d')
            elif ('%Y' not in self.date_code) and ('%y' not in self.date_code):
                self.date_code.append('%Y')
                self.file_date_info.append(dt.today().strftime('%Y'))
                self.pattern_sorting()
                if (self.file_date - dt.today()).days > 100:
                    year = str(int(self.file_date.strftime('%Y')) - 1)
                    self.regex_pattern, self.date_code = self.regex_transformer(self.pattern)
                    regex = re.compile(self.regex_pattern)
                    matches = regex.match(self.filename)
                    self.file_date_info = list(matches.groups())
                    self.date_code.append('%Y')
                    self.file_date_info.append(year)
                    self.pattern_sorting()
            elif ('%Y' in self.date_code) or ('%y' in self.date_code):
                self.pattern_sorting()

        else:
            self.file_date = 'Filename/pattern not matching'

    def pattern_sorting(self):
        month_sym = ['%m', '%b', '%B']
        day_sym = ['%d']
        if ('%W' in self.date_code):
            if (len(month_sym + self.date_code) > len(list(set(month_sym + self.date_code)))) \
                    and (len(day_sym + self.date_code) > len(list(set(day_sym + self.date_code)))):
                self.file_date = dt.strptime('-'.join(self.file_date_info), '-'.join(self.date_code))
            else:
                iso_var = 6
                date_dict = dict(zip(self.date_code,self.file_date_info))
                if '%Y' in date_dict.keys():
                    first_day = dt.strptime(date_dict['%Y']+'0101','%Y%m%d')
                    if first_day.weekday() <= 3:
                        iso_var = 13
                else:
                    first_day = dt.strptime(date_dict['%y'] + '0101', '%y%m%d')
                    if first_day.weekday() <= 3:
                        iso_var = 13
                self.date_code.append('%w')
                self.file_date_info.append('0')
                self.file_date = dt.strptime('-'.join(self.file_date_info), '-'.join(self.date_code)) - timedelta(
                    days=iso_var)
        elif (len(month_sym + self.date_code) > len(list(set(month_sym + self.date_code)))) \
                and (len(day_sym + self.date_code) == len(list(set(day_sym + self.date_code)))):
            self.date_code.append('%d')
            self.file_date_info.append('01')
            self.file_date = dt.strptime('-'.join(self.file_date_info), '-'.join(self.date_code))

        else:
            if len(self.date_code) not in [0, 1]:
                self.file_date = dt.strptime('-'.join(self.file_date_info), '-'.join(self.date_code))
            else:
                self.file_date = dt.strptime('1970-01-01', '%Y-%m-%d')



    def dayshifting(self):
        def date_back(wday):
            if isinstance(self.file_date,str):
                return self.file_date
            else:
                date = self.file_date
                while date.weekday()!=wday:
                    date = date - timedelta(days=1)
                return date

        special_shift = dict(month_end=lambda date: date+relativedelta(months=1,days=-1),
                             sun=lambda date: date_back(6),
                             sat=lambda date: date_back(5),
                             fri=lambda date: date_back(4),
                             thu=lambda date: date_back(3),
                             wed=lambda date: date_back(2),
                             tue=lambda date: date_back(1),
                             mon=lambda date: date_back(0))

        if (self.file_date != dt.strptime('1970-01-01','%Y-%m-%d')) and (self.file_date!='Filename/pattern not matching'):
            if self.dayshift[0]=='-':
                self.file_date = self.file_date - timedelta(days=int(self.dayshift[1:]))
            elif self.dayshift[0]=='+':
                self.file_date = self.file_date + timedelta(days=int(self.dayshift[1:]))
            elif self.dayshift in special_shift:
                self.file_date = special_shift[self.dayshift](self.file_date)
            elif self.dayshift[:12]=='business_day':
                try:
                    start,end = self.report_range.split('-',maxsplit=1)
                except:
                    start,end = ('mon','sun')
                week_days = ['mon','tue','wed','thu','fri','sat','sun']
                week_days_dict = {value:key for key, value in dict(enumerate(week_days)).items()}
                start = week_days_dict[start]
                end = week_days_dict[end]
                if start>end:
                    end+=7
                if len(self.dayshift)>12:
                    if self.dayshift[12]=='+':
                        shift = int(self.dayshift[13:])
                        rge = list(range((start+shift),(end+shift)+1))

                    elif self.dayshift[12]=='-':
                        shift = int(self.dayshift[13:])
                        rge = list(range((start-shift),(end-shift)+1))
                    else:
                        rge = list(range(start, end + 1))
                else:
                    rge = list(range(start,end+1))
                rge = list(map(lambda x: (x+7)%7 if x<0 else x%7,rge))
                while self.file_date.weekday() not in rge:
                    if self.dayshift[12] == '-':
                        self.file_date = self.file_date + timedelta(days=1)
                    else:
                        self.file_date = self.file_date - timedelta(days=1)

    def str_date(self,pattern):
        if ('%W' in pattern):
            week = str(int(self.file_date.strftime('%W')))
            if len(week)==1:
                week = '0'+week
            if week=='00':
                week = '53'
            temp_pattern = pattern.replace('%%','__sign__escape__')
            temp_pattern = temp_pattern.replace('%W','$-----------$')
            temp_pattern = temp_pattern.replace('__sign__escape__','%%')
            self.str_filedate = self.file_date.strftime(temp_pattern)
            self.str_filedate = self.str_filedate.replace('$-----------$',week)
        else:
            self.str_filedate = self.file_date.strftime(pattern)
        return self.str_filedate


if __name__=='__main__':
    import time
    print('FILENAME PATTERN TESTER FOR QV AUTOMATION\n')
    pattern = input('Input filename pattern: ')
    bus_day = input('Business days range [mon-sun]: ')
    if bus_day=='':
        bus_day = 'mon-sun'
    dshift = input('Day shift option [0]: ')
    if dshift=='':
        dshift = '0'
    output = input('Output filename pattern(s): ')
    filename = input('File name: ')
    file = DatedFile(filename,pattern,dshift,bus_day)
    print(file.str_date('Date: %Y-%m-%d (YYYY-MM_DD)\nWeek: %W'))
    print('Output filenames:')
    for out in output.split('|'):
        print(file.str_date(out))
    # if output_pattern !='':
    #     dayshift = input('dayshift option: ')
    #     options = ['-13', '-12', '-11', '-10', '-9', '-8', '-7', '-6', '-5', '-4', '-3', '-2', '-1', '0',
    #                '+1', '+2', '+3', '+4', '+5', '+6', '+7', '+8', '+9', '+10', '+11', '+12', '+13',
    #                "mon","tue","wed","thu","fri","sat","sun","month_end",
    #                "business_day-7","business_day-6","business_day-5",
    #                "business_day-4","business_day-3","business_day-2","business_day-1","business_day",
    #                "business_day+1","business_day+2","business_day+3","business_day+4","business_day+5",
    #                "business_day+6","business_day+7"]
    #     if dayshift not in options:
    #         out_file = DatedFile(filename, pattern)
    #     else:
    #         out_file = DatedFile(filename,pattern,dayshift)
    #     print('renamed filename: '+out_file.str_date(output_pattern))
    time.sleep(300)

    #
    #
    #
    print('End')
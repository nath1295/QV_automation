import os
import multiprocessing
import time
from shutil import copytree,rmtree
from distutils.dir_util import copy_tree
from datetime import datetime as dt

# This class is for running python script as tool
class PyScript:
    def __init__(self,tool,path):
        tool_name = tool.split('.')
        self.__base_name = '.'.join(tool_name[:-1])
        ext = tool_name[-1]
        if ext!='py':
            raise RuntimeError(f'"{tool}" does not look like a python script.')
        self.__path = path
        self.success = self.__run_python()

    def __run_python(self):
        files = os.listdir(self.__path)
        cwd = os.getcwd()
        if f"{self.__base_name}.py" not in files:
            return False
        else:
            os.chdir('python_temp')
            now = dt.now().timestamp()
            out_code = os.system(f'python {self.__base_name}.py')
            os.chdir(cwd)
            if out_code==0:
                try:
                    local = walking('python_temp')
                    local = list(map(lambda x: [x, os.path.getmtime(x)], local))
                    local = list(filter(lambda x: x[1] > now, local))
                    if len(local)>0:
                        copy_tree('python_temp', self.__path)
                    return True
                except Exception as e:
                    print(e)
                    return False
            else:
                return False

def run_script(tool, path):
    # Thread that host the python running workflow
    multiprocessing.freeze_support()
    return_dict = dict()
    print(f'start running tool "{tool}"')
    success = True
    copytree(path, 'python_temp')
    p = multiprocessing.Process(target=loading_py(tool, path, return_dict))
    st = time.perf_counter()
    p.start()
    p.join(600)
    if p.is_alive():
        p.terminate()
        p.join()
        ed = time.perf_counter() - st
        print(f'{tool}: Transformation failed; Time taken: {round(ed, 2)} seconds')
        success = False
    else:
        ed = time.perf_counter() - st
        if return_dict['status']:
            print(f'{tool}: Transformation completed; Time taken: {round(ed, 2)} seconds')
        else:
            print(f'{tool}: Transformation failed; Time taken: {round(ed, 2)} seconds')
            success = False
    rmtree('python_temp')
    return success

def loading_py(tool, path, return_dict):
    # QV running workflow
    pys = PyScript(tool,path)
    status = pys.success
    return_dict['status'] = status

def walking(path):
    items = list()
    for root, dirs, files in os.walk(path, topdown=False):
        for name in files:
            items.append(f'{root}\\{name}')
        for name in dirs:
            items.append(f'{root}\\{name}')
    items = set(items)
    return items




if __name__=='__main__':

    suc = run_script('test.py',r'C:\Users\dmtqv\Documents\QV_automation')
    print('done')
    # with open('temp.txt','r') as temp:
    #     tl = temp.read().split(',')
    #
    # flow = run_script(tl[0],tl[1])
    # if flow:
    #     flow = 't'
    # else:
    #     flow = 'f'
    # with open('return.txt', 'w') as temp:
    #     temp.write(flow)
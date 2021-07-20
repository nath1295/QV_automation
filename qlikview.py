from win32com.client import gencache
import time
import psutil
import multiprocessing



class Qlikview:
    def __init__(self):
        '''
        :param file: str, the qlikview tool name with extension '.qvw'
        '''
        self.qlik = gencache.EnsureDispatch('QlikTech.QlikView')

    def open_tool(self, file, path):
        self.file = file
        self.tool = self.qlik.OpenDoc(path + f'\\{file}')

    def reload_script(self):
        self.tool.ReloadEx(0, 1)

    def close_doc(self):
        try:
            self.tool.CloseDoc()
        except:
            print(f'Close {self.file} failed.')

    def close(self):
        self.qlik.Quit()


def run_flow(tool,path):
    # Thread that host the QV running workflow, killing the QV tool in case the QV transformation failed
    multiprocessing.freeze_support()
    success = True
    p = multiprocessing.Process(target=loading_flow, args=(tool, path))
    st = time.perf_counter()
    p.start()
    p.join(30)
    if p.is_alive():
        p.terminate()
        p.join()
        ed = time.perf_counter()-st
        print(f'{tool}: Transformation failed; Time taken: {round(ed,2)} seconds')
        success = False
        # kill qlikview if the script failed
        tasks = psutil.process_iter()
        for i in tasks:
            if i.name().lower() == 'qv.exe':
                i.kill()
    else:
        ed = time.perf_counter()-st
        print(f'{tool}: Transformation completed; Time taken: {round(ed,2)} seconds')
    return success

def loading_flow(tool,path):
    # QV running workflow
    QV = Qlikview()
    QV.open_tool(tool,path)
    print(f'{tool} opened')
    QV.reload_script()
    print(f'{tool} reloaded')
    QV.close_doc()
    print(f'{tool} closed')

if __name__=='__main__':

    with open('temp.txt','r') as temp:
        tl = temp.read().split(',')

    flow = run_flow(tl[0],tl[1])
    if flow:
        flow = 't'
    else:
        flow = 'f'
    with open('return.txt', 'w') as temp:
        temp.write(flow)

    # QV.close()
    # os.system('taskkill/im qv.exe')
    # qv = Qlikview()
    # qv.close()


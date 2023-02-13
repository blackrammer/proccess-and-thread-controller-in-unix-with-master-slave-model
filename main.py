import os
import requests
import psutil
import threading
import json
import hashlib
import pathlib
import re

URL = 'http://127.0.0.1:8000'
WORKER_COUNT = 5
worker_ids = []
file_database = []
md5_database = []
json_database = []

penaltis = []
finished = {
    "f":False
}

segment = 0
semaphore = threading.Semaphore(1)
lock = threading.Lock()
flock = threading.Lock()


def check_proccess_uptime(pids):
    for pid in pids:
        # print('checking procces #',pid)
        p = psutil.Process(pid)
        if( p.status() == psutil.STATUS_ZOMBIE):
            print('procces died')
    return p.status()


def md5_executer(path,working):
    print(f"{path} to creation md5")
    print(f"{path} exist : {os.path.exists(path)}")
    
    with open(path, "r") as f:
        print('opening ....')
        data = f.read()
        md5 = hashlib.md5(data.encode()).hexdigest()
        write = open(path + '.md5',"w")

        if not working :
            print('Error : ',path)
            write.write('Some Thing Wrong bro')
        else :
            write.write(md5)
        return 'success'


def worker_creator():
    global finished
    for i in range(0,WORKER_COUNT):
        parent_pid = os.getpid()
        #print('this is parent id :',parent_pid)
        n = os.fork()
        if(n == 0):
            #child number i
            print(i,' :proccess id :',os.getpid())
            while(True):
                #check for finish or not
                worker_ids.append(os.getpid())
                response = requests.get(URL+'/worker/{}'.format(os.getpid()))
                file = json.loads(response.text)
                # lock.acquire()
                # if len(json_database) - 1 <= segment:
                #     print('proccess:',os.getpid(),' All Work Done / Waiting For new Tasks')
                #     lock.release()
                #     return
                # lock.release()
                print('waiting for new jobs ...')

                # print(os.getpid(),' ::: ',file)
                #start to md5 stored file
                for path in file:
                    md5_res = ''
                    if re.search(r".*0.json$",path):
                         # doing someting wrong
                        md5_res = md5_executer(path,False)
                    else :
                        md5_res = md5_executer(path,True)
                    if(md5_res == 'success'):
                        # report done with path
                        requests.post(URL+'/report',json={'address':[os.getpid(),path]})
            return
        elif os.getpid() == parent_pid:
            #print('this is parent id :',parent_pid)
            ll = 1

def commander():
    print('commander one starting ...')
    file_list = []
    md5_list = []
    for file in os.scandir('TransactionFiles'):
        if file.is_file():
            file_database.append(file)
            format =pathlib.Path(file.name).suffix
            if format == '.md5' and file not in md5_database:
                md5_database.append(file)
            elif format == '.json' and file not in json_database:
                json_database.append(file)
        # break            
    lock.release()
    print('commander one end')

def get_md5_json(path):
    with open(path, "r") as f:
        data = f.read()
        md5 = hashlib.md5()
        md5.update(data.encode())
        hash_value = md5.hexdigest()
        return hash_value

def commander_watch():
    global segment,finished
    print('watching ...')
    while(True):
        lock.acquire()
        flock.acquire()
        print(len(json_database) - 1 , segment)
        if finished['f']:
            print("Finished Commander Thread...")
            return
        if len(json_database) - 1 <= segment:
            finished['f'] = True
            print("Finished Commander Thread...")
            flock.release()
            #finished req to server
            return
        flock.release()

        file = json_database[segment]
        fname = file.path
        segment += 1
        mdfile = fname + ".md5"
        if os.path.exists(mdfile):
            comperison_json = get_md5_json(fname)
            with open(mdfile,"r") as md5:
                comperison_md5 = md5.read()
                if comperison_md5 != comperison_json:
                        print("Error Md5 of file",fname,"Changed...")
                        penaltis.append(fname) 
                        # ------------------------
                        #write = open('penalti.txt',"w")
                        #write.write('Penaltis'+penaltis+':::'+os.getpid())
                        #-------------------------
                                  
        else:
            print(f"Re creation md5 of file {fname}")
            json_database.append(file)
            requests.post(URL+"/commander/dumy",json={'address':str(fname)})

        lock.release()

    # finished

# def commanders():
#     print('commander one starting ...')
#     file_list = []
#     for file in os.scandir('TransactionFiles'):
#         if file.is_file():
#             file_list.append(str(file.path))
#     response = requests.post(URL+"/commander/dumy",json={'address':file_list})
#     print('commander one end')



worker_creator()

lock.acquire()



threads = []

thread1 = threading.Thread(target=commander)

threads.append(thread1)



for i in range(5):
    
    thread2 = threading.Thread(target=commander_watch)
    threads.append(thread2)

for thread in threads:
    thread.start()


for thread in threads:
    thread.join()



# print (md5_database)

# print( ' //////// ', json_database)




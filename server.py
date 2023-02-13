from typing import Union
from fastapi import FastAPI
from pydantic import BaseModel
import threading

semaphore = threading.Semaphore(1)

app = FastAPI()

files_list = []
in_use_files_list = []
status = False


class File(BaseModel):
    address: list 
class Path(BaseModel):
    address: str 

@app.get("/")
def read_root():
    return 'server check <br>'+ files_list

@app.get("/worker/{worker_id}")
def read_item(worker_id: int):
    global files_list,in_use_files_list
    #check fo how many procces make
    semaphore.acquire()
    result = []
    offset = min(len(files_list),5)
    used = files_list[0:offset]
    files_list = files_list[offset:]

    for path in used:
        in_use_files_list.append([worker_id,path])
        print(f'Task scheduler ::: PID ={worker_id}  And FILE={path}')
        
    for path in in_use_files_list:
        if path[0] == worker_id:
            result.append(path[1])
    semaphore.release()
    return result


@app.post("/commander/dumy")
def commander(path: Path):
    print(path)
    semaphore.acquire()
    global files_list
    files_list.append(path.address)
    # files_list.append(file.address)
    #files_list = file.address.copy()
    semaphore.release()
    return "Files successfully collected by server"


# @app.post("/commander/dumy")
# def commander(file: File):
#     semaphore.acquire()
#     global files_list
#     files_list = file.address.copy()
#     print(files_list)
#     # files_list.append(file.address)
#     #files_list = file.address.copy()
#     semaphore.release()
#     return "Files successfully collected by server"


@app.post("/report")
def report_after_md5(file: File):
    semaphore.acquire()
    for task in in_use_files_list:
        if task[0] == file.address[0] and task[1] == file.address[1]:
            in_use_files_list.remove(task)
    print('md5 Created , creator :',file.address[0],'::: file :',file.address[1])
    semaphore.release()
    return str(len(in_use_files_list))





@app.get("/status")
def read_status():

    return status

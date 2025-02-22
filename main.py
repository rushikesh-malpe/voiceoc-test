
import os
import time
import shutil
import mysql.connector

from threading import Thread,Lock


#Paths
PROCESSING_DIR='processing'
IN_QUEUE_DIR="in-queue"
PROCESS_DIR='processed'


#Ensure all directories exits
for folder in [PROCESSING_DIR,IN_QUEUE_DIR,PROCESSING_DIR]:
    os.makedirs(folder,exist_ok=True)


#database config
db_config={
    "host":"localhost",
    "port":3367,
    "password":"password",
    "database":"voiceoc"
}


#lock for synchronization
lock=Lock()

conn=mysql.connector.connect(**db_config)
cursor=conn.cursor()


def create_files():
    """Create a text file in processing folder every second """
    i=1
    while True:
        file_path=os.path.join(PROCESSING_DIR,f"file_{i}.txt")
        with open(file=file_path,mode="w") as f:
            f.write(f"Data in file {i} \n")
        print("Created:",file_path)
        i+=1
        time.sleep(1)

def move_to_in_queue():
    """Move file from processing to in queue every 5 second if in-queue is empty"""
     
    while True:
        with lock:
            if not os.listdir(IN_QUEUE_DIR):
                files=os.listdir(PROCESSING_DIR)
                for file in files:
                    shutil.move(os.path.join(PROCESSING_DIR,file),os.path.join(IN_QUEUE_DIR,file))
                    print(f"Moved to in queue: {file}")
                time.sleep(5)



def process_queue():
    """Update MySQL and move file from in queue  to process file"""
    while True:
        with lock:
            files=os.listdir(IN_QUEUE_DIR)
            for file in files:
                try:
                    file_path=os.path.join(IN_QUEUE_DIR,file)
                    cursor.execute("UPDATE  process_queue SET process=1 WHERE filename= %s",(file,))
                    cursor.commit()
                    print(f"Update DB and moved to processed: {file}")
                except Exception as error:
                    print(f"DB Error: {error}")
                    continue

                shutil.move(file_path,os.path.join(PROCESS_DIR,file))
        time.sleep(2)




Thread(target=create_files,daemon=True).start()
Thread(target=move_to_in_queue,daemon=True).start()
Thread(target=process_queue,daemon=True).start()


while True:
    time.sleep(1)

























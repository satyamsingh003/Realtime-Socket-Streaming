import json
import socket
import time
import pandas as pd

def send_data_over_socket(file_path,host='127.0.0.1',port=9999,chunk_size=2):
    s=socket.socket(socket.AF_INET,socket.SOCK_STREAM) # Create an IPV4 TCP socket
    s.bind((host,port))  # s.bind(("127.0.0.1", 9999))
    s.listen(1) # ready to connect and by 1 means it can hold maximum of 1 connection
    print(f"Listening for connection on {host}:{port}")
    conn,addr=s.accept()  # conn is the new connection socket and addr is the client address ('127.0.0.1', 54321)
    print(f"Connection from {addr}")

    last_sent_index=0
    try:
        with open(file_path,'r') as file:
            # skip the file that were already sent
            for  _ in range(last_sent_index):
                next(file)
            records=[]
            for line in file:
                records.append(json.loads(line))  #converts json string into python dictionary
                if len(records)==chunk_size:
                    chunk=pd.DataFrame(records)  # it convert hte json file in the tabular form
                    print(chunk)
                    for record in chunk.to_dict(orient='records'):
                        serialize_data=json.dumps(record).encode('ut-8')   # it convert the json object into string and then into bytes
                        conn.send(serialize_data+b'\n')  # all the records are separated by \n so that tcp could know each decord different as it understand only bytes
                        time.sleep(5)
                        last_sent_index+=1
                    records=[]
    except (BrokenPipeError,ConnectionResetError):
        print("Client diconnected")
    finally:
        conn.close()
        print("Connection closed")

if __name__=='__main__':
    send_data_over_socket('datasets/yelp_academic_dataset_review.json')


#Python object
#      ↓
# JSON serialization
#      ↓
# String
#      ↓
# UTF-8 encoding
#      ↓
# Bytes
#      ↓
# TCP




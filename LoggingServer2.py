import datetime
import json
import logging
import socket
import threading
import time
import traceback
from logging.handlers import RotatingFileHandler, TimedRotatingFileHandler
import pickle
# import logging.handlers.MultiHandler
run_status = True
HEADER = 8
FORMAT = 'utf-8'
PORT = 9197
SERVER = socket.gethostbyname(socket.gethostname())
ADDR = (SERVER, PORT)
DISCONNECT_MSG = "!DISCONNECT!"
Connections={"ACTIVE_CONNECTIONS":{}, "INACTIVE_CONNECTIONS":{}}
total_conn = 0
server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.bind(ADDR)

def handle_client(conn, addr, max_log_size_bytes=100*1024*1024):
        global total_conn, Connections
        output_count = 0
        pnf_count = 0
        variant_count = 0
        key=0
        print(f"[NEW CONNECTION] {addr} connected.")
        # total_conn+=1
        if addr[0] in Connections["ACTIVE_CONNECTIONS"].keys():
                key = len(Connections["ACTIVE_CONNECTIONS"][addr[0]].keys())
                Connections["ACTIVE_CONNECTIONS"][addr[0]][len(Connections["ACTIVE_CONNECTIONS"][addr[0]].keys())] = {"Last Contact":str(datetime.datetime.now())}
        else:
                Connections["ACTIVE_CONNECTIONS"][addr[0]] = {}
                Connections["ACTIVE_CONNECTIONS"][addr[0]][key] = {"Last Contact":str(datetime.datetime.now())}
        print("CONNECTION STATUS: ", json.dumps(Connections, indent=4))
        connected=True
        logger = logging.getLogger()
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        file_handler = RotatingFileHandler(f'{addr[0]}.log', maxBytes=max_log_size_bytes, backupCount=2)
        # time_handler = TimedRotatingFileHandler(f'{addr}.log', when='D', interval=3, backupCount=7)
        # multi_handler = logging.handlers.MultiHandler([file_handler, time_handler])
        #stream_handler = logging.StreamHandler()
        #stream_handler.setFormatter(formatter)
        #stream_handler.setLevel(logging.INFO)
        #logger.addHandler(stream_handler)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        # logger.addHandler(time_handler)
        logger.setLevel(logging.INFO)
        msg_length = conn.recv(HEADER).decode(FORMAT)
        while connected:
                # logging.basicConfig(filename=f'{addr[0]}.log', level=logging.INFO,
                #                                         format="%(asctime)s - %(levelname)s - %(message)s")

                try:
                        if msg_length:
                                try:
                                        msg_length = int(msg_length)
                                except:
                                        msg_length = 1024
                                try:
                                        if msg_length == -1:
                                                raise ConnectionResetError
                                        msg = conn.recv(msg_length).decode(FORMAT)
                                except ConnectionResetError:
                                        # connected = False
                                        msg = DISCONNECT_MSG
                                Connections["ACTIVE_CONNECTIONS"][addr[0]][key]["Last Contact"] = str(datetime.datetime.now())
                                # print(msg)
                                if msg!="PING" and msg!=DISCONNECT_MSG:
                                        if "Completed_batch" in msg:
                                                output_count+=int(msg.split(':-:')[-1].split('Output_Count -')[1].split('-')[0].strip())
                                                maxthreads=int(msg.split(':-:')[-1].split('maxthreads -')[1].split('-')[0].strip())
                                                pnf_count+=int(msg.split(':-:')[-1].split('pnf_count -')[1].split('-')[0].strip())
                                                variant_count+=int(msg.split(':-:')[-1].split('variants_count -')[1].split('-')[0].strip())
                                                Connections["ACTIVE_CONNECTIONS"][addr[0]][key]['info'] = {"Output_crawled":output_count, "PNF":pnf_count, "Variants_gained":variant_count,"maxthreads":maxthreads}

                                        try:
                                                level = msg.split(':-:')[1]
                                                if level == "info":
                                                        logger.info(msg)
                                                elif level == "warning":
                                                        logger.warning(msg)
                                                elif level == "debug":
                                                        logger.debug(msg)
                                                elif level == "error":
                                                        logger.error(msg)
                                                else:
                                                        logger.info(msg)
                                        except :
                                                logger.info(msg)
                                                # print(traceback.format_exc())
                                                # print(msg)
                                                # print("level error")
                                                # pass


                        if msg == DISCONNECT_MSG:
                                print(f"[DISCONNECTED] {addr} disconnected.")
                                
                                try:
                                        if addr[0] in Connections["INACTIVE_CONNECTIONS"].keys():
                                                if key in Connections["INACTIVE_CONNECTIONS"][addr[0]].keys():
                                                        Connections["INACTIVE_CONNECTIONS"][addr[0]][key]["Disconnections"].append(str(datetime.datetime.now()))
                                                        Connections["INACTIVE_CONNECTIONS"][addr[0]][key]["Last Contact"] = Connections["ACTIVE_CONNECTIONS"][addr[0]][key]["Last Contact"]
                                                else:
                                                        Connections["INACTIVE_CONNECTIONS"][addr[0]][key] = Connections["ACTIVE_CONNECTIONS"][addr[0]][key]
                                                        Connections["INACTIVE_CONNECTIONS"][addr[0]][key]["Disconnections"]=[Connections["ACTIVE_CONNECTIONS"][addr[0]][key]["Last Contact"]]
                                        else:
                                                Connections["INACTIVE_CONNECTIONS"][addr[0]] = {}
                                                Connections["INACTIVE_CONNECTIONS"][addr[0]][key] = Connections["ACTIVE_CONNECTIONS"][addr[0]][key]
                                                Connections["INACTIVE_CONNECTIONS"][addr[0]][key]["Disconnections"]=[Connections["ACTIVE_CONNECTIONS"][addr[0]][key]["Last Contact"]]
                                        if len(Connections["INACTIVE_CONNECTIONS"][addr[0]][key]["Disconnections"])>10:
                                                Connections["INACTIVE_CONNECTIONS"][addr[0]][key]["Disconnections"] = Connections["INACTIVE_CONNECTIONS"][addr[0]][key]["Disconnections"][:11]
                                except Exception as e:
                                        print(e)
                                        print(traceback.format_exc())
                                del Connections["ACTIVE_CONNECTIONS"][addr[0]][key]
                                print(f"[ACTIVE CONNECTIONS] {threading.activeCount()-3}")
                                # print(Connections)
                                connected = False
                except Exception as e:
                        print(e)
                        print(traceback.format_exc())
                        
                if connected:
                        try:
                                msg_length = conn.recv(HEADER).decode(FORMAT)
                        except ConnectionResetError:
                                msg_length = -1

                        # print(f"[{addr} : {msg}]")

def start():

        server.listen()
        print("[INFO] STARTED")
        thread1 = threading.Thread(target=start_write_json, args=())
        thread1.start()
        while True:
                conn, addr = server.accept()
                thread = threading.Thread(target=handle_client, args=(conn, addr))
                thread.start()
                print(f"[ACTIVE CONNECTIONS] {threading.activeCount()-3}")
        pass


def start_write_json():
        global Connections
        while run_status:
                try:
                        with open('connections.pkl', 'wb') as file:
                  
                # A new file will be created
                                pickle.dump(Connections, file)
                except:
                        pass
                try:
                        with open("connections.info","w",encoding='utf-8', newline='') as file:
                                file.write(json.dumps(Connections, indent=4))
                                time.sleep(10)
                except:
                        try:
                                with open("connections1.info","w",encoding='utf-8', newline='') as file:
                                        file.write(json.dumps(Connections, indent=4))
                                        time.sleep(10)
                        except:
                                pass


if __name__ == '__main__':
        try:
                thread1 = threading.Thread(target=start_write_json, args=())
                thread1.start()
                print("[STARTING] Server is Starting..")
                start()
        except KeyboardInterrupt:
                run_status=False
        finally:
                run_status=False

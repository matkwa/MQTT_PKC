import socket
import struct
import time
import random

HOST = '127.0.0.1'
PORT = 1883
SENSOR_ID = 5

MSG_PUBLISH = 1
MSG_PUBACK = 2

FMT_PUBLISH = '!BBHf'
FMT_PUBACK = '!BBH'
PUBACK_SIZE = struct.calcsize(FMT_PUBACK)

TIMEOUT_SEC = 2.0
PUBLISH_INTERVAL_SEC = 3.0

def start_sensor():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
        try:
            client.connect((HOST, PORT))
            client.settimeout(TIMEOUT_SEC)
            print(f"Podłączono do brokera {HOST}:{PORT}\n")
        except ConnectionRefusedError:
            print("Błąd: Upewnij się, że broker jest uruchomiony.")
            return

        packet_id = 1

        while True:
            temp = round(random.uniform(20.0, 30.0), 2)
            frame = struct.pack(FMT_PUBLISH, MSG_PUBLISH, SENSOR_ID, packet_id, temp)
            
            ack_received = False
            
            while not ack_received:
                print(f"[SEND] Pkt: {packet_id} | Temp: {temp}°C")
                client.sendall(frame)

                try:
                    ack_data = client.recv(PUBACK_SIZE)
                    if not ack_data:
                        print("Połączenie przerwane.")
                        return

                    ack_type, ack_sensor_id, ack_packet_id = struct.unpack(FMT_PUBACK, ack_data)

                    if ack_type == MSG_PUBACK and ack_packet_id == packet_id:
                        print(f"[ACK]  Sukces -> Pkt: {packet_id}\n")
                        ack_received = True

                except socket.timeout:
                    print(f"[TIMEOUT] Brak ACK dla Pkt {packet_id}. Retransmisja...\n")

            packet_id += 1
            time.sleep(PUBLISH_INTERVAL_SEC)

if __name__ == '__main__':
    start_sensor()
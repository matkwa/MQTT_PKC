import socket
import struct

HOST = '127.0.0.1'
PORT = 1883

MSG_PUBLISH = 1
MSG_PUBACK = 2

FMT_PUBLISH = '!BBHf'
FMT_PUBACK = '!BBH'
PUBLISH_SIZE = struct.calcsize(FMT_PUBLISH)

def start_broker():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.bind((HOST, PORT))
        server.listen()
        print(f"Broker nasłuchuje na {HOST}:{PORT}")

        conn, addr = server.accept()
        with conn:
            print(f"Połączono z czujnikiem: {addr}\n")
            
            while True:
                try:
                    data = conn.recv(PUBLISH_SIZE)
                    if not data:
                        break

                    msg_type, sensor_id, packet_id, temp = struct.unpack(FMT_PUBLISH, data)

                    if msg_type == MSG_PUBLISH:
                        print(f"[RECV] ID: {sensor_id} | Pkt: {packet_id} | Temp: {temp:.2f}°C")
                        
                        ack_frame = struct.pack(FMT_PUBACK, MSG_PUBACK, sensor_id, packet_id)
                        conn.sendall(ack_frame)
                        print(f"[SEND] PUBACK -> Pkt: {packet_id}\n")

                except ConnectionResetError:
                    print("Połączenie zerwane przez klienta.")
                    break
                except Exception as e:
                    print(f"Błąd komunikacji: {e}")
                    break

if __name__ == '__main__':
    start_broker()
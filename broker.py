import socket
import struct
import logging
import threading

# Konfiguracja profesjonalnego loggera
logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
logger = logging.getLogger("Broker")

class MQTTBroker:
    # Stałe protokołu MQTT
    MSG_CONNECT = 1
    MSG_CONNACK = 2
    MSG_PUBLISH = 3
    MSG_PUBACK  = 4

    FMT_CONNECT = '!BB'
    FMT_PUBLISH = '!BHf'
    FMT_PUBACK  = '!BBH'

    def __init__(self, host: str = '127.0.0.1', port: int = 1883):
        self.host = host
        self.port = port

    def start(self) -> None:
        """Uruchamia serwer i nasłuchuje na połączenia w głównej pętli."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
            # allow reuse address pozwala na szybsze ponowne uruchomienie serwera
            server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server.bind((self.host, self.port))
            server.listen()
            logger.info(f"Broker MQTT-Lite nasłuchuje na {self.host}:{self.port}")

            try:
                # Nieskończona pętla akceptująca nowe połączenia
                while True:
                    conn, addr = server.accept()
                    logger.info(f"[{addr}] Nawiązano nowe połączenie TCP")
                    
                    # Dla każdego nowego czujnika tworzymy osobny wątek (daemon = True 
                    # oznacza, że wątki zamkną się same, gdy wyłączymy główny program brokera)
                    client_thread = threading.Thread(
                        target=self._handle_client, 
                        args=(conn, addr),
                        daemon=True
                    )
                    client_thread.start()
                    
            except KeyboardInterrupt:
                logger.info("Zamykanie brokera (Ctrl+C)...")

    def _handle_client(self, conn: socket.socket, addr: tuple) -> None:
        """Metoda działająca w osobnym wątku dla każdego podłączonego klienta."""
        connected = False

        with conn: # Gwarantuje zamknięcie połączenia przy wyjściu z funkcji
            while True:
                try:
                    # 1. Odbiór nagłówka (Fixed Header)
                    header = conn.recv(2)
                    if not header:
                        logger.warning(f"[{addr}] Czujnik rozłączył się.")
                        break
                        
                    msg_type, remaining_length = struct.unpack('!BB', header)

                    # 2. Faza autoryzacji (CONNECT musi być pierwszy)
                    if not connected:
                        if msg_type == self.MSG_CONNECT:
                            logger.info(f"[{addr}] Odebrano CONNECT. Odsyłam CONNACK.")
                            conn.sendall(struct.pack(self.FMT_CONNECT, self.MSG_CONNACK, 0))
                            connected = True
                            continue
                        else:
                            logger.error(f"[{addr}] BŁĄD: Pierwszy pakiet musi być CONNECT! Zrywam połączenie.")
                            break 

                    # 3. Obsługa danych (PUBLISH)
                    if msg_type == self.MSG_PUBLISH:
                        payload = conn.recv(remaining_length)
                        sensor_id, packet_id, temp = struct.unpack(self.FMT_PUBLISH, payload)
                        
                        logger.info(f"[{addr}] [RECV] Czujnik: {sensor_id} | Pakiet: {packet_id} | Temp: {temp:.2f}°C")
                        
                        # Odsyłanie PUBACK
                        ack_frame = struct.pack(self.FMT_PUBACK, self.MSG_PUBACK, 3, packet_id)
                        conn.sendall(ack_frame)
                        logger.info(f"[{addr}] [SEND] PUBACK -> Pakiet: {packet_id}")

                except ConnectionResetError:
                    logger.warning(f"[{addr}] Połączenie brutalnie zerwane przez klienta.")
                    break
                except Exception as e:
                    logger.error(f"[{addr}] Błąd komunikacji: {e}")
                    break

if __name__ == '__main__':
    broker = MQTTBroker()
    broker.start()
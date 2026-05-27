import socket
import struct
import time
import random
import logging

# Konfiguracja profesjonalnego loggera
logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
logger = logging.getLogger("Sensor")

class MQTTSensor:
    # Stałe protokołu MQTT
    MSG_CONNECT = 1
    MSG_CONNACK = 2
    MSG_PUBLISH = 3
    MSG_PUBACK  = 4

    FMT_PUBACK = '!BBH'
    PUBACK_SIZE = struct.calcsize(FMT_PUBACK)

    def __init__(self, sensor_id: int, host: str = '127.0.0.1', port: int = 1883):
        self.sensor_id = sensor_id
        self.host = host
        self.port = port
        self.timeout = 2.0
        self.publish_interval = 3.0
        self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    def start(self) -> None:
        """Inicjuje połączenie i główną pętlę czujnika."""
        try:
            self.client_socket.connect((self.host, self.port))
            self.client_socket.settimeout(self.timeout)
            logger.info(f"Podłączono do brokera na porcie {self.port}")
            
            if self._perform_handshake():
                self._run_publish_loop()

        except ConnectionRefusedError:
            logger.error("Broker nie odpowiada. Upewnij się, że serwer jest włączony.")
        finally:
            self.client_socket.close()
            logger.info("Zakończono działanie czujnika.")

    def _perform_handshake(self) -> bool:
        """Wysyła CONNECT i czeka na CONNACK. Zwraca True, jeśli sukces."""
        logger.info("Wysyłam pakiet CONNECT...")
        self.client_socket.sendall(struct.pack('!BB', self.MSG_CONNECT, 0))
        
        try:
            connack_data = self.client_socket.recv(2)
            if not connack_data:
                return False
                
            ack_type, _ = struct.unpack('!BB', connack_data)
            if ack_type == self.MSG_CONNACK:
                logger.info("Otrzymano CONNACK. Sesja MQTT otwarta.")
                return True
        except socket.timeout:
            logger.error("Brak odpowiedzi na CONNECT (Timeout).")
            
        return False

    def _run_publish_loop(self) -> None:
        """Główna pętla generująca i wysyłająca dane."""
        packet_id = 1
        
        while True:
            temp = round(random.uniform(20.0, 30.0), 2)
            self._publish_with_qos1(packet_id, temp)
            
            packet_id += 1
            time.sleep(self.publish_interval)

    def _publish_with_qos1(self, packet_id: int, temp: float) -> None:
        """Wysyła ramkę PUBLISH i gwarantuje jej dostarczenie (retransmisja)."""
        # Fixed Header (Typ=3, Długość=7), Zmienna reszta (SensorID, PacketID, Temp)
        frame = struct.pack('!BBBHf', self.MSG_PUBLISH, 7, self.sensor_id, packet_id, temp)
        
        while True:
            logger.info(f"[SEND] PUBLISH | Pakiet: {packet_id} | Temp: {temp}°C")
            self.client_socket.sendall(frame)

            try:
                ack_data = self.client_socket.recv(self.PUBACK_SIZE)
                if not ack_data:
                    logger.warning("Broker zamknął połączenie.")
                    break

                ack_type, _, ack_packet_id = struct.unpack(self.FMT_PUBACK, ack_data)

                if ack_type == self.MSG_PUBACK and ack_packet_id == packet_id:
                    logger.info(f"[RECV] PUBACK  | Pakiet {packet_id} dostarczony.")
                    break # Sukces, wychodzimy z pętli retransmisji

            except socket.timeout:
                logger.warning(f"[TIMEOUT] Brak PUBACK dla Pakietu {packet_id}. Ponawiam próbę...")


if __name__ == '__main__':
    sensor = MQTTSensor(sensor_id=6)
    sensor.start()
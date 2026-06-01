import socket
import struct
import time
import random
import logging
import sys

# Konfiguracja profesjonalnego loggera
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
)


class MQTTSensor:
    # Stałe protokołu MQTT
    MSG_CONNECT = 1
    MSG_CONNACK = 2
    MSG_PUBLISH = 3
    MSG_PUBACK = 4
    MSG_SUBSCRIBE = 5
    MSG_SUBACK = 6

    FMT_HEADER = "!BB"
    FMT_PUBACK = "!BBH"

    PUBACK_SIZE = struct.calcsize(FMT_PUBACK)  # 4 bajty
    PUBLISH_PAYLOAD_SIZE = struct.calcsize("!BHf")  # 7 bajtów

    def __init__(self, sensor_id: int, host: str = "127.0.0.1", port: int = 1883):
        self.sensor_id = sensor_id
        self.host = host
        self.port = port
        self.timeout = 2.0
        self.publish_interval = 3.0
        self.logger = logging.getLogger(f"Sensor-{sensor_id}")
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    def start(self) -> None:
        """Inicjuje połączenie i główną pętlę czujnika."""
        try:
            self._sock.connect((self.host, self.port))
            self._sock.settimeout(self.timeout)
            self.logger.info(f"Podłączono do brokera na {self.host}:{self.port}")

            if self._perform_handshake():
                self._run_publish_loop()

        except ConnectionRefusedError:
            self.logger.error(
                "Broker nie odpowiada. Upewnij się, że serwer jest włączony."
            )
        finally:
            self._sock.close()
            self.logger.info("Zakończono działanie czujnika.")

    def _perform_handshake(self) -> bool:
        """Wysyła CONNECT i czeka na CONNACK. Zwraca True jeśli sukces."""
        self.logger.info("Wysyłam pakiet CONNECT...")
        self._sock.sendall(struct.pack(self.FMT_HEADER, self.MSG_CONNECT, 0))

        try:
            data = self._recv_exact(2)
            msg_type, _ = struct.unpack(self.FMT_HEADER, data)
            if msg_type == self.MSG_CONNACK:
                self.logger.info("Otrzymano CONNACK. Sesja MQTT otwarta.")
                return True
        except (socket.timeout, ConnectionError):
            self.logger.error("Brak odpowiedzi na CONNECT (Timeout).")

        return False

    def _run_publish_loop(self) -> None:
        """Główna pętla generująca i wysyłająca dane."""
        packet_id = 1

        while True:
            temp = round(random.uniform(20.0, 30.0), 2)

            if not self._publish_with_qos1(packet_id, temp):
                self.logger.error("Nie można dostarczyć pakietu. Kończę pracę.")
                break

            # Zawijanie packet_id zgodnie ze specyfikacją MQTT (max 65535)
            packet_id = (packet_id % 65535) + 1
            time.sleep(self.publish_interval)

    def _publish_with_qos1(self, packet_id: int, temp: float) -> bool:
        """Wysyła ramkę PUBLISH i gwarantuje dostarczenie (QoS 1). Zwraca False przy błędzie połączenia."""
        frame = struct.pack(
            "!BBBHf",
            self.MSG_PUBLISH,
            self.PUBLISH_PAYLOAD_SIZE,
            self.sensor_id,
            packet_id,
            temp,
        )

        while True:
            self.logger.info(f"[SEND] PUBLISH | Pakiet: {packet_id} | Temp: {temp}°C")
            try:
                self._sock.sendall(frame)
            except OSError:
                self.logger.warning("Nie można wysłać — połączenie zerwane.")
                return False

            try:
                ack_data = self._recv_exact(self.PUBACK_SIZE)
                ack_type, _, ack_packet_id = struct.unpack(self.FMT_PUBACK, ack_data)

                if ack_type == self.MSG_PUBACK and ack_packet_id == packet_id:
                    self.logger.info(
                        f"[RECV] PUBACK  | Pakiet {packet_id} dostarczony."
                    )
                    return True  # Sukces

            except ConnectionError:
                self.logger.warning("Broker zamknął połączenie.")
                return False
            except socket.timeout:
                self.logger.warning(
                    f"[TIMEOUT] Brak PUBACK dla Pakietu {packet_id}. Ponawiam próbę..."
                )

    def _recv_exact(self, n: int) -> bytes:
        """Odczytuje dokładnie n bajtów, chroniąc przed częściowymi odczytami TCP."""
        buf = b""
        while len(buf) < n:
            chunk = self._sock.recv(n - len(buf))
            if not chunk:
                raise ConnectionError("Połączenie zamknięte podczas odbioru danych.")
            buf += chunk
        return buf


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(f"Użycie: python {sys.argv[0]} <sensor_id>")
        sys.exit(1)

    try:
        sid = int(sys.argv[1])
    except ValueError:
        print("Błąd: sensor_id musi być liczbą całkowitą.")
        sys.exit(1)

    sensor = MQTTSensor(sensor_id=sid)
    sensor.start()

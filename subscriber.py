import socket
import struct
import logging
import sys

# Konfiguracja profesjonalnego loggera
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
)


class MQTTSubscriber:
    # Stałe protokołu MQTT
    MSG_CONNECT = 1
    MSG_CONNACK = 2
    MSG_PUBLISH = 3
    MSG_PUBACK = 4
    MSG_SUBSCRIBE = 5
    MSG_SUBACK = 6

    FMT_HEADER = "!BB"
    FMT_PUBLISH = "!BHf"  # sensor_id (B), packet_id (H), temp (f) — 7 bajtów
    FMT_PUBACK = "!BBH"  # type (B), remaining_length (B), packet_id (H)

    PUBACK_REMAINING_LEN = 2  # payload PUBACK = tylko 2-bajtowe packet_id
    PUBLISH_PAYLOAD_SIZE = struct.calcsize(FMT_PUBLISH)  # 7 bajtów

    def __init__(
        self, sensor_filter: int = 0, host: str = "127.0.0.1", port: int = 1883
    ):
        # sensor_filter == 0 oznacza wildcard (wszystkie czujniki)
        self.sensor_filter = sensor_filter
        self.host = host
        self.port = port
        desc = (
            "wszystkie czujniki" if sensor_filter == 0 else f"czujnik {sensor_filter}"
        )
        self.logger = logging.getLogger(f"Subscriber [{desc}]")
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    def start(self) -> None:
        """Inicjuje połączenie, wykonuje handshake i wchodzi w pętlę odbioru."""
        try:
            self._sock.connect((self.host, self.port))
            self._sock.settimeout(5.0)
            self.logger.info(f"Podłączono do brokera na {self.host}:{self.port}")

            if self._perform_handshake() and self._send_subscribe():
                self._sock.settimeout(None)  # Blokujące oczekiwanie na wiadomości
                self._run_receive_loop()

        except ConnectionRefusedError:
            self.logger.error(
                "Broker nie odpowiada. Upewnij się, że serwer jest włączony."
            )
        finally:
            self._sock.close()
            self.logger.info("Zakończono działanie subskrybenta.")

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

    def _send_subscribe(self) -> bool:
        """Wysyła SUBSCRIBE z filtrem i czeka na SUBACK. Zwraca True jeśli sukces."""
        self.logger.info(f"Wysyłam pakiet SUBSCRIBE | Filtr: {self.sensor_filter}")
        # remaining_length = 1 (jeden bajt payloadu: sensor_filter)
        self._sock.sendall(
            struct.pack("!BBB", self.MSG_SUBSCRIBE, 1, self.sensor_filter)
        )
        try:
            data = self._recv_exact(2)
            msg_type, _ = struct.unpack(self.FMT_HEADER, data)
            if msg_type == self.MSG_SUBACK:
                self.logger.info(
                    "Otrzymano SUBACK. Subskrypcja aktywna. Oczekuję na dane..."
                )
                return True
        except (socket.timeout, ConnectionError):
            self.logger.error("Brak odpowiedzi na SUBSCRIBE (Timeout).")
        return False

    def _run_receive_loop(self) -> None:
        """Odbiera przesłane przez brokera pakiety PUBLISH i potwierdza je PUBACK."""
        while True:
            try:
                # Odczyt Fixed Header
                header = self._recv_exact(2)
                msg_type, remaining_length = struct.unpack(self.FMT_HEADER, header)

                if msg_type != self.MSG_PUBLISH:
                    self.logger.warning(
                        f"Nieoczekiwany typ pakietu: {msg_type}. Ignoruję."
                    )
                    continue

                payload = self._recv_exact(remaining_length)
                sensor_id, packet_id, temp = struct.unpack(self.FMT_PUBLISH, payload)
                self.logger.info(
                    f"[RECV] PUBLISH | Czujnik: {sensor_id} | Pakiet: {packet_id} | Temp: {temp:.2f}°C"
                )

                # Potwierdzenie dostarczenia (QoS 1)
                puback = struct.pack(
                    self.FMT_PUBACK,
                    self.MSG_PUBACK,
                    self.PUBACK_REMAINING_LEN,
                    packet_id,
                )
                self._sock.sendall(puback)
                self.logger.info(f"[SEND] PUBACK  | Pakiet {packet_id} potwierdzony.")

            except ConnectionError:
                self.logger.warning("Broker zamknął połączenie.")
                break

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
    if len(sys.argv) > 2:
        print(f"Użycie: python {sys.argv[0]} [sensor_id]")
        print("  Brak argumentu → nasłuchuje wszystkich czujników")
        sys.exit(1)

    try:
        sf = int(sys.argv[1]) if len(sys.argv) == 2 else 0
    except ValueError:
        print("Błąd: sensor_id musi być liczbą całkowitą.")
        sys.exit(1)

    sub = MQTTSubscriber(sensor_filter=sf)
    sub.start()

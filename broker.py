import socket
import struct
import logging
import threading
import queue

# Konfiguracja profesjonalnego loggera
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger("Broker")


class MQTTBroker:
    # Stałe protokołu MQTT
    MSG_CONNECT = 1
    MSG_CONNACK = 2
    MSG_PUBLISH = 3
    MSG_PUBACK = 4
    MSG_SUBSCRIBE = 5
    MSG_SUBACK = 6

    FMT_HEADER = "!BB"
    FMT_PUBLISH = "!BHf"  # sensor_id (B), packet_id (H), temp (f)
    FMT_PUBACK = "!BBH"  # type (B), remaining_length (B), packet_id (H)

    PUBLISH_PAYLOAD_SIZE = struct.calcsize(FMT_PUBLISH)  # 7 bajtów

    def __init__(self, host: str = "127.0.0.1", port: int = 1883):
        self.host = host
        self.port = port
        # Każdy subskrybent to krotka (kolejka, filtr).
        # filtr == 0 oznacza wildcard (wszystkie czujniki).
        self._subscribers: list[tuple[queue.Queue, int]] = []
        self._sub_lock = threading.Lock()

    def start(self) -> None:
        """Uruchamia serwer i nasłuchuje połączeń w głównej pętli."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
            server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server.bind((self.host, self.port))
            server.listen()
            logger.info(f"Broker MQTT-Lite nasłuchuje na {self.host}:{self.port}")
            try:
                while True:
                    conn, addr = server.accept()
                    logger.info(f"[{addr}] Nawiązano nowe połączenie TCP")
                    threading.Thread(
                        target=self._handle_client, args=(conn, addr), daemon=True
                    ).start()
            except KeyboardInterrupt:
                logger.info("Zamykanie brokera (Ctrl+C)...")

    # ── Pomocnicze ────────────────────────────────────────────────────────────

    def _recv_exact(self, conn: socket.socket, n: int) -> bytes:
        """Odczytuje dokładnie n bajtów, chroniąc przed częściowymi odczytami TCP."""
        buf = b""
        while len(buf) < n:
            chunk = conn.recv(n - len(buf))
            if not chunk:
                raise ConnectionError("Połączenie zamknięte podczas odbioru danych.")
            buf += chunk
        return buf

    def _forward_to_subscribers(
        self, frame: bytes, packet_id: int, sensor_id: int
    ) -> None:
        """Wrzuca ramkę PUBLISH do kolejki subskrybentów pasujących do sensor_id."""
        with self._sub_lock:
            # Budujemy listę celów wewnątrz locka, żeby lista była spójna,
            # ale samo q.put() robimy już poza nim.
            targets = [q for q, f in self._subscribers if f == 0 or f == sensor_id]

        for q in targets:
            q.put(frame)

        if targets:
            logger.info(
                f"[FORWARD] PUBLISH | Pakiet: {packet_id} → {len(targets)} subskrybent(ów)"
            )

    # ── Dispatcher klienta ────────────────────────────────────────────────────

    def _handle_client(self, conn: socket.socket, addr: tuple) -> None:
        """Punkt wejścia dla każdego klienta. Obsługuje handshake i wyznacza rolę."""
        with conn:
            try:
                # Faza 1: CONNECT (obowiązkowy pierwszy pakiet)
                header = self._recv_exact(conn, 2)
                msg_type, _ = struct.unpack(self.FMT_HEADER, header)

                if msg_type != self.MSG_CONNECT:
                    logger.error(
                        f"[{addr}] Oczekiwano CONNECT, otrzymano typ {msg_type}. Zrywam."
                    )
                    return

                conn.sendall(struct.pack(self.FMT_HEADER, self.MSG_CONNACK, 0))
                logger.info(f"[{addr}] CONNECT → CONNACK. Sesja otwarta.")

                # Faza 2: Identyfikacja roli na podstawie pierwszego pakietu po CONNECT
                header = self._recv_exact(conn, 2)
                msg_type, remaining_length = struct.unpack(self.FMT_HEADER, header)

                if msg_type == self.MSG_SUBSCRIBE:
                    self._handle_as_subscriber(conn, addr, remaining_length)
                elif msg_type == self.MSG_PUBLISH:
                    self._handle_as_sensor(conn, addr, remaining_length)
                else:
                    logger.error(
                        f"[{addr}] Nieznany typ pakietu po CONNECT: {msg_type}. Zrywam."
                    )

            except ConnectionError as e:
                logger.warning(f"[{addr}] Klient rozłączył się: {e}")
            except Exception as e:
                logger.error(f"[{addr}] Nieoczekiwany błąd: {e}")

    # ── Obsługa czujnika ──────────────────────────────────────────────────────

    def _handle_as_sensor(
        self, conn: socket.socket, addr: tuple, remaining_length: int
    ) -> None:
        """Przetwarza strumień pakietów PUBLISH od zidentyfikowanego czujnika."""
        logger.info(f"[{addr}] Rola: CZUJNIK")
        while True:
            try:
                payload = self._recv_exact(conn, remaining_length)
                sensor_id, packet_id, temp = struct.unpack(self.FMT_PUBLISH, payload)
                logger.info(
                    f"[{addr}] [RECV] Czujnik: {sensor_id} | Pakiet: {packet_id} | Temp: {temp:.2f}°C"
                )

                # PUBACK do czujnika (remaining_length=2: samo pole packet_id)
                conn.sendall(
                    struct.pack(self.FMT_PUBACK, self.MSG_PUBACK, 2, packet_id)
                )
                logger.info(f"[{addr}] [SEND] PUBACK → Pakiet: {packet_id}")

                # Przekazanie do pasujących subskrybentów
                fwd = struct.pack(
                    "!BBBHf",
                    self.MSG_PUBLISH,
                    self.PUBLISH_PAYLOAD_SIZE,
                    sensor_id,
                    packet_id,
                    temp,
                )
                self._forward_to_subscribers(fwd, packet_id, sensor_id)

                # Odczyt nagłówka następnego pakietu
                header = self._recv_exact(conn, 2)
                msg_type, remaining_length = struct.unpack(self.FMT_HEADER, header)
                if msg_type != self.MSG_PUBLISH:
                    logger.warning(f"[{addr}] Nieoczekiwany typ: {msg_type}. Zrywam.")
                    break

            except ConnectionError:
                logger.warning(f"[{addr}] Czujnik rozłączył się.")
                break
            except struct.error as e:
                logger.error(f"[{addr}] Błąd dekodowania ramki: {e}")
                break

    # ── Obsługa subskrybenta ──────────────────────────────────────────────────

    def _handle_as_subscriber(
        self, conn: socket.socket, addr: tuple, remaining_length: int
    ) -> None:
        """Rejestruje subskrybenta z filtrem, uruchamia wątek nadawczy i odbiera PUBACKi."""
        payload = self._recv_exact(conn, remaining_length)
        (sensor_filter,) = struct.unpack("!B", payload)

        desc = (
            "wszystkie czujniki" if sensor_filter == 0 else f"czujnik {sensor_filter}"
        )
        logger.info(f"[{addr}] Rola: SUBSKRYBENT | Filtr: {desc}")

        q: queue.Queue = queue.Queue()

        # Rejestracja przed SUBACK — żeby nie stracić wiadomości opublikowanych
        # w oknie między wysłaniem SUBACK a faktycznym dodaniem do listy
        with self._sub_lock:
            self._subscribers.append((q, sensor_filter))

        conn.sendall(struct.pack(self.FMT_HEADER, self.MSG_SUBACK, 0))

        sender = threading.Thread(
            target=self._subscriber_sender, args=(conn, q), daemon=True
        )
        sender.start()

        puback_size = struct.calcsize(self.FMT_PUBACK)
        try:
            while True:
                data = self._recv_exact(conn, puback_size)
                msg_type, _, packet_id = struct.unpack(self.FMT_PUBACK, data)
                if msg_type == self.MSG_PUBACK:
                    logger.info(
                        f"[{addr}] [RECV] PUBACK od subskrybenta | Pakiet: {packet_id}"
                    )
        except ConnectionError:
            logger.warning(f"[{addr}] Subskrybent rozłączył się.")
        finally:
            q.put(None)  # „Trucizna" — zatrzymuje wątek nadawczy
            with self._sub_lock:
                self._subscribers.remove((q, sensor_filter))

    @staticmethod
    def _subscriber_sender(conn: socket.socket, q: queue.Queue) -> None:
        """Wątek nadawczy: wyjmuje ramki z kolejki i wysyła je do subskrybenta."""
        while True:
            frame = q.get()
            if frame is None:  # Sygnał zakończenia
                break
            try:
                conn.sendall(frame)
            except Exception:
                break


if __name__ == "__main__":
    broker = MQTTBroker()
    broker.start()

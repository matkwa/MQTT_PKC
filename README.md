# MQTT-Lite — Własny Protokół Komunikacyjny

Projekt zaliczeniowy z przedmiotu **Protokoły Komunikacji Cyfrowej**  
*(specjalność: Komputerowe Systemy Sterowania)*

Projekt implementuje autorski, lekki protokół komunikacyjny wzorowany na architekturze MQTT, obsługujący przesyłanie danych z czujników do stacji centralnej (Brokera) oraz ich dystrybucję do zainteresowanych subskrybentów, z mechanizmem potwierdzeń odpowiadającym MQTT QoS 1 (*At least once*).

---

## 📌 Opis Projektu

System składa się z trzech niezależnych aplikacji komunikujących się ze sobą w architekturze **Publish–Subscribe** przy użyciu gniazd sieciowych (TCP/IPv4):

- **Wirtualny Czujnik (`sensor.py`):** Generuje symulowane dane temperaturowe i wysyła je do Brokera. Implementuje mechanizm weryfikacji dostarczenia pakietu (Timeout) oraz automatycznej retransmisji przy braku potwierdzenia (QoS 1).

- **Broker (`broker.py`):** Serwer centralny obsługujący wielu klientów jednocześnie w osobnych wątkach. Odbiera dane od czujników, potwierdza ich odbiór (PUBACK), a następnie przekazuje je do zarejestrowanych subskrybentów z uwzględnieniem filtrów.

- **Subskrybent (`subscriber.py`):** Rejestruje się w brokerze i odbiera dane z wybranych czujników. Obsługuje filtrowanie po `sensor_id` — może nasłuchiwać jednego konkretnego czujnika lub wszystkich jednocześnie (wildcard).

Całość napisana bez zewnętrznych bibliotek (np. `paho-mqtt`), opierając się wyłącznie na surowej manipulacji bajtami (moduł `struct`) i standardowej bibliotece Pythona.

---

## 🛠 Wymagania

- Python 3.9 lub nowszy
- Wyłącznie biblioteki standardowe: `socket`, `struct`, `threading`, `queue`, `time`, `random`, `logging`, `sys`

---

## 📦 Protokół — Format Ramek

Protokół przesyła dane w postaci czysto binarnej w standardzie sieciowym (*Network Byte Order* — Big Endian, prefiks `!` w `struct`).

Każda ramka zaczyna się od **Fixed Header** (2 bajty): typ pakietu i długość payloadu (`remaining_length`).

### Typy pakietów

| Wartość | Nazwa       | Kierunek                        |
| :-----: | :---------- | :------------------------------ |
| `1`     | CONNECT     | Czujnik / Subskrybent → Broker  |
| `2`     | CONNACK     | Broker → Czujnik / Subskrybent  |
| `3`     | PUBLISH     | Czujnik → Broker → Subskrybent  |
| `4`     | PUBACK      | Broker → Czujnik / Subskrybent → Broker |
| `5`     | SUBSCRIBE   | Subskrybent → Broker            |
| `6`     | SUBACK      | Broker → Subskrybent            |

---

### 1. CONNECT — `!BB` — 2 bajty

| Bajty | Typ            | Wartość | Opis                     |
| :---: | :------------- | :-----: | :----------------------- |
| `0`   | `uint8` (1B)   | `1`     | Typ pakietu: CONNECT     |
| `1`   | `uint8` (1B)   | `0`     | Remaining length (brak payloadu) |

---

### 2. CONNACK — `!BB` — 2 bajty

| Bajty | Typ            | Wartość | Opis                     |
| :---: | :------------- | :-----: | :----------------------- |
| `0`   | `uint8` (1B)   | `2`     | Typ pakietu: CONNACK     |
| `1`   | `uint8` (1B)   | `0`     | Return code (0 = sukces) |

---

### 3. PUBLISH — `!BBBHf` — 9 bajtów

| Bajty | Typ            | Opis                                   |
| :---: | :------------- | :------------------------------------- |
| `0`   | `uint8` (1B)   | Typ pakietu: PUBLISH (`3`)             |
| `1`   | `uint8` (1B)   | Remaining length (`7`)                 |
| `2`   | `uint8` (1B)   | **ID Czujnika** (sensor_id)            |
| `3–4` | `uint16` (2B)  | **Numer pakietu** (packet_id, 1–65535) |
| `5–8` | `float` (4B)   | **Temperatura** [°C]                   |

Używany w obu kierunkach: czujnik → broker oraz broker → subskrybent.

---

### 4. PUBACK — `!BBH` — 4 bajty

| Bajty | Typ            | Opis                                   |
| :---: | :------------- | :------------------------------------- |
| `0`   | `uint8` (1B)   | Typ pakietu: PUBACK (`4`)              |
| `1`   | `uint8` (1B)   | Remaining length (`2`)                 |
| `2–3` | `uint16` (2B)  | **Numer pakietu** do potwierdzenia     |

Używany w obu kierunkach: broker → czujnik (potwierdzenie odbioru) oraz subskrybent → broker (potwierdzenie dostarczenia).

---

### 5. SUBSCRIBE — `!BBB` — 3 bajty

| Bajty | Typ            | Opis                                          |
| :---: | :------------- | :-------------------------------------------- |
| `0`   | `uint8` (1B)   | Typ pakietu: SUBSCRIBE (`5`)                  |
| `1`   | `uint8` (1B)   | Remaining length (`1`)                        |
| `2`   | `uint8` (1B)   | **Filtr** (`0` = wszystkie, `N` = czujnik N)  |

---

### 6. SUBACK — `!BB` — 2 bajty

| Bajty | Typ            | Wartość | Opis                     |
| :---: | :------------- | :-----: | :----------------------- |
| `0`   | `uint8` (1B)   | `6`     | Typ pakietu: SUBACK      |
| `1`   | `uint8` (1B)   | `0`     | Remaining length (brak payloadu) |

---

## 🔄 Przebieg Komunikacji

```
CZUJNIK            BROKER              SUBSKRYBENT
   |                  |                     |
   |── CONNECT ───────►                     |
   |◄── CONNACK ───────                     |
   |                  |◄──── CONNECT ───────|
   |                  |───── CONNACK ──────►|
   |                  |◄──── SUBSCRIBE ─────|
   |                  |───── SUBACK ───────►|
   |                  |                     |
   |── PUBLISH ───────►                     |
   |◄── PUBACK ────────|──── PUBLISH ──────►|
   |                  |◄─── PUBACK ─────────|
   |── PUBLISH ───────►  (retransmisja      |
   |◄── PUBACK ────────   jeśli timeout)    |
```

**QoS 1 (At least once):** Czujnik wysyła pakiet i czeka na PUBACK. Jeśli odpowiedź nie nadejdzie w ciągu 2 sekund (timeout), pakiet jest wysyłany ponownie z tym samym `packet_id`. Pętla trwa do skutku.

---

## 🚀 Uruchomienie

Otwórz osobne terminale dla każdego komponentu. Broker musi być uruchomiony jako pierwszy.

**Terminal 1 — Broker:**
```bash
python broker.py
```

**Terminal 2 i 3 — Czujniki** (każdy z unikalnym `sensor_id`):
```bash
python sensor.py 5
python sensor.py 6
```

**Terminal 4 — Subskrybent** (opcjonalny argument filtruje po `sensor_id`):
```bash
python subscriber.py      # nasłuchuje wszystkich czujników
python subscriber.py 5    # nasłuchuje tylko czujnika nr 5
python subscriber.py 6    # nasłuchuje tylko czujnika nr 6
```

---

## 🏗 Architektura Techniczna

### Wielowątkowość w brokerze

Broker obsługuje każdego klienta w dedykowanym wątku (`threading.Thread`). Dostęp do współdzielonej listy subskrybentów jest chroniony przez `threading.Lock`.

### Bezpieczne wysyłanie do subskrybentów

Każdy subskrybent posiada własną kolejkę (`queue.Queue`). Wątki czujników wrzucają ramki do kolejek (operacja thread-safe), a dedykowany wątek nadawczy per-subskrybent jest jedynym pisarzem na dane gniazdo — co eliminuje ryzyko przeplatania bajtów przy równoczesnych publikacjach.

```
Czujnik-5 (wątek) ──┐
                     ├──► queue.put() ──► wątek nadawczy ──► socket.sendall()
Czujnik-6 (wątek) ──┘
```

### Ochrona przed częściowym odczytem TCP

TCP jest protokołem strumieniowym — pojedyncze wywołanie `recv(n)` może zwrócić mniej niż `n` bajtów. Wszystkie trzy moduły używają funkcji `_recv_exact(n)`, która pętli się aż do zebrania dokładnie `n` bajtów.

---

## 📁 Struktura Projektu

```
MQTT_PKC/
├── broker.py        # Serwer centralny (Broker)
├── sensor.py        # Klient — wirtualny czujnik
├── subscriber.py    # Klient — subskrybent danych
└── README.md
```
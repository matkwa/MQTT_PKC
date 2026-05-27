# Własny Protokół Komunikacyjny (MQTT-Lite)

Projekt zaliczeniowy z przedmiotu Protokoły Komunikacji Cyfrowej (specjalność: Komputerowe Systemy Sterowania). 
Projekt implementuje autorski, lekki protokół komunikacyjny wzorowany na architekturze MQTT, obsługujący przesyłanie danych z czujników do stacji centralnej (Brokera) z mechanizmem potwierdzeń (odpowiednik MQTT QoS 1 - *At least once*).

## 📌 Opis Projektu

System składa się z dwóch niezależnych aplikacji komunikujących się ze sobą w architekturze Klient-Serwer przy użyciu gniazd sieciowych (TCP/IPv4):
* **Wirtualny Czujnik (`sensor.py`):** Działa jako klient. Generuje symulowane dane (temperaturę) i wysyła je do serwera. Posiada zaimplementowany mechanizm weryfikacji dostarczenia pakietu (Timeout) oraz automatycznej retransmisji w przypadku braku potwierdzenia.
* **Uproszczony Broker (`broker.py`):** Działa jako serwer. Odbiera ramki z danymi od czujników, dekoduje je i natychmiast odsyła ramkę potwierdzającą (PUBACK).

Całość została napisana bez użycia zewnętrznych bibliotek (np. `paho-mqtt`), opierając się wyłącznie na surowej manipulacji bajtami (biblioteka `struct`).

---

## 🛠 Wymagania
* Python 3.6 lub nowszy
* Biblioteki standardowe: `socket`, `struct`, `time`, `random` (nie wymagają instalacji)

---

## 📦 Struktura Ramki Danych (Kontrakt)

Protokół przesyła dane w postaci czysto binarnej. Format danych ustalono w oparciu o standard sieciowy (*Network Byte Order* - Big Endian).

### 1. Ramka PUBLISH (Czujnik -> Broker)
Rozmiar: **8 bajtów** | Format struct: `!BBHf`

| Bajt | Typ (C) | Opis |
| :--- | :--- | :--- |
| `0` | `unsigned char` (1B) | **Typ wiadomości:** `0x01` (PUBLISH) |
| `1` | `unsigned char` (1B) | **ID Czujnika:** Unikalny numer (np. `0x05`) |
| `2-3` | `unsigned short` (2B)| **Numer pakietu:** Zwiększany z każdą wiadomością |
| `4-7` | `float` (4B) | **Dane pomiarowe:** Wartość temperatury zmiennoprzecinkowa |

### 2. Ramka PUBACK (Broker -> Czujnik)
Rozmiar: **4 bajty** | Format struct: `!BBH`

| Bajt | Typ (C) | Opis |
| :--- | :--- | :--- |
| `0` | `unsigned char` (1B) | **Typ wiadomości:** `0x02` (PUBACK) |
| `1` | `unsigned char` (1B) | **ID Czujnika:** ID czujnika, którego dotyczy potwierdzenie |
| `2-3` | `unsigned short` (2B)| **Numer pakietu:** Numer odebranego pakietu (do weryfikacji) |

---

## 🚀 Jak uruchomić projekt

1. Sklonuj repozytorium na swój komputer.
2. Otwórz terminal w folderze projektu i uruchom stację centralną (Brokera):
   ```bash
   python broker.py
# Mini Data Warehouse For E-commerce with Apache Airflow
[![en](https://img.shields.io/badge/lang-en-red.svg)](https://github.com/admczyk/Mini-Data-Warehouse-for-E-commerce-With-Apache-Airflow/blob/main/README.md)
[![pl](https://img.shields.io/badge/lang-pl-green.svg)](https://github.com/admczyk/Mini-Data-Warehouse-for-E-commerce-With-Apache-Airflow/blob/main/README.pl.md)

Projekt ten jest kompeltnym pipeline ETL (Extract, Transform, Load), który pobiera dane o produktach, koszykach i użytkownikach ze strony **dummyjson.com**, przetwarza je i ładuje do bazy danych PostgreSQL. Pipeline jest zarządzany przez Apache Airflow. Przetworzone dane są następnie przechowywane w bazie danych PostgreSQL, a następnie analizowane i wizualizowane za pomocą Power BI.

## Spis Treści
* [Przegląd Projektu](https://github.com/admczyk/Mini-Data-Warehouse-for-E-commerce-With-Apache-Airflow#przegląd-projektu)
* [Struktura Projektu](https://github.com/admczyk/Mini-Data-Warehouse-for-E-commerce-With-Apache-Airflow#struktura-projektu)
* [Wykorzystane Technologie](https://github.com/admczyk/Mini-Data-Warehouse-for-E-commerce-With-Apache-Airflow#wykorzystane-technologie)
* [Architektura Przepływu Danych](https://github.com/admczyk/Mini-Data-Warehouse-for-E-commerce-With-Apache-Airflow#architektura-przepływu-danych)
    - [Pipeline ETL](https://github.com/admczyk/Mini-Data-Warehouse-for-E-commerce-With-Apache-Airflow#pipeline-etl)
    - [Projekt Bazy Danych](https://github.com/admczyk/Mini-Data-Warehouse-for-E-commerce-With-Apache-Airflow#projekt-bazy-danych)
    - [DAGi Airflow](https://github.com/admczyk/Mini-Data-Warehouse-for-E-commerce-With-Apache-Airflow#dagi-airflow)
* [Analiza Danych i Wizualizacja](https://github.com/admczyk/Mini-Data-Warehouse-for-E-commerce-With-Apache-Airflow#analiza-danych-i-wizualizacja)
    - [Dashboard Koszyków](https://github.com/admczyk/Mini-Data-Warehouse-for-E-commerce-With-Apache-Airflow#dashboard-koszyków)
    - [Dashboard Produktów](https://github.com/admczyk/Mini-Data-Warehouse-for-E-commerce-With-Apache-Airflow#dashboard-produktów)
    - [Dashboard Użytkowników](https://github.com/admczyk/Mini-Data-Warehouse-for-E-commerce-With-Apache-Airflow#dashboard-użytkowników)
* [Instrukcja Uruchomienia](https://github.com/admczyk/Mini-Data-Warehouse-for-E-commerce-With-Apache-Airflow#instrukcja-uruchomienia)

## Przegląd Projektu
Projekt implementuje mini hurtownię danych dla danych e-commerce z wykorzystaniem Apache Airflow.

Pipeline pobiera dane produktowe z publicznego API, przekształca je do formatu analitycznego i ładuje do bazy danych PostgreSQL.

Zestaw danych obejmuje:

#### Produkty
- **informacje o produkcie** (product_id, title, description, category, price, discount_percentage, overall_rating, stock, brand, final_price, price_bucket, rating_bucket, value_score)
- **recenzje produktów** (review_id, product_id, review_comment, review_date, rating, review_year, review_month, review_bucket)
- **tagi produktów** (tag_id, product_id, tag)
#### Koszyki
- **informacje o koszykach** (cart_id, cart_total, cart_total_discounted, user_id, cart_total_quantity, cart_size_bucket_cart_value_bucket)
- **elementy koszyka** (cart_id, product_id, product_price, product_quantity, product_total, product_total_discounted)
#### Użytkownicy
- informacje o użytkownikach (user_id, first_name, last_name, gender, birth_date, city, state, state_code, postal_code, country, age_group)

Głównym celem projektu jest symulacja rzeczywistego workflow data engineeringowego, w tym:
- projektowanie pipeline’u ETL
- czyszczenie i walidacja danych
- feature engineering na potrzeby analityki
- zaaranżowanie workflow przy użyciu Apache Airflow

Dodatkowym celem jest analiza pozyskanych danych w celu wydobycia istotnych wniosków oraz wsparcia podejmowania decyzji opartych na danych.

## Struktura Projektu
```
mini_data_warehouse/
|
├── dags/                               # Zawiera DAGi Apache Airflow odpowiedzialne za orkiestrację workflow
|   ├── create_table.py                 # Inicjalizuje schemat bazy danych i tworzy tabele
|   ├── etl_carts.py                   # Definiuje workflow ETL dla danych koszyków
|   └── etl_products_users.py          # Definiuje workflow ETL dla danych produktów i użytkowników
├── data/                               # Przechowuje dane używane w pipeline
|   ├── raw                            # Surowe dane pobrane z API (format JSON)
|   └── transformed                    # Oczyszczone i przetworzone dane gotowe do załadowania (CSV)
├── etl/                                # Implementuje logikę ETL
|   ├── transform/                      # Obsługuje logikę transformacji danych
|   |   ├── carts_products_transform.py     # Transformuje dane elementów koszyka
|   |   ├── carts_transform.py              # Transformuje dane koszyków
|   |   ├── products_reviews_transform.py   # Transformuje dane recenzji produktów
|   |   ├── products_tags_transform.py      # Transformuje dane tagów produktów
|   |   ├── products_transform.py           # Transformuje dane produktów
|   |   └── users_transform.py              # Transformuje dane użytkowników
|   ├── extract.py                      # Pobiera dane z zewnętrznych źródeł
|   └── load.py                         # Ładuje dane do bazy PostgreSQL
├── sql/                                # Przechowuje zapytania SQL
|   ├── create/                         # Zapytania tworzące tabele
|   └── insert/                         # Zapytania wstawiające dane
├── utils/
|   └── files_io.py                     # Funkcje pomocnicze do obsługi plików
├── .env                                # Zmienne środowiskowe
├── docker-compose.yaml                 # Definicja środowiska (Airflow + PostgreSQL)
├── key_generator.py                    # Generuje FERNET_KEY i SECRET_KEY
├── requirements.txt                    # Lista zależności Pythona
└── README.md                           # Dokumentacja projektu
```

## Wykorzystane Technologie
- SQL
- PostgreSQL
- Python (ETL scripts)
- Apache Airflow
- Power BI
- Docker

## Architektura Przepływu Danych
### Pipeline ETL
Proces ETL składa się z:

#### 1. Extract
- Pobiera dane przy użyciu zapytań HTTP
- Pobiera surowe dane z zewnętrznego API
- Zapisuje dane jako JSON (dla śledzalności)

#### 2. Transform
- Dane są odczytywane z pliku JSON przy użyciu Pandas
- Zagnieżdżone struktury są spłaszczane
- Dane są normalizowane
- Obsługiwane są brakujące wartości i usuwane niespójności logiczne
- Tworzone są dodatkowe kolumny analityczne
- Dane zapisywane są jako CSV

#### Load
- Dane są odczytywane z CSV przy użyciu Pandas
- Połączenie realizowane przez Airflow PostgresHook
- Dane są wstawiane do tabeli w PostgreSQL

### Projekt Bazy Danych
Ta część projektu jest nadal w trakcie budowy...  Wróć tutaj później :]

### DAGi Airflow
Projekt wykorzystuje Apache Airflow do zaaranżowania workflow danych za pomocą DAGów (Directed Acyclic Graphs). Każdy DAG definiuje sekwencję zadań oraz ich zależności, umożliwiając automatyczne i powtarzalne przetwarzanie danych.
- __create_table.py__ – odpowiada za inicjalizację schematu bazy danych
- __etl_carts.py__ – główny DAG realizujący pipeline ETL dla koszyków (uruchamiany co godzinę)
- __etl_products_users.py__ – główny DAG realizujący pipeline ETL dla produktów i użytkowników (uruchamiany raz dziennie)

## Analiza Danych i Wizualizacja
W tej sekcji przedstawiono interaktywne rozwiązanie analityczne zbudowane w Power BI. Zostało ono zaprojektowane w celu analizy zachowań klientów, wyników produktów oraz dynamiki koszyków zakupowych w środowisku e-commerce.
Dashboard został podzielony na trzy główne widoki analityczne: **Analiza Koszyków**, **Analiza Produktów** oraz **Analiza Użytkowników**.
> **Ważna informacja:**
> Ze względu na brak danych czasowych w wyodrębnionym zbiorze dancyh, analiza trendów w czasie nie została wykonana.

### Dashboard Koszyków
![Dashboard Koszyków](https://github.com/admczyk/Mini-Data-Warehouse-for-E-commerce-With-Apache-Airflow/blob/main/img/Carts_Dashboard_IMG.png)
Widok koszyków koncentruje się na analizie zachowań zakupowych na poziomie koszyka, dostarczając informacji o jego strukturze, rozkładzie wartości oraz ogólnej wydajności sprzedaży.
#### Kluczowe metryki:
- **Całkowita Sprzedaż** - łączny przychód wygenerowany ze wszystkich koszyków
- **Średnia Wartość Koszyka**
- **Średnia Liczba Produktów w Koszyku**
- **Łączny Wpływ Rabatów** - porównanie sprzedaży brutto i netto
#### Kluczowe analizy:
- **Wykres Punktowy Wartość Koszyka vs Liczba produktów** - pokazuje zależność między liczbą produktów w koszyku a jego wartością, wskazując wartości odstające
- **Rozkład Wielkości Koszyków** - segmentacja koszyków według wielkości
- **Rozkład Wartości Koszyków** - analiza koncentracji przychodów w różnych przedziałach wartości
- **Top 10 Koszyków według Sprzedaży** - identyfikacja transakcji o najwyższej wartości

### Dashboard Produktów
![Dashboard Produktów](https://github.com/admczyk/Mini-Data-Warehouse-for-E-commerce-With-Apache-Airflow/blob/main/img/Products_Dashboard_IMG.png)
Widok produktów koncentruje się na analizie wydajności produktów, ocen klientów oraz rentowności w podziale na kategorie i podkategorie.
#### Kluczowe metryki:
- **Łączna Liczba Sprzedanych Jednostek**
- **Liczba Unikalnych Produktów**
- **Średnia Ocena**
- **Średnia Wartość Koszyka**
#### Kluczowe analizy:
- **Wykres Punktowy Ocena vs Sprzedaż** - ocena zależności między ocenami produktów a ich sprzedażą
- **Top 10 Produktów według Sprzedaży** - najlepiej sprzedające się produkty pod względem przychodu
- **Zysk według Podkategorii** - analiza rentowności w podziale na kategorie produktów

### Dashboard Użytkowników
![Dashboard Użytkowników](https://github.com/admczyk/Mini-Data-Warehouse-for-E-commerce-With-Apache-Airflow/blob/main/img/Users_Dashboard_IMG.png)
Widok użytkowników koncentruje się na liczbie klientów, ich zachowaniach, strukturze wiekowej oraz rozkładzie płci.
#### Kluczowe metryki:
- **Łączna Liczba Użytkowników**
- **Średni Przychód na Użytkownika**
- **Średnia Liczba Zamówień na Użytkownika**
- **Aktywni Użytkownicy**
#### Kluczowe analizy:
- **Mapa Sprzedaży według Stanów** - geograficzny rozkład użytkowników w poszczególnych stanach USA
- **Rozkład Grup Wiekowych** - podział użytkowników na grupy wiekowe
- **Rozkład Grup Płci** - analiza struktury użytkowników ze względu na płeć
- **Top 10 Klientów** - najlepsi klienci pod względem łącznej sprzedaży


## Instrukcja uruchomienia
#### 1. Sklonuj repozytorium
```
git clone https://github.com/admczyk/Mini-Data-Warehouse-for-E-commerce-With-Apache-Airflow.git
cd Mini-Data-Warehouse-for-E-commerce-With-Apache-Airflow
```
#### 2. Stwórz środowisko wirtualne
Upewnij się, że masz Pythona, następnie stwórz i aktywuj środowisko witrualne:
```
python -m venv venv
source venv/bin/activate    # On macOS/Linux
venv\Scripts\activate       # On Windows
```
#### 3. Zainstaluj zależności
Zainstaluj wymagane biblioteki Pythona:
```
pip install -r requirements.txt
```
#### 4. Stwórz zmienne środowiskowe
Stwórz plik `.env` w głownym folderze projektu i przypisz następujące wartości:
```
POSTGRES_USERNAME= 
POSTGRES_PASSWORD=
POSTGRES_HOST=
POSTGRES_PORT=
POSTGRES_DATABASE_NAME=

AIRFLOW_UID=                            # 1000 on macOS/Linux or 50000 on Windows
AIRFLOW_WWW_USER_USERNAME=
AIRFLOW_WWW_USER_PASSWORD=
FERNET_KEY=
SECRET_KEY=

WEBSITE_PATH="https://dummyjson.com/"
```
`FERNET_KEY` oraz `SECRET_KEY` powinny zostać wygenerowane. Możesz do tego wykorzystać plik `key_generator.py` zawarty w projekcie.

#### 5. Uruchomienie środowiska
Uruchom Docker compose aby włączyć Airflow i PostgreSQL
```
docker compose up -d
```
Ta komenda aktywuje:
- Airflow webserver
- Airflow scheduler
- PostgreSQL database

#### 6. Dostęp do Airflow UI
Otwórz przeglądarkę i przejdź pod adres:
```
http://localhost:8080
```
lub _(jeśli powyższy nie działa)_:
```
http://127.0.0.1:8080
```
Zaloguj się przy użyciu nazwy użytkownika i hasła ustawionego w zmiennych środowiskowych.

#### 7. Uruchomienie pipeline
1. Włącz DAG `create_product_table`
2. Uruchom go ręcznie aby stworzyć tabele SQL
3. Włącz DAG `etl_carts` oraz `etl_products_users`
4. Uruchom je ręcznie lub poczekaj na scheduler

#### 8. Verify Results
- Dane surowe - `data/raw/`
- Dane przetworzone - `data/transformed/`
- Tabela w bazie danych:
    ```
    docker exec -it postgres bash

    psql -U YOUR_POSTGRES_USERNAME -d YOUR_POSTGRES_DATABASE

    SELECT * FROM products LIMIT(10);
    ```
### Zatrzymanie środowiska
Aby zatrzymać wszystkie aktywowane serwisy wywołaj komendę:
```
docker compose down
```
### Reset środowiska
Aby usunąć wszelkie dane i zacząć z świeżym środowiskiem wywołaj komendę:
```
docker compose down -v
```
## Możliwe usprawnienia do późniejszej implementacji
- Dodanie logowania dla lepszego monitoroeania i debugowania
- Integracja z narzędziami BI (Power BI / Tableau)
- Wdrożenie pipeline'u do chmury

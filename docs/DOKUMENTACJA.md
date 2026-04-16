# Dokumentacja Techniczna - TO DB Converter

---

## Spis Treści

1. [Wprowadzenie](#1-wprowadzenie)
   - [1.1. Opis problemu](#11-opis-problemu)
   - [1.2. Cel projektu](#12-cel-projektu)
   - [1.3. Zakres funkcjonalny](#13-zakres-funkcjonalny)

2. [Specyfikacja Wymagań](#2-specyfikacja-wymagań)
   - [2.1. Wymagania funkcjonalne](#21-wymagania-funkcjonalne)
   - [2.2. Wymagania niefunkcjonalne](#22-wymagania-niefunkcjonalne)

3. [Architektura Systemu](#3-architektura-systemu)
   - [3.1. Przegląd architektury](#31-przegląd-architektury)
   - [3.2. Komponenty systemu](#32-komponenty-systemu)
   - [3.3. Przepływ danych](#33-przepływ-danych)

4. [Projekt Implementacyjny](#4-projekt-implementacyjny)
   - [4.1. Struktura projektu](#41-struktura-projektu)
   - [4.2. Klasy i interfejsy](#42-klasy-i-interfejsy)
   - [4.3. Algorytmy transformacji](#43-algorytmy-transformacji)
   - [4.4. Obsługa błędów](#44-obsługa-błędów)

5. [Konfiguracja i Uruchomienie](#5-konfiguracja-i-uruchomienie)
   - [5.1. Wymagania systemowe](#51-wymagania-systemowe)
   - [5.2. Instalacja](#52-instalacja)
   - [5.3. Konfiguracja](#53-konfiguracja)
   - [5.4. Uruchomienie](#54-uruchomienie)

6. [Testowanie i Walidacja](#6-testowanie-i-walidacja)
   - [6.1. Strategia testowania](#61-strategia-testowania)
   - [6.2. Testy jednostkowe](#62-testy-jednostkowe)
   - [6.3. Testy integracyjne](#63-testy-integracyjne)
   - [6.4. Walidacja poprawności danych](#64-walidacja-poprawności-danych)

7. [Instrukcja Użytkownika](#7-instrukcja-użytkownika)
   - [7.1. Interfejs CLI](#71-interfejs-cli)
   - [7.2. Przykłady użycia](#72-przykłady-użycia)
   - [7.3. Rozwiązywanie problemów](#73-rozwiązywanie-problemów)

---

# 1. Wprowadzenie

## 1.1. Opis problemu

Współczesne systemy informatyczne często korzystają z różnych typów baz danych w zależności od specyficznych wymagań aplikacji. Relationalne bazy danych (takie jak PostgreSQL) sprawdzają się doskonale w scenariuszach wymagających ścisłej kontroli nad strukturą danych, transakcyjności oraz złożonych zapytań SQL. Z kolei bazy dokumentowe (jak MongoDB) oferują elastyczność w modelowaniu danych, co jest szczególnie przydatne w aplikacjach wymagających szybkiego prototypowania lub przechowywania danych o zmiennej strukturze.

Problem, który rozwiązuje projekt TO DB Converter, dotyczy konieczności przenoszenia danych między tymi dwoma systemami baz danych. Samodzielne implementowanie takiej transformacji jest zadaniem złożonym i podatnym na błędy, ponieważ wymaga:
- zrozumienia struktury danych źródłowych,
- mapowania typów danych między systemami,
- obsługi relacji między tabelami (w przypadku PostgreSQL) lub zagnieżdżonych dokumentów (w przypadku MongoDB),
- wnioskowania schematu bazy docelowej na podstawie danych źródłowych.

Brak automatycznego narzędzia do tego typu konwersji zmusza programistów do ręcznego pisania skryptów transformujących dane, co jest czasochłonne, podatne na błędy i trudne do utrzymania w miarę ewolucji schematów baz danych.

## 1.2. Cel projektu

Głównym celem projektu **TO DB Converter** jest stworzenie uniwersalnego narzędzia, które umożliwia automatyczną, dwukierunkową konwersję danych między PostgreSQL a MongoDB przy zachowaniu integralności danych i relacji.

Cele szczegółowe:
1. **Automatyzacja transformacji** – eliminacja potrzeby ręcznego pisania skryptów konwersji danych.
2. **Wnioskowanie schematu** – dynamiczne tworzenie struktury bazy docelowej na podstawie danych źródłowych.
3. **Zachowanie relacji** – prawidłowe odwzorowanie связей między danymi w obu kierunkach konwersji.
4. **Obsługa typów danych** – automatyczne rozpoznawanie i konwersja typów danych (INTEGER, VARCHAR, TIMESTAMP, itp.).
5. **Prostota użycia** – intuicyjny interfejs konfiguracji poprzez pliki właściwości i parametry linii poleceń.

Projekt ma również na celu dostarczenie elastycznego rozwiązania, które można łatwo rozszerzać o nowe funkcjonalności lub dostosowywać do specyficznych wymagań użytkowników.

## 1.3. Zakres funkcjonalny

Projekt TO DB Converter obejmuje następujące funkcjonalności:

### 1.3.1. Konwersja PostgreSQL → MongoDB

- **Ekstrakcja metadanych** – odczytywanie schematu tabel, kolumn, kluczy głównych i obcych z bazy PostgreSQL.
- **Ekstrakcja danych** – pobieranie danych z tabel w formie relacyjnej.
- **Transformacja relacyjno-dokumentowa** – przekształcanie płaskich tabel w zagnieżdżone dokumenty JSON z zachowaniem relacji.
- **Eksport do MongoDB** – zapisywanie dokumentów do kolekcji MongoDB z automatycznym tworzeniem schematu kolekcji.

### 1.3.2. Konwersja MongoDB → PostgreSQL

- **Ekstrakcja dokumentów** – pobieranie dokumentów z kolekcji MongoDB.
- **Transformacja dokumentowo-relacyjna** – spłaszczanie zagnieżdżonych struktur dokumentów do tabel z odpowiednimi kluczami obcymi.
- **Wnioskowanie schematu** – automatyczne tworzenie tabel w PostgreSQL na podstawie struktury dokumentów.
- **Obsługa typów** – rozpoznawanie i mapowanie typów danych z formatu BSON na typy SQL.

### 1.3.3. Funkcjonalności wspólne

- **Dwukierunkowa konwersja** – obsługa obu kierunków transformacji za pomocą jednego narzędzia.
- **Konfiguracja** – elastyczna konfiguracja poprzez pliki właściwości i parametry CLI.
- **Obsługa błędów** – mechanizmy informowania o błędach i awariach podczas procesu konwersji.

---

# 2. Specyfikacja Wymagań

## 2.1. Wymagania funkcjonalne

Wymagania funkcjonalne określają kluczowe operacje i zachowania systemu TO DB Converter.

### RF-01: Konwersja PostgreSQL → MongoDB

System musi umożliwić konwersję danych z relacyjnej bazy PostgreSQL do dokumentowej bazy MongoDB z następującymi możliwościami:

- **RF-01.1**: Automatyczne odczytywanie metadanych tabel (kolumny, typy danych, klucze główne i obce) z bazy PostgreSQL.
- **RF-01.2**: Ekstrakcja wszystkich rekordów z wybranych tabel wraz z danymi powiązanymi przez klucze obce.
- **RF-01.3**: Transformacja płaskich struktur relacyjnych w zagnieżdżone dokumenty JSON z osadzaniem danych powiązanych.
- **RF-01.4**: Tworzenie pól zawierających dane z tabel powiązanych w formacie `<referencedTable>_data`.
- **RF-01.5**: Zapis przekształconych dokumentów do kolekcji MongoDB, gdzie nazwa kolekcji odpowiada nazwie tabeli źródłowej.

### RF-02: Konwersja MongoDB → PostgreSQL

System musi umożliwić konwersję danych z dokumentowej bazy MongoDB do relacyjnej bazy PostgreSQL z następującymi możliwościami:

- **RF-02.1**: Ekstrakcja dokumentów ze wszystkich kolekcji MongoDB.
- **RF-02.2**: Automatyczne wnioskowanie schematu tabel PostgreSQL na podstawie struktury dokumentów (nazwy kolumn, typy danych).
- **RF-02.3**: Spłaszczanie zagnieżdżonych obiektów do kolumn z prefiksami (np. `address_city`, `address_street`).
- **RF-02.4**: Tworzenie osobnych tabel dla zagnieżdżonych tablic (relacje 1:N) z automatycznym dodawaniem kluczy obcych.
- **RF-02.5**: Automatyczne tworzenie tabel w PostgreSQL zgodnie z wywnioskowanymschemą.
- **RF-02.6**: Opcjonalne usuwanie istniejących tabel przed załadowaniem nowych danych (kontrolowane parametrem `postgres.dropExistingTables`).

### RF-03: Zarządzanie konfiguracją

System musi zapewnić elastyczną konfigurację poprzez:

- **RF-03.1**: Odczyt parametrów połączenia z pliku `application.properties` (hosty, porty, nazwy baz, użytkownicy, hasła).
- **RF-03.2**: Możliwość ustawienia kierunku konwersji: `POSTGRES_TO_MONGO` lub `MONGO_TO_POSTGRES`.
- **RF-03.3**: Nadpisanie kierunku konwersji poprzez argumenty linii poleceń (`--direction` lub `-d`).
- **RF-03.4**: Obsługę connection string dla MongoDB jako alternatywy do parametrów host/port.

### RF-04: Mapowanie typów danych

System musi automatycznie mapować typy danych między systemami:

**Mapowanie PostgreSQL → MongoDB:**
- Typy SQL (INTEGER, VARCHAR, TIMESTAMP, BOOLEAN, itp.) → typy BSON MongoDB

**Mapowanie MongoDB → PostgreSQL:**
- `Integer` → `INT`
- `Long` → `BIGINT`
- `Double`, `Float` → `DOUBLE PRECISION`
- `Boolean` → `BOOLEAN`
- `Date` → `TIMESTAMP`
- Inne typy → `VARCHAR`

### RF-05: Obsługa relacji

System musi zachować integralność relacyjną:

- **RF-05.1**: Identyfikacja kluczy obcych w PostgreSQL i osadzanie powiązanych danych w dokumentach MongoDB.
- **RF-05.2**: Tworzenie kluczy obcych w PostgreSQL dla tabel pochodzących z zagnieżdżonych struktur MongoDB.
- **RF-05.3**: Obsługa relacji 1:1 (osadzanie lub spłaszczanie) i 1:N (osobne tabele/kolekcje).

### RF-06: Logowanie i raportowanie

System musi informować użytkownika o przebiegu konwersji:

- **RF-06.1**: Logowanie kluczowych etapów procesu (nawiązywanie połączeń, ekstrakcja danych, transformacja, zapis).
- **RF-06.2**: Raportowanie liczby przetworzonych tabel/kolekcji i rekordów/dokumentów.
- **RF-06.3**: Szczegółowe komunikaty o błędach wraz z informacją o ich przyczynie.

## 2.2. Wymagania niefunkcjonalne

Wymagania niefunkcjonalne definiują ograniczenia techniczne i jakościowe systemu.

### RNF-01: Wydajność

- **RNF-01.1**: System powinien efektywnie obsługiwać tabele/kolekcje zawierające do 100 000 rekordów/dokumentów.
- **RNF-01.2**: Operacje wstawiania do bazy danych powinny wykorzystywać batch processing (operacje wsadowe) w celu zwiększenia wydajności.
- **RNF-01.3**: Ekstrakcja danych powinna być wykonywana z użyciem PreparedStatement dla zapewnienia bezpieczeństwa i wydajności.

### RNF-02: Niezawodność

- **RNF-02.1**: System musi poprawnie zamykać połączenia z bazami danych nawet w przypadku wystąpienia błędów.
- **RNF-02.2**: W przypadku niepowodzenia operacji system powinien zwrócić kod błędu i szczegółowy komunikat.
- **RNF-02.3**: System powinien obsługiwać typowe błędy połączeń (timeout, odmowa dostępu, niepoprawne dane logowania).

### RNF-03: Przenośność

- **RNF-03.1**: Aplikacja musi być niezależna od platformy (Windows, Linux, macOS) dzięki wykorzystaniu Javy 21.
- **RNF-03.2**: Wszystkie zależności muszą być zarządzane przez Maven i dostępne w publicznych repozytoriach.
- **RNF-03.3**: Konfiguracja powinna być przechowywana w plikach zewnętrznych (properties), umożliwiając łatwą migrację między środowiskami.

### RNF-04: Rozszerzalność

- **RNF-04.1**: Architektura systemu powinna umożliwiać łatwe dodawanie nowych transformatorów danych poprzez implementację interfejsu `IDataTransformer`.
- **RNF-04.2**: System powinien być zbudowany w oparciu o interfejsy (`IMetadataExtractor`, `IDocumentLoader`, `IDatabaseConnector`), umożliwiając wymianę implementacji.
- **RNF-04.3**: Kod powinien być modularny, z jasno określonymi odpowiedzialnościami poszczególnych klas.

### RNF-05: Bezpieczeństwo

- **RNF-05.1**: Dane logowania do baz danych (hasła) powinny być przechowywane w plikach konfiguracyjnych chronionych odpowiednimi uprawnieniami systemu operacyjnego.
- **RNF-05.2**: System powinien wykorzystywać parametryzowane zapytania (PreparedStatement) w celu zapobiegania atakom SQL injection.
- **RNF-05.3**: Połączenia z bazami danych powinny być nawiązywane z użyciem protokołów bezpiecznych, jeśli są dostępne.

### RNF-06: Utrzymywalność

- **RNF-06.1**: Kod źródłowy powinien być czytelny, zgodny z konwencjami Java (naming conventions, formatting).
- **RNF-06.2**: Kluczowe komponenty powinny być pokryte testami jednostkowymi (JUnit, Mockito).
- **RNF-06.3**: System powinien wykorzystywać framework logowania (SLF4J), umożliwiający łatwą konfigurację poziomu szczegółowości logów.

### RNF-07: Zgodność

- **RNF-07.1**: System musi być kompatybilny z PostgreSQL 15 lub nowszym.
- **RNF-07.2**: System musi być kompatybilny z MongoDB 7 lub nowszym.
- **RNF-07.3**: Wymagana wersja Java: 21 lub nowsza.
- **RNF-07.4**: Narzędzie budowania: Maven 3.8 lub nowszy.

### RNF-08: Użyteczność

- **RNF-08.1**: Interfejs linii poleceń powinien być intuicyjny i oferować pomoc (`--help`).
- **RNF-08.2**: Komunikaty błędów powinny być zrozumiałe dla użytkownika i zawierać wskazówki dotyczące rozwiązania problemu.
- **RNF-08.3**: Dokumentacja techniczna i użytkownika powinna być dostępna w repozytorium projektu.

---

*Przygotowano na potrzeby projektu TO DB Converter*
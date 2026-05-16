# TO DB Converter - Dokumentacja Techniczna V2

---

## Spis Treści

1. [Architektura systemu](#1-architektura-systemu)
2. [Uruchomienie i konfiguracja](#2-uruchomienie-i-konfiguracja)
3. [Interaktywny kreator (ConfigWizard)](#3-interaktywny-kreator-configwizard)
4. [Wykrywanie relacji między tabelami](#4-wykrywanie-relacji-między-tabelami)
5. [Strategie transformacji relacji](#5-strategie-transformacji-relacji)
6. [Wzorce projektowe MongoDB](#6-wzorce-projektowe-mongodb)
7. [Przepływ danych krok po kroku](#7-przepływ-danych-krok-po-kroku)
8. [Struktura plików i odpowiedzialności klas](#8-struktura-plików-i-odpowiedzialności-klas)
9. [Testowanie](#9-testowanie)
10. [Znane ograniczenia](#10-znane-ograniczenia)

---

# 1. Architektura systemu

System TO DB Converter wykorzystuje architekturę **Pipe-and-Filter** – dane przepływają przez sekwencyjne etapy: ekstrakcja → transformacja → ładowanie.

```
┌──────────────┐     ┌──────────────────┐     ┌──────────────┐
│  Ekstraktor  │───▶│   Transformer     │───▶│    Loader    │
│ (PostgreSQL  │     │ (UniversalTransf.│     │ (MongoDB lub │
│  lub MongoDB)│     │  + PatternOpt.)  │     │  PostgreSQL) │
└──────────────┘     └──────────────────┘     └──────────────┘
```

Kierunki konwersji:
- **PostgreSQL → MongoDB** (POSTGRES_TO_MONGO)
- **MongoDB → PostgreSQL** (MONGO_TO_POSTGRES)

Główny orchestrator: `ConverterService` – zarządza połączeniami, wywołuje ekstrakcję, transformację i ładowanie w odpowiedniej kolejności.

---

# 2. Uruchomienie i konfiguracja

## 2.1. Wymagania

- Java 21
- Docker + Docker Compose (dla środowiska deweloperskiego/testowego)
- Maven 3.8+ (do budowy)

## 2.2. Szybki start

```bash
# 1. Budowa
mvn clean package

# 2. Uruchomienie baz danych (testowe)
docker-compose -f docker-compose-test.yml up -d

# 3a. Interaktywny kreator (zalecany)
java -jar target/to-db-converter-1.0-SNAPSHOT.jar --wizard

# 3b. Headless (używa application.properties z CWD lub classpath)
java -jar target/to-db-converter-1.0-SNAPSHOT.jar

# 4. Zatrzymanie
docker-compose -f docker-compose-test.yml down -v
```

## 2.3. Plik konfiguracyjny application.properties

Konfiguracja wczytywana jest w kolejności:
1. **CWD** (Current Working Directory) – plik `application.properties` w bieżącym katalogu
2. **Classpath** – plik wbudowany w JAR (`src/main/resources/application.properties`)

### Kluczowe parametry:

```properties
# PostgreSQL
postgres.host=localhost
postgres.port=5432
postgres.database=testdb
postgres.username=user
postgres.password=password
postgres.schema=public
postgres.dropExistingTables=true

# MongoDB
mongo.host=localhost
mongo.port=27017
mongo.database=testdb
mongo.username=root
mongo.password=rootpassword

# Kierunek konwersji
conversion.direction=POSTGRES_TO_MONGO

# Globalna strategia relacji (EMBED | REFERENCE)
relationship.strategy=EMBED

# Strategie per-tabela (nadpisują globalną)
relationship.strategy.customers=EMBED
relationship.strategy.orders=REFERENCE

# M:N tryb (FULL | IDS) per para tabel
relationship.mn_mode.students_courses=IDS

# Wzorce MongoDB (patrz sekcja 6)
pattern.attribute.enabled=true
pattern.attribute.threshold=3
pattern.bucket.enabled=false
pattern.bucket.size=10
pattern.subset.enabled=false
pattern.subset.limit=10
pattern.outlier.enabled=false
pattern.outlier.threshold=50
pattern.computed.enabled=false
pattern.computed.fields=total:SUM(price)
pattern.approximation.enabled=false
pattern.approximation.granularity=100
pattern.approximation.fields=stock_quantity
```

## 2.4. Docker Compose

System zawiera dwa pliki docker-compose:

| Plik | Port PG | Port Mongo | Użytkownik PG | Baza PG | Zastosowanie |
|------|---------|------------|---------------|---------|-------------|
| `docker-compose.yml` | 5433 | 27018 | postgres | source_db | Produkcja |
| `docker-compose-test.yml` | 5432 | 27017 | user | testdb | Testy/rozwój |

Oba ładują `database/init-postgres.sql` zawierający 19 tabel testowych z różnymi typami relacji.

---

# 3. Interaktywny kreator (ConfigWizard)

Kreator uruchamiany flagą `--wizard` zapewnia interaktywny interfejs TUI (Text User Interface) oparty na bibliotece **JLine 3**.

## 3.1. Ekrany kreatora

| Ekran | Opis |
|-------|------|
| **Step 1/4**: Database Connections | Konfiguracja połączeń PostgreSQL i MongoDB |
| **Step 2/4**: Relationship Strategies | Per-table wybór EMBED/REFERENCE + M:N FULL/IDS |
| **Step 3/4**: Schema Design Patterns | Włączenie/konfiguracja wzorców MongoDB |
| **Step 4/4**: Conversion Direction | Wybór kierunku konwersji |

## 3.2. Nawigacja

| Klawisz | Działanie |
|---------|-----------|
| ↑ ↓ | Nawigacja między wierszami/opcjami |
| ← → | Przełączanie EMBED ↔ REFERENCE |
| Tab | Przełączanie FULL ↔ IDS (dla M:N) |
| Spacja | Włącz/wyłącz checkbox (patterns) |
| Enter | Potwierdzenie |

## 3.3. Wykrywanie tabel

Kreator łączy się do PostgreSQL, pobiera schemat wszystkich tabel za pomocą `MetadataExtractor`, analizuje klucze obce i wyświetla tabelę z propozycjami strategii. Kolumna "Suggested" podpowiada EMBED dla tabel z ≤1000 wierszami, REFERENCE dla większych.

---

# 4. Wykrywanie relacji między tabelami

Kluczowy komponent: **`MetadataExtractor.java`** – wykorzystuje JDBC `DatabaseMetaData` do analizy schematu.

## 4.1. Typy relacji

```java
public enum RelationshipType { ONE_TO_ONE, ONE_TO_MANY, MANY_TO_MANY }
```

## 4.2. Wykrywanie 1:1 vs 1:N

Metoda `determineRelationshipTypes()` (linia 136):

Dla każdego klucza obcego sprawdzana jest **unikalność kolumny FK** poprzez:
1. Czy kolumna jest częścią **PRIMARY KEY** (dowolnego, nie tylko jednokolumnowego)?
2. Czy istnieje **UNIQUE INDEX** na tej kolumnie (sprawdzone przez `getIndexInfo()` JDBC)?

Jeśli tak → **ONE_TO_ONE**. W przeciwnym razie → **ONE_TO_MANY**.

> **Uwaga**: `isColumnUnique()` sprawdza czy nazwa kolumny FK występuje w tablicy kolumn PK (`getPrimaryKeyColumnArray()`). Jeśli kolumna FK jest częścią composite PK, zostanie uznana za unikalną.

## 4.3. Wykrywanie M:N przez tabele junction

Metoda `detectManyToManyRelationships()` (linia 183) działa w dwóch krokach:

### Krok 1: Dwukierunkowe FK (rzadkie)
Sprawdza czy dwie tabele mają wzajemne klucze obce (A → B i B → A). Jeśli tak → **MANY_TO_MANY**.

### Krok 2: Tabela junction (główna ścieżka)
Tabela jest uznawana za junction jeśli spełnia WARUNKI (sprawdzone inline w `detectManyToManyRelationships()`, linia ~202):
1. Ma **dokładnie 2 klucze obce**
2. Liczba kolumn ≤ **5** (PK + 2FK + do 2 dodatkowych kolumn)

Jeśli warunki są spełnione, OBA klucze obce są ustawiane na **MANY_TO_MANY**.

### Przykłady z bazy testowej:

| Tabela | FK1 | FK2 | Kolumny | Junction? |
|--------|-----|-----|---------|-----------|
| enrollments | student_id | course_id | 5 | ✅ |
| movie_roles | actor_id | movie_id | 5 | ✅ |
| order_items | order_id | product_id | 5 | ✅ |
| project_tasks | project_id | assignee_id | 6 | ❌ (>5 kolumn) |

## 4.4. Self-referencja

Klucz obcy wskazujący na tę samą tabelę (np. `employees.manager_id → employees.employee_id`).
- Pomijany w wykrywaniu M:N (dwukierunkowe sprawdzenie pomija self-ref)
- Klasyfikowany jako **ONE_TO_MANY** (self-ref 1:N)

---

# 5. Strategie transformacji relacji

## 5.1. EMBED (domyślna)

Dane powiązane są **zagnieżdżane** w dokumencie nadrzędnym:

| Relacja | Efekt w MongoDB |
|---------|----------------|
| 1:1 | Obiekt osadzony jako pole (`customer: { ... }`) |
| 1:N | Tablica osadzona jako pole (`orders: [{ ... }, { ... }]`) → nazwa pluralizowana |
| M:N (FULL) | Obiekty powiązane osadzone w tablicy (`courses: [{ ... }, { ... }]`) |
| M:N (IDS) | Tablica ID (`courses_ids: [101, 102]`) |

## 5.2. REFERENCE

Dane powiązane **nie są zagnieżdżane** – pozostają w osobnych kolekcjach.
- Dla 1:1: zapisywane jest tylko `referencedTable_id`
- Dla 1:N: dzieci nie są osadzane (pozostają jako osobna kolekcja)
- Dla M:N: jak EMBED (strategia dotyczy tylko 1:1 i 1:N)

## 5.3. M:N tryb FULL vs IDS

Dotyczy tylko relacji M:N przez tabele junction:

| Tryb | Efekt |
|------|-------|
| **FULL** | W dokumencie nadrzędnym osadzane są pełne obiekty powiązane |
| **IDS** | W dokumencie nadrzędnym zapisywana jest tylko tablica ID (`{table}_ids`) |

### Jak działa M:N przez junction (PG→Mongo):

1. `buildJunctionMnIndexes()` – dla tabeli nadrzędnej (np. `students`) znajduje tabele junction (np. `enrollments`) i buduje indeks: `student_id → [dane kursów]`
2. Tabela junction jest **pomijana** jako 1:N child
3. Powiązane obiekty (np. `courses`) są osadzane bezpośrednio w dokumencie nadrzędnym

---

# 6. Wzorce projektowe MongoDB

Implementacja w **`MongoDbPatternOptimizer.java`**. Kolejność aplikacji (hardcoded):

```
Computed → Approximation → Attribute → Bucket → Subset → Outlier
```

## 6.1. Computed Pattern

Pre-kalkuluje wartości na podstawie istniejących pól.

**Config:**
```properties
pattern.computed.fields=total_price:SUM(price,discount),count:COUNT(items)
```

Format: `fieldName:OPERATION(args)`
- `SUM(a,b,c)` – sumuje wartości kolumn w obrębie dokumentu
- `COUNT(field)` – zlicza elementy w tablicy

## 6.2. Approximation Pattern

Zaokrągla wartości liczbowe do zadanej granularności.

**Config:**
```properties
pattern.approximation.granularity=100
pattern.approximation.fields=population
```

Example: `40123 → 40100`, `40500 → 40500`

## 6.3. Attribute Pattern

Grupuje kolumny o wspólnym prefiksie w tablicę `{prefix}_attrs` z wpisami `{k, v, u}`.

**Próg:** min 3 kolumny z tym samym prefiksem (konfigurowalny przez `pattern.attribute.threshold`).

**Przykład:**
```json
// Przed: { release_US: "2024-01-01", release_FR: "2024-02-01", release_UK: "2024-03-01" }
// Po: { releases: [{k: "release_US", v: "2024-01-01", u: "US"}, {k: "release_FR", v: "2024-02-01", u: "FR"}, ...] }
```

Pole `u` zawiera sufiks wyodrębniony z nazwy kolumny (część po ostatnim podkreślniku).

## 6.4. Bucket Pattern

Grupuje dokumenty w "wiaderka" po N elementów, według klucza grupującego.

**Config:**
```properties
pattern.bucket.key=customerId
pattern.bucket.size=10
```

Tworzy dokumenty z polami: `_id`, `bucket_id`, `count`, `data` (tablica zgrupowanych dokumentów).

**Obsługa null:** Dokumenty z brakującym kluczem grupującego są umieszczane w osobnym bucketu (klucz `_NULL_`).

## 6.5. Subset Pattern

Dzieli duże tablice na część "gorącą" (w głównym dokumencie) i "zimną" (w osobnej kolekcji `{table}_extras`).

**Config:**
```properties
pattern.subset.limit=10
```

Elementy powyżej limitu są przenoszone do osobnej kolekcji z zachowaniem oryginalnej struktury (klucze zachowane, a nie zagnieżdżone pod `item`).

## 6.6. Outlier Pattern

Izoluje dokumenty z tablicami przekraczającymi próg do osobnej kolekcji `{table}_outliers`.

**Config:**
```properties
pattern.outlier.threshold=50
```

Obsługuje WSZYSTKIE oversizowane tablice w dokumencie (nie tylko pierwszą). Dokument z `has_extras: true`.

---

# 7. Przepływ danych krok po kroku

## 7.1. PostgreSQL → MongoDB

```
START
  │
  ▼
Połączenie do PostgreSQL (PostgreSQLConnection)
  │
  ▼
Ekstrakcja metadanych (MetadataExtractor)
  ├── Lista tabel, kolumn, typów
  ├── Klucze główne (PK)
  ├── Klucze obce (FK) – surowa lista
  ├── determineRelationshipTypes() → 1:1 / 1:N
  └── detectManyToManyRelationships() → M:N
  │
  ▼
Ekstrakcja DANYCH ze wszystkich tabel (DataExtractor)
  ├── Batch processing (1000 rekordów)
  ├── Konwersja typów specjalnych (JSON/JSONB → String)
  └── Dane w Map<String, List<Map<String, Object>>>
  │
  ▼
Dla KAŻDEJ tabeli:
  │
  ├── Wyczyść kolekcję MongoDB (clearCollection)
  ├── Utwórz indeksy na PK i FK
  │
  ├── Dla każdego batcha rekordów:
  │   │
  │   ├── buildChildIndexes()     → indeks dzieci (parentId → [dzieci])
  │   ├── buildReferenceIndexes() → indeks referencji (table → pk → rekord)
  │   ├── buildManyToManyIndexes() → indeks M:N bidirect.
  │   └── buildJunctionMnIndexes() → indeks M:N przez junction
  │   │
  │   ├── Dla każdego rekordu nadrzędnego:
  │   │   ├── Kopiuj pola rodzica
  │   │   ├── Obsłuż FK 1:1 → embed lub reference ID
  │   │   ├── Obsłuź 1:N children → embed array (lub skip dla REFERENCE)
  │   │   ├── Obsłuź M:N junction → embed related entities
  │   │   └── Obsłuź M:N bidirect. → embed/reference
  │   │
  │   ├── applyPatterns() → wzorce MongoDB
  │   └── loadDocuments() → zapis do MongoDB
  │
  ▼
Zamknij połączenia
  │
  ▼
KONIEC
```

## 7.2. MongoDB → PostgreSQL

```
START
  │
  ▼
Połączenie do MongoDB (MongoDBConnection)
  │
  ▼
Lista kolekcji (pomijając system.*)
  │
  ▼
FAZA 1: Odkrywanie schematu
  ├── Próbka 10 dokumentów z każdej kolekcji
  ├── flattenToRelational() → wnioskowanie tabel i kolumn
  └── Map<String, TableMetadata> – metadane docelowe
  │
  ▼
FAZA 2: Tworzenie tabel w PostgreSQL
  ├── DROP TABLE IF EXISTS ... CASCADE (opcjonalnie)
  └── CREATE TABLE z wywnioskowanymi kolumnami i typami
  │
  ▼
FAZA 3: Ładowanie danych
  ├── Batch processing (1000 dokumentów)
  ├── flattenToRelational() → spłaszczenie do wierszy
  ├── Deduplikacja przez processedIds
  └── INSERT INTO ... batch
  │
  ▼
FAZA 4: Dodawanie kluczy obcych
  └── ALTER TABLE ... ADD CONSTRAINT ...
  │
  ▼
Zamknij połączenia
  │
  ▼
KONIEC
```

## 7.3. Heurystyki spłaszczania (MongoDB → PostgreSQL)

| Wzór w dokumencie | Interpretacja |
|-------------------|---------------|
| Pole `id` lub `_id` | Klucz główny tabeli |
| Pole kończące się na `_id` (nie `_ids`) | Klucz obcy do tabeli o nazwie bez `_id` |
| Pole kończące się na `_ids` (tablica) | Relacja M:N – tworzona tabela junction `{parent}_{ref}_junction` |
| Pole będące tablicą (kończy się na "s") | Relacja 1:N – osobna tabela potomna z FK do rodzica |
| Pole będące obiektem z polem `id` | Referencja do osobnej tabeli (FK) |
| Pole będące obiektem z kluczem kończącym się na `_data` | Referencja do tabeli o nazwie bez `_data` |
| Pole będące obiektem BEZ pola `id` | Spłaszczenie do kolumn z prefiksem (`address_city`) |

---

# 8. Struktura plików i odpowiedzialności klas

## 8.1. Główne komponenty

| Plik | Odpowiedzialność |
|------|-----------------|
| **Main.java** | Entry point, parsowanie CLI (`--wizard`, `-d`), ładowanie configu |
| **ConverterService.java** | Orchestrator – wybór kierunku, kolejność operacji |
| **ConfigWizard.java** | Interaktywny kreator TUI (JLine 3) |

## 8.2. Połączenia (connection)

| Plik | Odpowiedzialność |
|------|-----------------|
| `IDatabaseConnector.java` | Interfejs: connect(), disconnect(), isConnected() |
| `PostgreSQLConnection.java` | Połączenie JDBC do PostgreSQL |
| `MongoDBConnection.java` | Połączenie przez MongoDB Sync Driver |

## 8.3. Ekstrakcja (extractor)

| Plik | Odpowiedzialność |
|------|-----------------|
| `MetadataExtractor.java` | Pobiera schemat: tabele, kolumny, PK, FK, typy relacji (1:1/1:N/M:N) |
| `DataExtractor.java` | Pobiera dane z PostgreSQL (batch, konwersja typów specjalnych) |
| `MongoExtractor.java` | Pobiera dokumenty z MongoDB (batch, konwersja ObjectId→String) |

## 8.4. Transformacja (transformer)

| Plik | Odpowiedzialność |
|------|-----------------|
| `UniversalTransformer.java` | Główna logika transformacji w obu kierunkach, 3 indeksy pomocnicze |
| `MongoDbPatternOptimizer.java` | 6 wzorców MongoDB, forward + reverse |
| `IDataTransformer.java` | Interfejs: transformToDocuments(), flattenToRelational() |

## 8.5. Ładowanie (exporter)

| Plik | Odpowiedzialność |
|------|-----------------|
| `MongoDBExporter.java` | Zapis do MongoDB (batch insert, indeksy, JSON export) |
| `PostgresLoader.java` | Zapis do PostgreSQL (CREATE TABLE, INSERT, FK) |

## 8.6. Modele (model)

| Plik | Odpowiedzialność |
|------|-----------------|
| `TableMetadata.java` | Metadane tabeli: nazwa, schema, kolumny, PK, FK |
| `ColumnMetadata.java` | Kolumna: nazwa, typ, nullable, PK/FK flagi |
| `ForeignKeyMetadata.java` | Klucz obcy: kolumna, tabela referencjonowana, typ relacji |

## 8.7. Konfiguracja

| Plik | Odpowiedzialność |
|------|-----------------|
| `DatabaseConfig.java` | Odczyt i walidacja `application.properties`, per-table strategie |

## 8.8. Narzędzia (util)

| Plik | Odpowiedzialność |
|------|-----------------|
| `TypeMapper.java` | Mapowanie typów Java/BSON ↔ SQL dla PostgreSQL |

## 8.9. Baza testowa

| Plik | Opis |
|------|------|
| `database/init-postgres.sql` | 19 tabel testowych (1:N, M:N, 1:1, self-ref, composite PK, JSON, NULL, reserved words) |

## 8.10. Pliki konfiguracyjne Docker

| Plik | Opis |
|------|------|
| `docker-compose.yml` | Środowisko produkcyjne (port 5433/27018) |
| `docker-compose-test.yml` | Środowisko testowe (port 5432/27017) |

---

# 9. Testowanie

## 9.1. Testy jednostkowe (JUnit 5 + Mockito)

| Klasa testowa | Testowana klasa | Liczba testów |
|---------------|----------------|---------------|
| `DatabaseConfigTest` | `DatabaseConfig` | 12 |
| `UniversalTransformerTest` | `UniversalTransformer` | 16 |
| `MongoDbPatternOptimizerTest` | `MongoDbPatternOptimizer` | 7 |
| `MongoToSqlEdgeCaseTest` | `UniversalTransformer.flattenToRelational` | 1 |
| `TypeMapperTest` | `TypeMapper` | 2 |

## 9.2. Testy E2E

| Klasa testowa | Opis |
|---------------|------|
| `FullCycleE2ETest` | Round-trip: create PG → run → verify (prosty schemat) |
| `ComprehensiveE2ETest` | 7 testów: EMBED, REFERENCE, IDS, patterns, round-trip, content, M:N content |
| `FullSchemaE2ETest` | 16 testów: pełny schemat 19 tabel, wszystkie wzorce, strategie M:N, Bucket, Subset, Outlier, Computed |
| `MongoToPostgresE2ETest` | 4 testy: konwersja MongoDB → PostgreSQL, spłaszczanie dokumentów, odtwarzanie FK |

Wszystkie testy wymagają działających kontenerów Docker (PostgreSQL + MongoDB).

## 9.3. Uruchomienie testów

```bash
docker-compose -f docker-compose-test.yml up -d
mvn test
```

---

# 10. Znane ograniczenia

1. **Brak rekurencyjnego zagnieżdżania** – dzieci dzieci nie są automatycznie zagnieżdżane (np. `order_items` wewnątrz `orders` wewnątrz `customers`). Przy strategii REFERENCE dla `orders`, tabela `order_items` pozostaje w osobnej kolekcji.
2. **Self-referencja w embeddingu** – self-referencjonujące tabele (np. `employees.manager_id`) nie są zagnieżdżane jako dzieci (uniknięcie nieskończonej rekurencji).
3. **1:1 w ConfigWizard** – ConfigWizard łączy się do bazy i pobiera typy relacji z `MetadataExtractor`, ale interfejs TUI wyświetla zarówno 1:1 jak i 1:N jako "1:N" (nie rozróżnia wizualnie). Podczas właściwej konwersji `MetadataExtractor` poprawnie rozróżnia 1:1 od 1:N i transformer stosuje odpowiednią strategię.
4. **M:N → 1:N kolizja** – Tabele z 2 FK są traktowane jako junction (M:N). Jeśli tabela ma 2 FK ale powinna być 1:N child (np. `project_tasks` z 6 kolumnami), próg `≤5` kolumn wyklucza ją z M:N. Dla tabel z 5 kolumnami i 2 FK (np. `order_items`) klasyfikacja jako junction jest poprawna (M:N między orders a products).
5. **`_id` w MongoDB** – W PG→Mongo kierunku `_id` jest generowane automatycznie przez MongoDB driver jako ObjectId. Oryginalny klucz główny z PG jest zachowany jako osobne pole (np. `customer_id`).

---

*Dokumentacja przygotowana na potrzeby projektu TO DB Converter.*

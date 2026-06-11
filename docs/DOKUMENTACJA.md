# TO DB Converter — Dokumentacja Techniczna

---

## Spis Treści

1. [Architektura systemu](#1-architektura-systemu)
2. [Uruchomienie i konfiguracja](#2-uruchomienie-i-konfiguracja)
3. [Interaktywny kreator (ConsoleWizard)](#3-interaktywny-kreator-consolewizard)
4. [Wykrywanie relacji między tabelami](#4-wykrywanie-relacji-między-tabelami)
5. [Strategie transformacji relacji](#5-strategie-transformacji-relacji)
6. [Wzorce projektowe MongoDB](#6-wzorce-projektowe-mongodb)
7. [Przepływ danych krok po kroku](#7-przepływ-danych-krok-po-kroku)
8. [Struktura plików i odpowiedzialności klas](#8-struktura-plików-i-odpowiedzialności-klas)
9. [Testowanie](#9-testowanie)
10. [Znane ograniczenia](#10-znane-ograniczenia)
11. [Wykryte i naprawione błędy](#11-wykryte-i-naprawione-błędy)

---

# 1. Architektura systemu

System **TO DB Converter** realizuje konwersję **PostgreSQL → MongoDB** z użyciem architektury **Pipe-and-Filter** (sekwencyjne etapy Extract → Transform → Load) wzbogaconej o warstwę konfiguracji (TUI wizard + plik `.properties`).

```
┌──────────┐     ┌──────────────┐    ┌──────────────┐     ┌──────────────┐
│  Config  │───▶│  JDBCSchema  │───▶│ JDBCData     │───▶│ Universal    │
│ .props / │     │  Extractor   │    │ Extractor    │     │ Transformer  │
│  Wizard  │     │ (schemat+FK) │    │  (wiersze)   │     │ + Patterns   │
└──────────┘     └──────────────┘    └──────────────┘     └──────┬───────┘
                                                                 │
                                                                 ▼
                                                       ┌──────────────┐
                                                       │  MongoDB     │
                                                       │  Loader      │
                                                       └──────────────┘
```

**Główny orchestrator:** `ETLPipeline.execute()` — koordynuje wszystkie 4 etapy, propaguje wyjątki, drukuje podsumowanie.

**Kierunek konwersji:** Jednokierunkowy — wyłącznie **PostgreSQL → MongoDB**.

**Wersja:** 1.0.0 (shaded JAR z wszystkimi zależnościami).

---

# 2. Uruchomienie i konfiguracja

## 2.1. Wymagania

- Java 21 (LTS)
- Maven 3.9+ (do budowy ze źródeł)
- Docker + Docker Compose (dla środowiska testowego)
- MongoDB 7+ oraz PostgreSQL 16+ (docelowo) — Docker dostarcza oba

## 2.2. Szybki start

```bash
# 1. Budowa shaded JAR
mvn clean package

# 2. Uruchomienie baz (inicjalizacja seed)
docker compose up -d

# 3a. Interaktywny kreator konfiguracji
java -jar target/to-db-converter-1.0.0.jar wizard

# 3b. Bezpośrednie uruchomienie migracji (po wygenerowaniu db-converter.properties)
java -jar target/to-db-converter-1.0.0.jar run

# 3c. Weryfikacja połączeń (bez migracji)
java -jar target/to-db-converter-1.0.0.jar validate

# 3d. Generowanie raportu HTML przed/po migracji
java -jar target/to-db-converter-1.0.0.jar report

# 4. Zatrzymanie środowiska
docker compose down -v
```

## 2.3. CLI — komendy

| Komenda | Aliasy | Opis |
|---------|--------|------|
| `run` | `-r`, `--run` | Wykonuje pełną migrację z pliku `db-converter.properties` |
| `wizard` | `-w`, `--wizard` | Uruchamia interaktywny TUI do konfiguracji |
| `validate` | `-v`, `--validate` | Testuje połączenia do obu baz bez wykonywania migracji |
| `report` | `-rp`, `--report` | Generuje raport HTML z porównaniem źródła (PostgreSQL) i celu (MongoDB) |

Każda komenda akceptuje opcjonalny parametr `--config=<ścieżka>` wskazujący niestandardowy plik konfiguracyjny. Komenda `report` dodatkowo akceptuje `--output=<ścieżka>` oraz `--samples=<N>` (liczba próbek na tabelę/kolekcję).

## 2.4. Plik konfiguracyjny `db-converter.properties`

Plik jest wczytywany z bieżącego katalogu (`CWD`). Przykład:

```properties
# === Połączenie źródłowe (PostgreSQL) ===
source.jdbc.url=jdbc:postgresql://localhost:5433/source_db
source.jdbc.username=postgres
source.jdbc.password=password

# === Połączenie docelowe (MongoDB) ===
target.mongodb.uri=mongodb://admin:password@localhost:27018
target.mongodb.database=mydb_converted

# === Domyślna strategia relacji (EMBED | REFERENCE) ===
relationship.strategy.default=EMBED

# === Nadpisania strategii per para tabel ===
# Format: child_table.parent_table=STRATEGY
relationship.strategy.activity_logs.employees=REFERENCE
relationship.strategy.employee_details.employees=EMBED
relationship.strategy.employee_projects.employees=REFERENCE
relationship.strategy.employee_projects.projects=REFERENCE
relationship.strategy.employees.departments=EMBED
relationship.strategy.employees.employees=REFERENCE

# === Wzorce projektowe (patrz sekcja 6) ===
# Format: pattern.<typ>.<tabela>=<konfiguracja>
pattern.attribute.employee_details=personal=pesel:PESEL,birth_date:Data,bio:Bio
pattern.attribute.enrollments=details=notes:Notatki,grade:Ocena,enrolled_at:Data
pattern.computed.departments=employee_count=COUNT(employees.id)
pattern.computed.employees=subordinate_count=COUNT(employees.id)
pattern.computed.projects=participant_count=COUNT(employee_projects.employee_id)
pattern.subset.employees=activity_logs=3
pattern.outlier.employees=activity_logs=3
pattern.approximation.metrics=population:100,visits:1000

# === Bezpieczniki ===
safeguard.max_children_per_parent=1000
```

## 2.5. Docker Compose

Plik `docker-compose.yml` definiuje trzy kontenery:

| Kontener | Obraz | Port hostowy | Uwierzytelnianie |
|----------|-------|--------------|------------------|
| `todbconverter-postgres-test` | postgres:16-alpine | 5433 | postgres / password |
| `todbconverter-mongo-test` | mongo:7.0 | 27018 | admin / password |
| `todbconverter-pgadmin` | dpage/pgadmin4 | 5050 | admin@admin.com / admin |

Seed (`postgres-init/01-schema.sql` + `02-data.sql`) ładowany jest automatycznie przez `docker-entrypoint-initdb.d`.

**Uwaga:** Porty `5433` i `27018` są mapowaniami hosta — wewnątrz kontenera Postgres nasłuchuje na `5432`, MongoDB na `27017`. Konfiguracja w `db-converter.properties` MUSI używać portów hosta.

---

# 3. Interaktywny kreator (ConsoleWizard)

Wizard to aplikacja TUI (Text User Interface) zbudowana na bibliotece **JLine 3** z obsługą trybu raw.

## 3.1. Ekrany kreatora (4 kroki)

| Krok | Tytuł | Opis |
|------|-------|------|
| **1/4** | Connections | URL JDBC, username, password, URI Mongo, docelowa baza |
| **2/4** | Relationship Strategies | Tabela relacji z wyborem strategii EMBED/REFERENCE per para |
| **3/4** | Design Patterns | Matryca tabel × {Attribute, Computed, Subset, Outlier, Approximation} z konfiguracją |
| **4/4** | Summary | Podgląd gotowej konfiguracji, zapis do `db-converter.properties` |

### 3.2.1 Format konfiguracji w wizardzie

W kroku 3 każdy wzorzec wymaga innego formatu konfiguracji:
- **Attribute** — `arrayName=col:Key,col:Key,...` (np. `releases=release_US:USA,release_France:France`)
- **Computed** — `fieldName=FUNC(childTable.column)` (np. `order_count=COUNT(orders.id)`)
- **Subset** — `childTable=limit` (np. `reviews=3`)
- **Outlier** — `childTable=threshold` (np. `activity_logs=3`)
- **Approximation** — `field1:gran1,field2:gran2` (np. `population:100,visits:1000`)

## 3.2. Nawigacja w wizardzie

| Klawisz | Działanie |
|---------|-----------|
| `↑` `↓` `←` `→` | Poruszanie się po tabeli/matrycy |
| `Tab` | Przejście do następnego pola (Step 1) |
| `Enter` | Potwierdzenie / submity kroku |
| `Space` | Przełączenie wartości strategii LUB włączenie wzorca |
| `Ctrl+C` | Anulowanie bez zapisu |

W kroku 3 po włączeniu wzorca (Spacja) wyświetlany jest prompt inline do podania konfiguracji (np. `arrayName=col:key,col:key`).

## 3.3. Generowanie pliku konfiguracyjnego

Po przejściu wszystkich 4 kroków wizard generuje plik `db-converter.properties` z:
- Domyślnymi wartościami połączeń (z możliwością edycji)
- Strategiami dla każdej wykrytej relacji
- Skonfigurowanymi wzorcami
- Wszystkimi kluczami są escapowane (`\:`, `\=`) zgodnie z formatem Java Properties

Przykładowy wynik z kroku 4 (summary):
```
Source Database
  JDBC URL: jdbc:postgresql://localhost:5433/source_db
  Username: postgres

Target MongoDB
  URI:      mongodb://localhost:27018
  Database: mydb_converted

Relationship Strategies
  employees.employees = REFERENCE
  employees.departments = EMBED
  activity_logs.employees = EMBED
  ...

Design Patterns
  departments.computed = employee_count=COUNT(employees.id)
  employees.computed = subordinate_count=COUNT(employees.id)
  employees.subset = activity_logs=3
  ...
```

## 3.4. Bezpieczeństwo

Hasło w wizardzie wprowadzane jest przez zwykłe `readLine()` (bez maskowania), zgodnie z resztą pól. Pole może być puste, a marker `<saved>` pozwala zachować istniejące hasło. Komunikaty błędów są przepuszczane przez `sanitizeError()` aby nie wyciekły credentials.

---

# 4. Wykrywanie relacji między tabelami

Kluczowy komponent: **`JDBCSchemaExtractor`** + **`SchemaGraph`**.

## 4.1. Klasyfikacja tabel (`TableType`)

```java
public enum TableType {
    PRIMARY_ENTITY,    // Brak FK wchodzących — samodzielna kolekcja
    CHILD_ENTITY,      // Ma FK — może być embedded
    JUNCTION_TABLE     // Ma 2+ FK — tabela łącząca M:N
}
```

Klasyfikacja zachodzi w `JDBCSchemaExtractor.extractSchema()` po zliczeniu FK.

## 4.2. Kardynalność (`Cardinality`)

```java
public enum Cardinality {
    ONE_TO_ONE,
    ONE_TO_MANY,
    MANY_TO_MANY
}
```

## 4.3. Wykrywanie 1:1 vs 1:N — `determineCardinality()`

Metoda `JDBCSchemaExtractor.determineCardinality(Connection connection, ForeignKeyMetadata fk)`:

1. Pobiera indeksy przez `meta.getIndexInfo(catalog, schema, fkTable, unique=true, approx=false)`.
2. **Grupuje wyniki po `INDEX_NAME`** — `getIndexInfo()` zwraca JEDEN WIERSZ na kolumnę indeksu.
3. Jednokolumnowy UNIQUE INDEX lub jednokolumnowy PRIMARY KEY → **ONE_TO_ONE**.
4. Wszystko inne → **ONE_TO_MANY**.

> **Krytyczne:** Poprzednia implementacja nie grupowała po `INDEX_NAME`, co powodowało błędną klasyfikację composite PK (np. `PRIMARY KEY(employee_id, project_id)`) jako 1:1 — skutkowało utratą danych w tabelach junction. Naprawione w BUG 1 (szczegóły w sekcji 11).

## 4.4. Wykrywanie M:N

System **nie ustawia kardynalności MANY_TO_MANY** bezpośrednio. Zamiast tego:

- Tabele z 2+ FK (junction) są klasyfikowane jako `JUNCTION_TABLE`.
- Oba FK junction otrzymują kardynalność `ONE_TO_MANY`.
- `EdgeStrategyRegistry` + `detectAndHandleCycles()` wymuszają strategię **REFERENCE** dla obu krawędzi junction (zapobiega nieskończonej rekurencji embed).
- Wynikowy efekt: junction staje się osobną kolekcją w MongoDB (np. `employee_project`) z dokumentami `{employee_id, project_id}`.

**Przykład z bazy testowej:**

| Tabela | FK1 | FK2 | Typ | Kolekcja MongoDB |
|--------|-----|-----|-----|-----------------|
| `employee_projects` | employee_id | project_id | JUNCTION | `employee_project` (3 docs) |
| `enrollments` | brak | brak | PRIMARY | `enrollment` (3 docs) |
| `projects` | brak | brak | PRIMARY | `project` (2 docs) |

## 4.5. Self-referencja

Klucz obcy wskazujący tę samą tabelę (np. `employees.manager_id → employees.id`):
- Wykrywany w `ForeignKeyMetadata.isSelfReference()`.
- Wymusza strategię **REFERENCE** w `UniversalTransformer.detectAndHandleCycles()` (safety net) — zapobiega nieskończonemu zagnieżdżaniu.
- Tabela zostaje w osobnej kolekcji; parent nie ma pola z child IDs (minimal reference pattern).

## 4.6. Wykrywanie cykli i obsługa

`SchemaGraph.detectCycles()` wykorzystuje algorytm Tarjana (DFS z kolorowaniem). Jeśli wykryje cykl:
- Wypisuje ostrzeżenie z listą krawędzi w cyklu.
- Automatycznie ustawia strategię **REFERENCE** dla pierwszej krawędzi w cyklu.
- Pozwala kontynuować transformację (topological sort pomija edges z cyklu).

## 4.7. Sortowanie topologiczne (Kahn's algorithm)

`SchemaGraph.topologicalSort()`:
- Oblicza `inDegree` dla każdej tabeli (liczba przychodzących FK).
- Przetwarza tabele z `inDegree=0` (liście grafu).
- Wynik: lista tabel w kolejności przetwarzania (od liści do korzeni).

Przykładowy wynik z bazy testowej:
```
Topological order: [employee_projects, projects, activity_logs,
                    employee_details, enrollments, employees, departments]
```

---

# 5. Strategie transformacji relacji

```java
public enum Strategy { EMBED, REFERENCE }
```

Strategia określa, czy powiązane dane są **zagnieżdżane** w dokumencie rodzica, czy **pozostają w osobnej kolekcji**.

## 5.1. Klucz strategii — format `child.parent`

W `db-converter.properties` klucz ma format `child_table.parent_table`:

```
relationship.strategy.activity_logs.employees=EMBED
```

Oznacza: "dla krawędzi gdzie child=`activity_logs`, parent=`employees`, użyj strategii EMBED".

`DatabaseConfig.getStrategy(fkTableName, pkTableName)` sprawdza **oba kierunki** (z tolerancją na kolejność), więc użytkownik może wpisać `employees.activity_logs` i też zadziała.

## 5.2. EMBED

Powiązane dokumenty są osadzane bezpośrednio w dokumencie rodzica:

| Kardynalność | Efekt w MongoDB |
|--------------|-----------------|
| ONE_TO_ONE | Pole obiektowe (np. `employee_details: {...}`) |
| ONE_TO_MANY | Pole tablicowe (np. `activity_logs: [{...}, {...}]`) |
| MANY_TO_MANY (junction) | Niedozwolone — wymuszane REFERENCE |

**Nazewnictwo pola osadzanego:**
- Domyślnie: nazwa tabeli child (np. `activity_logs`).
- Jeśli parent ma **więcej niż jeden FK do tego samego child**, nazwa otrzymuje suffix `_by_<fk_column>` (np. `users_by_customer_id`) — naprawione w BUG 5.

**Wymuszenia:**
- Self-ref → zawsze REFERENCE.
- Junction tables (M:N) → zawsze REFERENCE dla obu krawędzi.

## 5.3. REFERENCE

Powiązane dokumenty **nie są osadzane**. Dzieci pozostają w osobnej kolekcji z FK do rodzica. Rodzic **nie otrzymuje tablicy child IDs** — jest to **minimal reference pattern** zgodny z best practices MongoDB dla relacji 1:N.

Aby uzyskać listę child IDs u rodzica, należy użyć **Computed Pattern** (`COUNT(relation)`) lub samodzielnie wykonać aggregation pipeline w MongoDB.

## 5.4. Safeguard — automatyczne REFERENCE dla dużych tablic

`UniversalTransformer.checkUnboundedArrays()`:
- Dla każdej relacji EMBED zlicza faktyczną liczbę dzieci.
- Jeśli `childCount > safeguard.max_children_per_parent` (domyślnie 1000):
  - Wypisuje ostrzeżenie.
  - Automatycznie zmienia strategię na REFERENCE dla tej krawędzi.
- Chroni przed tworzeniem nieograniczonych tablic w MongoDB.

---

# 6. Wzorce projektowe MongoDB

System implementuje **5 wzorców** z katalogu MongoDB Design Patterns:

```
Attribute → Computed → Subset → Outlier → Approximation
```

Kolejność aplikacji jest zachowana przez `UniversalTransformer.applyPatterns()`. Approximation jest uruchamiany ostatni, aby mógł zaokrąglić również wartości liczbowe wstawione przez Computed. Outlier dodaje flagę `has_extras` zanim Approximation zaokrągli wartości w dokumencie.

## 6.1. Attribute Pattern

Grupuje wybrane kolumny w tablicę obiektów `{key, value}` — przydatne dla tabel z wieloma opcjonalnymi/rzadko używanymi polami.

**Config:**
```properties
pattern.attribute.<tabela>=<arrayName>=<col1>:<key1>,<col2>:<key2>,...
```

**Przykład:** `pattern.attribute.employee_details=personal=pesel:PESEL,birth_date:Data,bio:Bio`

**Przed (PostgreSQL):**
```
pesel:        "90010112345"
birth_date:   1990-01-01
bio:          "Programista Java..."
```

**Po (MongoDB):**
```json
{
  "personal": [
    {"key": "PESEL", "value": "90010112345"},
    {"key": "Data",  "value": ISODate("1990-01-01T00:00:00Z")},
    {"key": "Bio",   "value": "Programista Java..."}
  ]
}
```

**Implementacja:** `AttributePattern.apply()` (`core/transformer/patterns/AttributePattern.java`)

**Zachowania specjalne:**
- Kolumny źródłowe są **usuwane** z dokumentu po przeniesieniu do atrybutu (naprawione w BUG 3 — wcześniej pozostawiały null gdy wartość była nullem).
- `java.sql.Array` (np. `TEXT[]`) jest konwertowany do listy przez refleksję (PgArray).
- Daty są konwertowane do `ISODate` w UTC.

## 6.2. Computed Pattern

Pre-kalkuluje wartości zagregowane podczas transformacji — eliminuje konieczność późniejszych `$lookup` w MongoDB.

**Config:**
```properties
pattern.computed.<tabela>=<fieldName>=<FUNC>(<childTable>.<column>)
```

**Obsługiwane funkcje:** `COUNT`, `SUM`.

**Przykłady:**

```properties
# Liczba pracowników w dziale
pattern.computed.departments=employee_count=COUNT(employees.id)

# Liczba podwładnych (self-ref)
pattern.computed.employees=subordinate_count=COUNT(employees.id)

# Liczba uczestników projektu (przez junction)
pattern.computed.projects=participant_count=COUNT(employee_projects.employee_id)
```

**Wynik w MongoDB:**
```json
{
  "_id": 1,
  "name": "IT",
  "employee_count": 2,
  "employees": [...]
}
```

**Implementacja:** `ComputedPattern.apply()` (`core/transformer/patterns/ComputedPattern.java`)

**Zachowania specjalne:**
- `SUM` zwraca `Integer`/`Long`/`BigDecimal` w zależności od zakresu (naprawione w BUG 6 — wcześniej `intValue()` powodowało cichy overflow).
- Regex parsera akceptuje wielkość liter (naprawione w BUG 7).
- `String.valueOf()` fallback dla porównań `Integer/Long` (naprawione w BUG 10).

## 6.3. Subset Pattern

Osadza **N ostatnich rekordów** dzieci w głównym dokumencie — przydatne dla dużych list (np. logi, komentarze), gdzie użytkownik chce szybki dostęp do najnowszych.

**Config:**
```properties
pattern.subset.<tabela>=<childTable>=<limit>
```

**Przykład:** `pattern.subset.employees=activity_logs=3` — każdy employee dostaje pole `recent_activity_logs` z maks. 3 ostatnimi logami.

**Wynik w MongoDB:**
```json
{
  "_id": 1,
  "name": "Jan Kowalski",
  "activity_logs": [...],            // EMBED (pełna lista, jeśli włączony)
  "recent_activity_logs": [...3..]   // SUBSET (3 najnowsze)
}
```

**Implementacja:** `SubsetPattern.apply()` (`core/transformer/patterns/SubsetPattern.java`)

**Zachowania specjalne:**
- Kolumna FK w subsecie jest **usuwana** (naprawione w BUG 4 — wcześniej heurystyka `_id` suffix czasem nie trafiała).
- Limit jest walidowany (≥0, liczba całkowita) — naprawione w BUG 8/9.
- Sortowanie po `created_at` malejąco (najnowsze pierwsze).

## 6.4. Approximation Pattern

Zaokrągla wartości liczbowe do najbliższej wielokrotności podanej granularności — przydatne dla pól, które zmieniają się często, ale użytkownik nie potrzebuje pełnej precyzji (np. populacja miasta, liczba odwiedzin). Wzorzec odpowiada katalogowi MongoDB Schema Design Patterns (`approximation-schema-pattern.md`).

**Config:**
```properties
pattern.approximation.<tabela>=<field1>:<granularity1>,<field2>:<granularity2>,...
```

**Przykład:** `pattern.approximation.metrics=population:100,visits:1000`

**Przed (PostgreSQL):**
```
population: 40123
visits:     9847
```

**Po (MongoDB):**
```json
{
  "city": "New Perth",
  "population": 40100,
  "visits": 10000
}
```

**Implementacja:** `ApproximationPattern.apply()` (`core/transformer/patterns/ApproximationPattern.java`)

**Zachowania specjalne:**
- Zaokrąglanie `HALF_UP`: `40150 / 100 = 402 → 40200`, `40149 / 100 = 401 → 40100`.
- Wynik zwracany jest w najwęższym typie mieszczącym wartość (`Integer` → `Long` → `BigDecimal`) — identyczna strategia jak `ComputedPattern.computeSum` (naprawione w BUG 6 dla overflow).
- Wartości nieliczbowe (string nie-liczbowy) są pomijane bez rzucania wyjątku.
- Granularity ≤ 0 lub niepoprawna liczba są cicho ignorowane (zgodne z `AttributePattern`).
- Granularity = 1 — operacja idempotentna (wartość bez zmian).
- Konfliktów z innymi wzorcami brak — operuje na **skalarach numerycznych**, podczas gdy:
  - **Attribute** grupuje kolumny w tablicę obiektów,
  - **Computed** wstawia nowe pola zagregowane,
  - **Subset** osadza podzbiór dzieci.

**Ograniczenia:**
- Konwersja jest **stratna** — oryginalna wartość (np. `40123`) nie jest zapamiętywana. Po `PG → Mongo → PG` pole będzie miało wartość zaokrągloną (`40100`). To jest inherentna cecha wzorca (patrz `approximation-schema-pattern.md` — aplikacja zapisuje snapshoty tylko przy przekroczeniu progu).

---

## 6.5. Outlier Pattern

Identyfikuje dokumenty, które mają nadmierną liczbę dzieci (ang. *outliers*) i ogranicza rozmiar osadzonej tablicy, dodając flagę `has_extras`. Reszta danych pozostaje w osobnej kolekcji (wymagana strategia `REFERENCE`). Wzorzec odpowiada katalogowi MongoDB Design Patterns (`outlier-pattern.md`).

**Wymaganie:** Relacja child→parent musi być skonfigurowana jako `REFERENCE`. Użycie Outlier + `EMBED` = błąd transformacji. Zaimplementowano 3-warstwową walidację:

1. **Wizard krok 3 (ConsoleWizard)** — ⚠️ żółte ostrzeżenie + `reader.readLine("")` pauza po wykryciu Outlier + EMBED
2. **Wizard krok 4 — Summary (ConsoleWizard)** — czerwona sekcja "Configuration Warnings" z listą konfliktów przed zapisem pliku
3. **Runtime pre-flight (UniversalTransformer.validateOutlierConfigs)** — sprawdzenie PRZED główną pętlą przekształceń; przy konflikcie `TransformationException`, 0 kolekcji zapisanych do MongoDB

**Config:**
```properties
pattern.outlier.<tabela>=<childTable>=<threshold>
```

**Przykład:** `pattern.outlier.employees=activity_logs=3`

**Wynik w MongoDB — pracownik normalny (≤3 logi):**
```json
{
  "name": "Anna Nowak",
  "activity_logs": [
    { "id": 4, "action": "login", "ip": "10.0.0.1" }
  ],
  "has_extras": false
}
```

**Wynik — outlier (>3 logi, np. 5):**
```json
{
  "name": "Jan Kowalski",
  "activity_logs": [
    { "id": 3, "action": "logout" },
    { "id": 2, "action": "edit_file" },
    { "id": 1, "action": "login" }
  ],
  "has_extras": true
}
```

Reszta logów Jana (id 4, 5) znajduje się w osobnej kolekcji `activity_log` — aplikacja widząc `has_extras: true` może odpytać tę kolekcję po pełną historię.

**Implementacja:** `OutlierPattern.apply()` (`core/transformer/patterns/OutlierPattern.java`)

**Zachowania specjalne:**
- Wymaga strategii REFERENCE — walidowane dwutorowo: (1) pre-flight `validateOutlierConfigs()` przed główną pętlą, (2) ponownie w `parsePatternContext` przez `strategyRegistry.getStrategy()` (uwzględnia safety fuse'y: cykle, self-ref, unbounded arrays).
- Kolumna FK jest usuwana z osadzonych kopii (redundantna w kontekście rodzica).
- Flaga `has_extras: true/false` pozwala aplikacji odpytać osobną kolekcję.
- Dzieci są sortowane malejąco po PK dla deterministycznego outputu.
- Próg `threshold=0` = zawsze `has_extras: true`, pusta tablica osadzona.

**Brak konfliktów:**
- **Subset** tworzy `recent_X` — Outlier tworzy bezpośrednio `X` (różne pola).
- **Computed** liczy COUNT/SUM na wszystkich dzieciach — Outlier nie ingeruje w `rawData`.
- **Approximation** zaokrągla top-level skalary — Outlier osadza tablicę dzieci (Approximation nie wchodzi do zagnieżdżonych struktur).
- **Attribute** grupuje kolumny w key-value — Outlier nie modyfikuje kolumn.

---

# 7. Przepływ danych krok po kroku

## 7.1. Diagram sekwencji

```
Użytkownik
  │
  ▼
[java -jar ... run] ────────────▶ Main.main()
                                       │
                                       ▼
                                  Picocli dispatch ──▶ RunCommand.call()
                                                          │
                                                          ▼
                                                    ETLPipeline.execute()
                                                          │
                          ┌───────────────────────────────┼───────────────────────────────┐
                          │                               │                               │
                          ▼                               ▼                               ▼
              loadConfig(db-converter.properties)  SchemaExtractor.extractSchema()   DataExtractor.extractAll()
                          │                               │                               │
                          │                               ▼                               │
                          │                       SchemaGraph (toposort)                │
                          │                               │                               │
                          └───────────────────────────────┴───────────────────────────────┘
                                                          │
                                                          ▼
                                              UniversalTransformer.transform()
                                                          │
                                 ┌─────────────────────────┼──────────────────────────────┐
                                 ▼                         ▼                              ▼
                           BuildRelIndex           For each table:                ApplyPatterns v60s
                           (parent→children)        For each row:              (Attribute → Computed →
                                                     ApplyEmbed/Reference       Subset → Outlier → Approx)
                                                          │
                                                          ▼
                                                    MongoDBLoader.load()
                                                          │
                                                          ▼
                                                    Summary print
```

## 7.2. Szczegóły `ETLPipeline.execute()`

| Krok | Metoda | Opis |
|------|--------|------|
| 1 | `loadConfig()` | Wczytanie `db-converter.properties` do `DatabaseConfig` |
| 2 | `validateConnections()` | Test JDBC + MongoDB (szybki ping) |
| 3 | `SchemaExtractor.extractSchema()` | JDBC `DatabaseMetaData` → `SchemaGraph` |
| 4 | `DataExtractor.extractAll(graph)` | `SELECT * FROM ...` → `Map<String, List<Map>>` |
| 5 | `Transformer.transform(graph, data, config)` | Toposort + per-table relacje + wzorce |
| 6 | `MongoDBLoader.load(transformed, graph)` | Insert per kolekcja + indeksy |
| 7 | `printSummary()` | Tabele, dokumenty, czas trwania |

## 7.3. Szczegóły `UniversalTransformer.transform()`

Przed główną pętlą, `transform()` wykonuje sekwencyjnie 3 kroki przygotowawcze:

1. **Wykrywanie cykli** — `detectAndHandleCycles()` wymusza REFERENCE dla self-ref i cykli (safety net).
2. **Safeguard unbounded arrays** — `checkUnboundedArrays()` downgrade EMBED→REFERENCE dla zbyt dużych tablic.
3. **Pre-flight Outlier** — `validateOutlierConfigs()` rzuca `TransformationException` jeśli Outlier + EMBED na tej samej relacji.

Następnie dla każdej tabeli w kolejności topologicznej:

1. **Indeks relacji** — `buildRelationshipIndex()` tworzy `Map<childTable.fkColumn, Map<parentPkValue, List<childRow>>>` dla O(1) lookupów dzieci.

2. **Dla każdego wiersza rodzica:**
   - Skopiuj pola z `rawData[parentTable]`.
   - **Dla każdej krawędzi wchodzącej** (incoming FK):
     - Pobierz strategię z `EdgeStrategyRegistry`.
     - Pobierz dzieci z indeksu.
     - Jeśli `EMBED`:
       - `applyEmbedStrategy()` → wybiera `applyOneToOne` / `applyOneToMany` wg kardynalności.
       - `buildChildFieldName()` — unikalna nazwa pola (suffix `_by_<fk_col>` przy kolizji).
     - Jeśli `REFERENCE`:
       - Pomijanie — dzieci zostają w osobnej kolekcji z FK.
   - **Zastosuj wzorce** — `applyPatterns()` (Attribute → Computed → Subset → Outlier → Approximation):
     - Dla każdego włączonego `pattern.<type>.<table>`:
       - `parsePatternContext()` parsuje config → context map.
       - `applier.apply(document, context)` modyfikuje dokument.
   - Dodaj do listy transformed.

## 7.4. Szczegóły `MongoDBLoader.load()`

Dla każdej kolekcji docelowej:
1. `dropCollection()` (clean start).
2. `bulkWrite()` z wsadem dokumentów.
3. `createIndex({pkCol: 1})` — unikalny indeks na oryginalnym PK.
4. `createIndex({fkCol: 1})` — dla każdej krawędzi REFERENCE (szybki lookup).

Kolekcje typu `CHILD_ENTITY` z EMBED są **pomijane** przy ładowaniu (już są w parent doc).

---

# 8. Struktura plików i odpowiedzialności klas

## 8.1. Entry point i CLI

| Plik | Odpowiedzialność |
|------|-----------------|
| `Main.java` | Entry point, parsowanie CLI przez Picocli |
| `cli/commands/RunCommand.java` | Komenda `run` — pełna migracja |
| `cli/commands/WizardCommand.java` | Komenda `wizard` — TUI kreator |
| `cli/commands/ValidateCommand.java` | Komenda `validate` — test połączeń |
| `cli/commands/ReportCommand.java` | Komenda `report` — generowanie raportu HTML |

## 8.2. Połączenia (connection)

| Plik | Odpowiedzialność |
|------|-----------------|
| `IDatabaseConnector.java` | Interfejs: `connect()`, `disconnect()`, `isConnected()` |
| `JDBCConnection.java` | Połączenie JDBC (PostgreSQL) przez `DriverManager` |
| `MongoDBConnection.java` | Połączenie MongoDB sync driver 5.x |

## 8.3. Ekstrakcja (extractor)

| Plik | Odpowiedzialność |
|------|-----------------|
| `JDBCSchemaExtractor.java` | JDBC `DatabaseMetaData` → tabele, kolumny, PK, FK, kardynalność |
| `JDBCDataExtractor.java` | `SELECT * FROM ...` → `Map<String, List<Map<String,Object>>>` + konwersja typów |

## 8.4. Transformacja (transformer)

| Plik | Odpowiedzialność |
|------|-----------------|
| `UniversalTransformer.java` | Orkiestracja: indeks relacji, EMBED/REFERENCE, wzorce, cykle, safeguard |
| `patterns/PatternApplier.java` | Interfejs: `apply(Map, Map context)` |
| `patterns/AttributePattern.java` | Implementacja Attribute |
| `patterns/ComputedPattern.java` | Implementacja Computed (COUNT/SUM) |
| `patterns/SubsetPattern.java` | Implementacja Subset |
| `patterns/ApproximationPattern.java` | Implementacja Approximation (HALF_UP rounding) |
| `patterns/OutlierPattern.java` | Implementacja Outlier (threshold + has_extras) |

## 8.5. Ładowanie (loader)

| Plik | Odpowiedzialność |
|------|-----------------|
| `MongoDBLoader.java` | `bulkWrite`, indeksy, pomijanie EMBED children |

## 8.6. Modele (model)

| Plik | Odpowiedzialność |
|------|-----------------|
| `TableMetadata.java` | Tabela: nazwa, schema, kolumny, PK, FK, typ tabeli |
| `ColumnMetadata.java` | Kolumna: nazwa, typ SQL/JSBC, nullable, PK/FK flagi |
| `ForeignKeyMetadata.java` | FK: pkTable, pkColumn, fkTable, fkColumn, kardynalność |
| `SchemaGraph.java` | Graf relacji, topological sort, cycle detection |
| `Strategy.java` | Enum: EMBED, REFERENCE |
| `Cardinality.java` | Enum: ONE_TO_ONE, ONE_TO_MANY, MANY_TO_MANY |
| `TableType.java` | Enum: PRIMARY_ENTITY, CHILD_ENTITY, JUNCTION_TABLE |

## 8.7. Konfiguracja

| Plik | Odpowiedzialność |
|------|-----------------|
| `DatabaseConfig.java` | Load/save `.properties`, per-edge strategie, per-table wzorce |
| `EdgeStrategyRegistry.java` | Lookup strategii dla pary tabel, fallback do default |

## 8.8. UI (TUI)

| Plik | Odpowiedzialność |
|------|-----------------|
| `ConsoleWizard.java` | 4-krokowy kreator, JLine3 raw mode, table navigation |
| `TerminalRenderer.java` | Helpery do formatowania tabel i ANSI markup |

## 8.9. Serwisy (service)

| Plik | Odpowiedzialność |
|------|-----------------|
| `ETLPipeline.java` | Główny orkiestrator: config → schema → data → transform → load |

## 8.10. Narzędzia (util)

| Plik | Odpowiedzialność |
|------|-----------------|
| `DateUtils.java` | Konwersje dat: Timestamp/LocalDate/LocalDateTime/Instant/PgArray/PGobject |
| `StringUtils.java` | Sanityzacja stringów (escape dla MongoDB, error sanitization) |

## 8.11. Raport (report)

| Plik | Odpowiedzialność |
|------|-----------------|
| `report/HtmlReportGenerator.java` | Generuje samodzielny plik HTML z porównaniem źródła (schemat + sample data) i celu (kolekcje + sample dokumenty), mapowaniem relacji i konfiguracją wzorców |

## 8.12. Wyjątki (exception)

| Plik | Kiedy rzucany |
|------|---------------|
| `ConverterException.java` | Bazowy dla wszystkich |
| `ConnectionException.java` | Błąd połączenia (bazowy) |
| `SourceConnectionException.java` | Błąd połączenia do źródła (PostgreSQL) |
| `TargetConnectionException.java` | Błąd połączenia do celu (MongoDB) |
| `ConfigException.java` | Błąd parsowania/odczytu konfiguracji |
| `SchemaException.java` | Błąd ekstrakcji schematu (np. brak tabel) |
| `TransformationException.java` | Błąd transformacji (wzorce, kardynalność) |
| `CycleDetectedException.java` | Cykl w grafie (po auto-fixie) |
| `UnboundedArrayException.java` | Tablica dzieci przekracza safeguard |
| `PatternException.java` | Błąd parsowania/zastosowania wzorca |

---

# 9. Testowanie

## 9.1. Statystyki

| Kategoria | Liczba |
|-----------|--------|
| Testy jednostkowe | 136 |
| Testy integracyjne | 3 (FullMigrationIntegrationTest, MongoDBLoaderTest) |
| **Łącznie** | **139** |

## 9.2. Testy jednostkowe (JUnit 5 + AssertJ)

| Klasa testowa | Testowana klasa |
|---------------|----------------|
| `UniversalTransformerTest` | `UniversalTransformer` (5 wzorców, EMBED/REFERENCE, self-ref) |
| `ManyToManyTransformerTest` | M:N w transformer |
| `JDBCSchemaExtractorTest` | `JDBCSchemaExtractor` |
| `JDBCDataExtractorTest` | `JDBCDataExtractor` |
| `MongoDBLoaderTest` | `MongoDBLoader` |
| `SchemaGraphTest` | `SchemaGraph` |
| `TableMetadataTest` | `TableMetadata` |
| `ColumnMetadataTest` | `ColumnMetadata` |
| `ForeignKeyMetadataTest` | `ForeignKeyMetadata` |
| `DatabaseConfigTest` | `DatabaseConfig` |
| `EdgeStrategyRegistryTest` | `EdgeStrategyRegistry` |
| `DateUtilsTest` | `DateUtils` |
| `StringUtilsTest` | `StringUtils` |
| `AttributePatternTest` | `AttributePattern` |
| `ComputedPatternTest` | `ComputedPattern` |
| `SubsetPatternTest` | `SubsetPattern` |
| `ApproximationPatternTest` | `ApproximationPattern` |
| `OutlierPatternTest` | `OutlierPattern` |
| `ExceptionHandlingTest` | Hierarchia wyjątków |
| `AppTest` | Smoke test |

## 9.3. Testy integracyjne

| Klasa | Opis |
|-------|------|
| `FullMigrationIntegrationTest` | Pełen przebieg: PostgreSQL seed → konwersja → MongoDB weryfikacja |

Wymaga działających kontenerów Docker (`docker compose up -d`).

## 9.4. Uruchomienie

```bash
# Tylko testy jednostkowe
mvn test -Dtest='!*IntegrationTest'

# Wszystkie testy (wymaga Docker)
mvn test

# Konkretna klasa
mvn test -Dtest=UniversalTransformerTest
```

## 9.5. Pokrycie funkcjonalne

| Funkcja | Pokrycie |
|---------|----------|
| Schema extraction (tabele, FK, PK) | ✅ |
| Kardynalność 1:1 / 1:N / M:N | ✅ |
| Cykle i self-ref | ✅ |
| Topological sort | ✅ |
| EMBED 1:1 / 1:N | ✅ |
| REFERENCE z M:N junction | ✅ |
| Attribute pattern | ✅ |
| Computed COUNT | ✅ |
| Computed SUM | ✅ |
| Subset pattern | ✅ |
| Approximation pattern | ✅ |
| Outlier pattern (threshold + has_extras) | ✅ |
| EMBED+Outlier pre-flight validation | ✅ |
| Timezone handling | ✅ |
| Integer/Long comparison | ✅ |
| Composite PK nie-klasyfikowane jako 1:1 | ✅ |

---

# 10. Znane ograniczenia

1. **Kierunek jednokierunkowy** — system konwertuje wyłącznie PostgreSQL → MongoDB. Brak odwrotnego kierunku (MongoDB → PostgreSQL).

2. **Brak polecenia zatrzymania (graceful shutdown)** — `Ctrl+C` podczas `run` przerywa bez rollbacku; kolekcje MongoDB mogą być w stanie częściowym.

3. **REFERENCE bez parent refs** — przy strategii REFERENCE parent nie otrzymuje tablicy child IDs (minimal reference). Aby uzyskać listę IDs u rodzica, należy użyć **Computed COUNT** lub wykonać aggregation w MongoDB.

4. **Subset + EMBED razem** — jeśli parent ma strategię EMBED dla child oraz Subset pattern, oba pola będą obecne (`child` pełna lista + `recent_child` podzbiór). System nie wykrywa tego konfliktu — to świadoma decyzja projektowa.

5. **Computed COUNT przez junction** — liczy dokumenty w junction, nie unikalne encje po drugiej stronie. Dla `employee_projects.employee_id` z `COUNT()` zwraca liczbę wierszy junction (np. 2 dla pracownika w 2 projektach), a nie unikalnych projektów.

6. **Wzorce dla tabel JUNCTION_TABLE** — system nie blokuje konfiguracji wzorców dla junction, ale ich efekt może być nieintuicyjny (np. `subset` na junction nie ma sensu).

7. **Brak auto-walidacji konfliktu EMBED+Subset** — patrz punkt 4.

8. **Composite PK w 1:1** — choć naprawione dla M:N, system może błędnie sklasyfikować composite PK jako 1:1 jeśli tabela ma dokładnie 1 FK i composite PK złożony z (fkCol, inneKolumny). Edge case rzadki w praktyce.

9. **Brak retry na chwilowe błędy sieciowe** — timeout JDBC/Mongo powoduje pełne przerwanie.

10. **Wizard nie obsługuje composite FK jako M:N** — jeśli junction ma composite FK (3+ kolumn), wizard wyświetli to jako standardowe FK.

---

# 11. Wykryte i naprawione błędy

Podczas developmentu i testów zidentyfikowano 12 błędów, z czego 10 zostało naprawionych. Poniżej lista wraz z priorytetami.

| # | Priorytet | Opis | Plik | Status |
|---|-----------|------|------|--------|
| 1 | P0 | Composite PK junction → 1:1 → utrata danych | `JDBCSchemaExtractor.determineCardinality` | ✅ Naprawiony |
| 2 | P0 | Pattern mógł usunąć kolumnę FK | `UniversalTransformer` | ⚠️ Wymaga głębokiego refactoru |
| 3 | P0 | Attribute pattern zostawiał null przy null values | `AttributePattern.apply` | ✅ Naprawiony |
| 4 | P1 | Subset pattern używał heurystyki `_id` suffix | `SubsetPattern` + context | ✅ Naprawiony |
| 5 | P1 | Field name collision gdy parent ma 2+ FKs do child | `buildChildFieldName` | ✅ Naprawiony |
| 6 | P1 | Computed SUM `intValue()` overflow | `ComputedPattern.computeSum` | ✅ Naprawiony |
| 7 | P2 | Computed pattern regex case-sensitive | `parsePatternContext` | ✅ Naprawiony |
| 8 | P2 | Subset limit ujemny → `subList(0, -1)` crash | `parsePatternContext` | ✅ Naprawiony |
| 9 | P2 | Subset limit nie-numeryczny → NFE | `parsePatternContext` | ✅ Naprawiony |
| 10 | P2 | `Integer.equals(Long)` cicho odfiltrowywał dzieci | `parsePatternContext` | ✅ Naprawiony |
| 11 | P3 | Attribute — malformed mappings cicho ignorowane | `AttributePattern` | 🔜 Do rozważenia |
| 12 | P3 | Computed `matches()` wymagał pełnego match (whitespace) | `parsePatternContext` | ✅ Naprawiony (`.trim()`) |

## 11.1. Szczegóły napraw

### BUG 1 — Composite PK → 1:1 (P0)

**Problem:** `getIndexInfo(unique=true)` zwraca 1 wiersz na kolumnę indeksu. Dla `PRIMARY KEY(employee_id, project_id)` zwraca 2 wiersze — oba z `NON_UNIQUE=false`. Stara implementacja widziała pierwszy i zwracała `ONE_TO_ONE`, powodując utratę danych w junction (zostawał tylko `children.get(0)`).

**Fix:** `determineCardinality()` grupuje wyniki po `INDEX_NAME` i sprawdza czy jest to single-column index. Tylko jednokolumnowe UNIQUE/PK → `ONE_TO_ONE`.

### BUG 3 — Attribute null (P0)

**Problem:** W `AttributePattern.apply()` kolumna źródłowa była usuwana z dokumentu tylko gdy wartość była nie-null. Przy null wartościach powstawały niespójne dokumenty (atrybut przeniesiony, oryginalna kolumna zostawała z null).

**Fix:** Kolumna źródłowa jest **zawsze** usuwana po przeniesieniu do atrybutu, niezależnie od wartości.

### BUG 4 — Subset FK heuristic (P1)

**Problem:** `SubsetPattern` heurystycznie szukał kolumny FK po sufiksie `_id` w danych subset. Przy niestandardowych nazwach kolumn FK heurystyka nie trafiała.

**Fix:** `parsePatternContext` dla subset przekazuje `fkColumn` przez context map. `SubsetPattern.apply` używa jawnej nazwy kolumny z context zamiast heurystyki.

### BUG 5 — Field name collision (P1)

**Problem:** Gdy parent miał 2+ FKs do tego samego child (np. `users` z `customer_id` i `seller_id`), oba embedowały się pod tą samą nazwą pola (`users`), nadpisując się nawzajem.

**Fix:** `buildChildFieldName()` sprawdza liczbę incoming edges z tego samego child table. Jeśli >1, nazwa pola otrzymuje suffix `_by_<fkColumn>` (np. `users_by_customer_id`).

### BUG 6 — SUM overflow (P1)

**Problem:** `computeSum()` zwracał wynik przez `sum.intValue()` bez sprawdzenia zakresu. Dla dużych sum (np. 5 mld) cicho obcinał do `Integer.MAX_VALUE`.

**Fix:** Sprawdzenie `compareTo(Integer.MAX_VALUE)` / `Long.MAX_VALUE`. Jeśli mieści się w `int` → int, jeśli w `long` → long, w przeciwnym razie `BigDecimal`.

### BUG 7 — Case-sensitive regex (P2)

**Problem:** Regex `(FUNC)\((child)\.(column))` akceptował tylko wielkie litery. `count(orders.id)` nie działało.

**Fix:** Regex zmieniony na `[A-Za-z]+` + `funcExpr.trim()`.

### BUG 8/9 — Subset limit validation (P2)

**Problem:** Ujemny limit (`-1`) powodował `subList(0, -1)` → `IllegalArgumentException`. Nie-numeryczny limit powodował `NumberFormatException`. Oba przypadki crashowały całą transformację.

**Fix:** `try/catch` wokół `Integer.parseInt()` + sprawdzenie `limit >= 0`. Przy błędzie rzucany `TransformationException` z czytelnym komunikatem.

### BUG 10 — Integer/Long comparison (P2)

**Problem:** `parentPk.equals(c.get(fkCol))` zwracał `false` gdy `parentPk` był `Integer` a `c.get(fkCol)` zwracał `Long` (lub odwrotnie). JDBC zwraca różne typy dla tego samego pola w zależności od rozmiaru. Skutkowało cichym odfiltrowaniem wszystkich dzieci — `COUNT` zwracał 0, `SUM` zwracał 0.

**Fix:** Dodany fallback `String.valueOf(parentPk).equals(String.valueOf(childFk))` — string comparison zawsze trafia.

### BUG 12 — Whitespace w Computed (P3)

**Problem:** `matcher.matches()` wymaga pełnego match. `COUNT(orders.id)` z trailing whitespace nie matchowało.

**Fix:** `funcExpr.trim()` przed matching.

## 11.2. Błędy świadomie nie naprawione

### BUG 2 — Pattern niszczy FK index (P0, ale złożone)

**Problem teoretyczny:** Jeśli pattern.Attribute targetuje kolumnę FK, parent traci możliwość znalezienia dzieci. W praktyce: atrybuty targetują pola biznesowe (PESEL, bio), nie FK. Walidacja byłaby nadmiarowa.

**Status:** Wymaga przebudowy indeksów (parent → children) tak, aby był budowany PRZED zastosowaniem wzorców, oraz walidacji pattern config vs FK columns. Obecny kod buduje indeks z `rawData` (dane surowe), więc technicznie nie jest zepsuty — tylko walidacja nie chroni przed błędną konfiguracją użytkownika.

### BUG 11 — Malformed attribute mappings (P3)

**Problem:** `release_US:USA,release_France` (brak drugiego dwukropka) jest cicho ignorowane. Połowa configa nie działa.

**Status:** Wymaga dodania `logger.warn()` dla malformed entries. Niska wartość biznesowa (błąd użytkownika, łatwy do debugowania).

---

## Dodatek A — Szybki test end-to-end

```bash
# 1. Czyste środowisko
docker compose down -v && docker compose up -d

# 2. Czekaj aż seed się załaduje (~10s)
until docker exec todbconverter-postgres-test psql -U postgres -d source_db -c "SELECT count(*) FROM employees" 2>/dev/null | grep -q 3; do sleep 1; done

# 3. Wygeneruj config wizardem
java -jar target/to-db-converter-1.0.0.jar wizard

# 4. Migracja
java -jar target/to-db-converter-1.0.0.jar run

# 5. Weryfikacja
docker exec todbconverter-mongo-test mongosh "mongodb://admin:password@localhost:27017/mydb_converted?authSource=admin" \
  --quiet --eval 'db.getCollectionNames().forEach(n => print(n + ": " + db[n].countDocuments() + " docs"))'
```

Oczekiwany output (5 kolekcji, 14 dokumentów):
```
employee: 3
department: 3
enrollment: 3
project: 2
employee_project: 3
```

## Dodatek B — Przykładowe dane wyjściowe

Poniżej rzeczywiste dokumenty wygenerowane przez system na bazie seed (3 pracowników, 2 projekty, 2 działy):

**`department` (IT):**
```json
{
  "id": 1,
  "name": "IT",
  "location": "Warszawa",
  "employees": [
    {"id": 1, "name": "Jan Kowalski", "manager_id": null, ...},
    {"id": 2, "name": "Anna Nowak", "manager_id": 1, ...}
  ],
  "employee_count": 2
}
```

**`employee` (Jan):**
```json
{
  "id": 1,
  "name": "Jan Kowalski",
  "email": "jan@firma.pl",
  "department_id": 1,
  "manager_id": null,
  "activity_logs": [/* 3 wpisy EMBED */],
  "employee_details": {
    "id": 1,
    "personal": [
      {"key": "PESEL", "value": "90010112345"},
      {"key": "Data", "value": ISODate("1990-01-01T00:00:00Z")},
      {"key": "Bio", "value": "Programista Java..."}
    ]
  },
  "subordinate_count": 2,
  "recent_activity_logs": [/* 3 ostatnie SUBSET */]
}
```

**`project` (Alpha):**
```json
{
  "id": 1,
  "name": "Project Alpha",
  "metadata": {"budget": 100000, "tags": ["IT", "dev"]},
  "config": BinData(0, "AAEC"),
  "participant_count": 2
}
```

**`employee_project` (junction):**
```json
[
  {"employee_id": 1, "project_id": 1},
  {"employee_id": 1, "project_id": 2},
  {"employee_id": 2, "project_id": 1}
]
```

**`enrollment` (z Attribute pattern na text[]):**
```json
{
  "id": 1,
  "student_id": 1,
  "course_id": 101,
  "details": [
    {"key": "Notatki", "value": ["Bardzo dobry", "Polecam"]},
    {"key": "Ocena", "value": 4.5},
    {"key": "Data", "value": ISODate("2026-06-01T20:31:18.375Z")}
  ]
}
```

---

*Dokumentacja przygotowana dla projektu TO DB Converter v1.0.0.*

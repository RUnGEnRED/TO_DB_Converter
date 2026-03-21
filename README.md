# TO DB Converter

Narzêdzie konwertuj¹ce dane z relacyjnej bazy danych (PostgreSQL) do bazy dokumentowej (MongoDB) ze zagnie¿d¿aniem obiektów.

## Opis projektu

Aplikacja w jêzyku Java, która dynamicznie analizuje strukturê relacyjnej bazy danych, pobiera dane wraz z metadanymi o relacjach i przekszta³ca je do zagnie¿d¿onych dokumentów JSON w MongoDB.

## Technologie

- **Jêzyk:** Java 17
- **Zarz¹dzanie projektem:** Maven
- **Bazy danych:** 
  - PostgreSQL 15 (Ÿród³o - model relacyjny)
  - MongoDB 7 (cel - model dokumentowy)
- **Biblioteki:**
  - PostgreSQL JDBC Driver
  - MongoDB Java Driver
  - Jackson (mapowanie JSON)
  - SLF4J (logowanie)

## Uruchomienie projektu

### Wymagania wstêpne
- Docker Desktop zainstalowany i uruchomiony
- Java JDK 17 lub nowsza
- Maven 3.8+

### Krok 1: Uruchomienie baz danych

```bash
docker-compose up -d
```

Sprawdzenie, czy kontenery dzia³aj¹:
```bash
docker ps
```

Powinny byæ uruchomione:
- `to_db_postgres` (port 5432)
- `to_db_mongodb` (port 27017)

### Krok 2: Budowanie projektu

```bash
mvn clean install
```

### Krok 3: Uruchomienie aplikacji

```bash
mvn exec:java -Dexec.mainClass="com.todbconverter.Main"
```

lub po zbudowaniu JAR:

```bash
java -jar target/to-db-converter-1.0-SNAPSHOT.jar
```
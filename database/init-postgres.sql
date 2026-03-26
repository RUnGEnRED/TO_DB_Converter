-- PostgreSQL database creation script

CREATE TABLE klient (
    id SERIAL PRIMARY KEY,
    imie VARCHAR(100) NOT NULL,
    nazwisko VARCHAR(100) NOT NULL,
    email VARCHAR(150) UNIQUE NOT NULL,
    telefon VARCHAR(20),
    data_rejestracji DATE
);

CREATE TABLE zamowienie (
    id SERIAL PRIMARY KEY,
    klient_id INTEGER NOT NULL,
    data_zamowienia TIMESTAMP,
    status VARCHAR(50) DEFAULT 'nowe',
    wartosc_calkowita DECIMAL(10, 2),
    FOREIGN KEY (klient_id) REFERENCES klient(id) ON DELETE CASCADE
);

CREATE TABLE produkt (
    id SERIAL PRIMARY KEY,
    nazwa VARCHAR(200) NOT NULL,
    opis TEXT,
    cena DECIMAL(10, 2) NOT NULL,
    ilosc_w_magazynie INTEGER DEFAULT 0
);

CREATE TABLE pozycja_zamowienia (
    id SERIAL PRIMARY KEY,
    zamowienie_id INTEGER NOT NULL,
    produkt_id INTEGER NOT NULL,
    ilosc INTEGER NOT NULL,
    cena_jednostkowa DECIMAL(10, 2) NOT NULL,
    FOREIGN KEY (zamowienie_id) REFERENCES zamowienie(id) ON DELETE CASCADE,
    FOREIGN KEY (produkt_id) REFERENCES produkt(id)
);

INSERT INTO klient (imie, nazwisko, email, telefon, data_rejestracji) VALUES
('Jan', 'Kowalski', 'jan.kowalski@example.com', '123456789', '2024-01-15'),
('Anna', 'Nowak', 'anna.nowak@example.com', '987654321', '2024-02-20'),
('Piotr', 'Wisniewski', 'piotr.wisniewski@example.com', '555123456', '2024-03-10');

INSERT INTO produkt (nazwa, opis, cena, ilosc_w_magazynie) VALUES
('Laptop Dell', 'Laptop biznesowy 15 cali', 3499.99, 10),
('Mysz Logitech', 'Mysz bezprzewodowa', 99.99, 50),
('Klawiatura mechaniczna', 'Klawiatura gamingowa RGB', 299.99, 25),
('Monitor Samsung 27', 'Monitor 4K', 1299.99, 15),
('Sluchawki Sony', 'Sluchawki z redukcja szumow', 899.99, 30);

INSERT INTO zamowienie (klient_id, data_zamowienia, status, wartosc_calkowita) VALUES
(1, '2024-06-10 10:30:00', 'zrealizowane', 3599.98),
(1, '2024-06-15 14:20:00', 'w trakcie', 1599.98);

INSERT INTO zamowienie (klient_id, data_zamowienia, status, wartosc_calkowita) VALUES
(2, '2024-06-12 09:15:00', 'zrealizowane', 399.98);

INSERT INTO zamowienie (klient_id, data_zamowienia, status, wartosc_calkowita) VALUES
(3, '2024-06-18 16:45:00', 'nowe', 2199.98);

INSERT INTO pozycja_zamowienia (zamowienie_id, produkt_id, ilosc, cena_jednostkowa) VALUES
(1, 1, 1, 3499.99),
(1, 2, 1, 99.99);

INSERT INTO pozycja_zamowienia (zamowienie_id, produkt_id, ilosc, cena_jednostkowa) VALUES
(2, 4, 1, 1299.99),
(2, 3, 1, 299.99);

INSERT INTO pozycja_zamowienia (zamowienie_id, produkt_id, ilosc, cena_jednostkowa) VALUES
(3, 3, 1, 299.99),
(3, 2, 1, 99.99);

INSERT INTO pozycja_zamowienia (zamowienie_id, produkt_id, ilosc, cena_jednostkowa) VALUES
(4, 1, 1, 3499.99),
(4, 5, 1, 899.99);

SELECT 'Klienci' as tabela, COUNT(*) as liczba FROM klient
UNION ALL
SELECT 'Produkty', COUNT(*) FROM produkt
UNION ALL
SELECT 'Zamówienia', COUNT(*) FROM zamowienie
UNION ALL
SELECT 'Pozycje zamówień', COUNT(*) FROM pozycja_zamowienia;

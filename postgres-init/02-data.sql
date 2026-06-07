-- =============================================
-- TO_DB Converter - Przykładowe dane testowe
-- =============================================

-- Dzialy
INSERT INTO departments (name, location) VALUES
('IT', 'Warszawa'),
('HR', 'Krakow'),
('Marketing', 'Gdansk');

-- Pracownicy
INSERT INTO employees (name, email, department_id, manager_id) VALUES
('Jan Kowalski', 'jan@firma.pl', 1, NULL),
('Anna Nowak', 'anna@firma.pl', 1, 1),
('Piotr Wisniewski', 'piotr@firma.pl', 2, 1);

-- Szczegoly pracownikow (1:1)
INSERT INTO employee_details (employee_id, pesel, birth_date, bio) VALUES
(1, '90010112345', '1990-01-01', 'Programista Java z 5-letnim doswiadczeniem'),
(2, '85050567890', '1985-05-05', 'Specjalista HR');

-- Projekty z danymi JSONB
INSERT INTO projects (name, metadata, config) VALUES
('Project Alpha', '{"budget": 100000, "tags": ["IT", "dev"]}', E'\\x000102'),
('Project Beta', '{"budget": 50000, "tags": ["marketing"]}', E'\\x030405');

-- Przypisanie pracownikow do projektow (N:M - Pure Junction)
INSERT INTO employee_projects (employee_id, project_id) VALUES
(1, 1),
(1, 2),
(2, 1);

-- Zapisy na kursy (N:M - Payload Junction)
INSERT INTO enrollments (student_id, course_id, grade, notes) VALUES
(1, 101, 4.5, ARRAY['Bardzo dobry', 'Polecam']),
(1, 102, 5.0, ARRAY['Swietny kurs']),
(2, 101, 3.5, ARRAY['Trudny ale wart']);

-- Logi aktywnosci (potencjalnie unbounded array)
INSERT INTO activity_logs (employee_id, action, details) VALUES
(1, 'login', '{"ip": "192.168.1.1", "browser": "Chrome"}'),
(1, 'edit_file', '{"file": "config.xml", "lines_changed": 15}'),
(1, 'logout', '{"ip": "192.168.1.1"}'),
(2, 'login', '{"ip": "10.0.0.1", "browser": "Firefox"}');

-- =============================================
-- TO_DB Converter - Przykładowa baza testowa
-- =============================================

-- Tabela nadrzedna (Primary Entity)
CREATE TABLE departments (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    location VARCHAR(100)
);

-- Tabela z 1:N (Child Entity)
CREATE TABLE employees (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(100) UNIQUE,
    department_id INT REFERENCES departments(id),
    manager_id INT REFERENCES employees(id)  -- samoreferencja!
);

-- Tabela z 1:1 (Child Entity z UNIQUE FK)
CREATE TABLE employee_details (
    id SERIAL PRIMARY KEY,
    employee_id INT UNIQUE NOT NULL REFERENCES employees(id),
    pesel VARCHAR(11),
    birth_date DATE,
    bio TEXT
);

-- Tabela z danymi JSONB (zaawansowany typ PostgreSQL)
CREATE TABLE projects (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    metadata JSONB,  -- { "budget": 100000, "tags": ["IT", "dev"] }
    config BYTEA     -- dane binarne
);

-- Junction table N:M (Pure)
CREATE TABLE employee_projects (
    employee_id INT REFERENCES employees(id),
    project_id INT REFERENCES projects(id),
    PRIMARY KEY (employee_id, project_id)
);

-- Junction table N:M (Payload)
CREATE TABLE enrollments (
    id SERIAL PRIMARY KEY,
    student_id INT,
    course_id INT,
    grade DECIMAL(3,1),
    enrolled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    notes TEXT[]
);

-- Tabela z potencjalnym unbounded array
CREATE TABLE activity_logs (
    id SERIAL PRIMARY KEY,
    employee_id INT REFERENCES employees(id),
    action VARCHAR(50),
    details JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

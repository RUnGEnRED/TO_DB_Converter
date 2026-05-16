-- =============================================================================
-- TO DB Converter - Test Database Schema (Idempotent)
-- =============================================================================
-- Designed to test ALL converter features:
--  1:N  = orders -> order_items (classic parent-child)
--  M:N  = students <-> courses via enrollments (junction table)
--  M:N  = actors <-> movies via movie_roles (junction table with extra data)
--  1:1  = employee_details (FK + UNIQUE)
--  self = employees.manager_id -> employees.employee_id
--  composite PK = order_versions(order_id, version)
--  special types = JSONB, TEXT[], BYTEA in documents
--  NULL handling = projects, project_tasks
--  reserved words = "user" table with "select", "from" columns
-- =============================================================================

BEGIN;

-- =============================================================================
-- DROP all tables (idempotent - allows re-run)
-- =============================================================================
DROP TABLE IF EXISTS enrollments      CASCADE;
DROP TABLE IF EXISTS movie_roles      CASCADE;
DROP TABLE IF EXISTS order_items      CASCADE;
DROP TABLE IF EXISTS order_versions   CASCADE;
DROP TABLE IF EXISTS project_tasks    CASCADE;
DROP TABLE IF EXISTS employee_details CASCADE;
DROP TABLE IF EXISTS orders           CASCADE;
DROP TABLE IF EXISTS projects         CASCADE;
DROP TABLE IF EXISTS documents        CASCADE;
DROP TABLE IF EXISTS configuration    CASCADE;
DROP TABLE IF EXISTS employees        CASCADE;
DROP TABLE IF EXISTS products         CASCADE;
DROP TABLE IF EXISTS customers        CASCADE;
DROP TABLE IF EXISTS courses          CASCADE;
DROP TABLE IF EXISTS students         CASCADE;
DROP TABLE IF EXISTS actors           CASCADE;
DROP TABLE IF EXISTS movies           CASCADE;
DROP TABLE IF EXISTS user_groups      CASCADE;
DROP TABLE IF EXISTS "user"           CASCADE;

-- =============================================================================
-- SECTION 1: ONE-TO-MANY (Classic Order System)
-- =============================================================================
-- customers 1──N orders 1──N order_items
-- products  1──N order_items

CREATE TABLE customers (
    customer_id SERIAL PRIMARY KEY,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    email VARCHAR(150) UNIQUE NOT NULL,
    phone VARCHAR(20),
    registration_date DATE,
    notes TEXT
);

CREATE TABLE products (
    product_id SERIAL PRIMARY KEY,
    name VARCHAR(200) NOT NULL,
    description TEXT,
    price DECIMAL(10, 2) NOT NULL,
    stock_quantity INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL REFERENCES customers(customer_id) ON DELETE CASCADE,
    order_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(50) DEFAULT 'NEW',
    total_amount DECIMAL(10, 2)
);

CREATE TABLE order_items (
    item_id SERIAL PRIMARY KEY,
    order_id INTEGER NOT NULL REFERENCES orders(order_id) ON DELETE CASCADE,
    product_id INTEGER NOT NULL REFERENCES products(product_id),
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    unit_price DECIMAL(10, 2) NOT NULL
);

-- =============================================================================
-- SECTION 2: MANY-TO-MANY via Junction (Students <-> Courses)
-- =============================================================================
-- enrollments = junction table with extra data (grade, enrollment_date)

CREATE TABLE students (
    student_id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(150) UNIQUE NOT NULL,
    enrolled_date DATE
);

CREATE TABLE courses (
    course_id SERIAL PRIMARY KEY,
    title VARCHAR(200) NOT NULL,
    instructor VARCHAR(100) NOT NULL,
    semester INTEGER,
    credits INTEGER DEFAULT 3,
    metadata JSONB
);

CREATE TABLE enrollments (
    enrollment_id SERIAL PRIMARY KEY,
    student_id INTEGER NOT NULL REFERENCES students(student_id) ON DELETE CASCADE,
    course_id INTEGER NOT NULL REFERENCES courses(course_id) ON DELETE CASCADE,
    enrollment_date DATE DEFAULT CURRENT_DATE,
    grade VARCHAR(2),
    UNIQUE(student_id, course_id)
);

-- =============================================================================
-- SECTION 3: MANY-TO-MANY via Junction (Actors <-> Movies)
-- =============================================================================
-- movie_roles = junction table with salary + character_name

CREATE TABLE actors (
    actor_id SERIAL PRIMARY KEY,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    birth_date DATE,
    biography TEXT
);

CREATE TABLE movies (
    movie_id SERIAL PRIMARY KEY,
    title VARCHAR(200) NOT NULL,
    release_year INTEGER,
    genre VARCHAR(50),
    rating DECIMAL(3,1),
    box_office BYTEA
);

CREATE TABLE movie_roles (
    role_id SERIAL PRIMARY KEY,
    actor_id INTEGER NOT NULL REFERENCES actors(actor_id) ON DELETE CASCADE,
    movie_id INTEGER NOT NULL REFERENCES movies(movie_id) ON DELETE CASCADE,
    character_name VARCHAR(100),
    salary DECIMAL(12,2)
);

-- =============================================================================
-- SECTION 4: COMPOSITE PRIMARY KEY
-- =============================================================================
-- order_versions uses (order_id, version) as composite PK

CREATE TABLE order_versions (
    order_id INTEGER NOT NULL,
    version INTEGER NOT NULL,
    customer_id INTEGER NOT NULL REFERENCES customers(customer_id),
    modified_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    notes TEXT,
    PRIMARY KEY (order_id, version)
);

-- =============================================================================
-- SECTION 5: SELF-REFERENCING (Employee Hierarchy)
-- =============================================================================
-- employees.manager_id -> employees.employee_id

CREATE TABLE employees (
    employee_id SERIAL PRIMARY KEY,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    position VARCHAR(100),
    manager_id INTEGER REFERENCES employees(employee_id),
    hire_date DATE,
    salary DECIMAL(10,2)
);

-- =============================================================================
-- SECTION 6: ONE-TO-ONE (Employee Details)
-- =============================================================================
-- employee_details.employee_id has UNIQUE constraint => genuine 1:1

CREATE TABLE employee_details (
    detail_id SERIAL PRIMARY KEY,
    employee_id INTEGER NOT NULL UNIQUE REFERENCES employees(employee_id) ON DELETE CASCADE,
    address TEXT,
    emergency_contact VARCHAR(100),
    iban VARCHAR(34)
);

-- =============================================================================
-- SECTION 7: SPECIAL DATA TYPES
-- =============================================================================
-- Tests JSONB, TEXT[], BYTEA, JSON handling in conversion

CREATE TABLE documents (
    doc_id SERIAL PRIMARY KEY,
    title VARCHAR(200) NOT NULL,
    content TEXT,
    metadata JSONB,
    tags TEXT[],
    signature BYTEA,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE configuration (
    config_id SERIAL PRIMARY KEY,
    key_name VARCHAR(100) NOT NULL UNIQUE,
    value JSON,
    is_active BOOLEAN DEFAULT true
);

-- =============================================================================
-- SECTION 8: NULL HANDLING
-- =============================================================================
-- Most columns nullable to test NULL propagation

CREATE TABLE projects (
    project_id SERIAL PRIMARY KEY,
    name VARCHAR(200) NOT NULL,
    description TEXT,
    start_date DATE,
    end_date DATE,
    budget DECIMAL(12,2),
    status VARCHAR(20) DEFAULT 'PLANNING'
);

CREATE TABLE project_tasks (
    task_id SERIAL PRIMARY KEY,
    project_id INTEGER NOT NULL REFERENCES projects(project_id) ON DELETE CASCADE,
    task_name VARCHAR(200) NOT NULL,
    assignee_id INTEGER REFERENCES employees(employee_id),
    due_date DATE,
    completed_at TIMESTAMP
);

-- =============================================================================
-- SECTION 9: RESERVED WORDS
-- =============================================================================
-- Table/column names that are PostgreSQL reserved words

CREATE TABLE "user" (
    "id" SERIAL PRIMARY KEY,
    "name" VARCHAR(100) NOT NULL,
    "select" VARCHAR(50),
    "from" DATE
);

CREATE TABLE user_groups (
    group_id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT true
);

-- =============================================================================
-- DATA: customers & products
-- =============================================================================

INSERT INTO customers (first_name, last_name, email, phone, registration_date, notes) VALUES
('Jan', 'Kowalski', 'jan.kowalski@example.com', '123456789', '2024-01-15', 'VIP customer'),
('Anna', 'Nowak', 'anna.nowak@example.com', '987654321', '2024-02-20', NULL),
('Piotr', 'Wisniewski', 'piotr.wisniewski@example.com', '555123456', '2024-03-10', 'Regular buyer'),
('Maria', 'Lewandowska', 'maria.lewandowska@example.com', NULL, '2024-04-05', NULL);

INSERT INTO products (name, description, price, stock_quantity) VALUES
('Laptop Dell XPS 15', 'Business laptop 15-inch', 3499.99, 10),
('Wireless Mouse', 'Wireless mouse', 99.99, 50),
('Mechanical Keyboard', 'RGB gaming keyboard', 299.99, 25),
('Samsung Monitor 27"', '4K Monitor', 1299.99, 15),
('Sony Headphones', 'Noise cancelling', 899.99, 30),
('Webcam HD', '720p webcam', 149.99, NULL);

-- =============================================================================
-- DATA: orders & order_items
-- =============================================================================

INSERT INTO orders (customer_id, order_date, status, total_amount) VALUES
(1, '2024-06-10 10:30:00', 'COMPLETED', 3599.98),
(1, '2024-06-15 14:20:00', 'IN_PROGRESS', 1599.98),
(2, '2024-06-12 09:15:00', 'COMPLETED', 399.98),
(3, '2024-06-18 16:45:00', 'NEW', 2199.98),
(4, '2024-07-01 11:00:00', 'NEW', 4499.97);

INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES
(1, 1, 1, 3499.99), (1, 2, 1, 99.99),
(2, 4, 1, 1299.99), (2, 3, 1, 299.99),
(3, 3, 1, 299.99), (3, 2, 1, 99.99),
(4, 1, 1, 3499.99), (4, 5, 1, 899.99),
(5, 1, 1, 3499.99), (5, 6, 1, 149.99), (5, 2, 2, 99.99);

-- =============================================================================
-- DATA: students, courses & enrollments
-- =============================================================================

INSERT INTO students (name, email, enrolled_date) VALUES
('Alicja Adamska', 'alicja@example.com', '2024-09-01'),
('Bartek Baran', 'bartek@example.com', '2024-09-01'),
('Celina Czarnecka', 'celina@example.com', '2024-09-01'),
('Damian Dabrowski', 'damian@example.com', '2024-09-01');

INSERT INTO courses (title, instructor, semester, credits, metadata) VALUES
('Introduction to Algorithms', 'Dr Kowalski', 1, 4, '{"difficulty": "beginner"}'),
('Database Systems', 'Prof Nowak', 2, 5, '{"difficulty": "intermediate"}'),
('Object-Oriented Programming', 'Dr Wisniewski', 2, 4, NULL),
('Computer Networks', 'Prof Lewandowski', 3, 3, '{"difficulty": "advanced"}');

INSERT INTO enrollments (student_id, course_id, enrollment_date, grade) VALUES
(1, 1, '2024-09-01', 'A'), (1, 2, '2024-09-01', 'B'),
(2, 1, '2024-09-02', NULL), (2, 3, '2024-09-02', 'A'),
(3, 2, '2024-09-03', 'C'), (3, 4, '2024-09-03', 'B'),
(4, 1, '2024-09-04', NULL), (4, 3, '2024-09-04', NULL);

-- =============================================================================
-- DATA: actors, movies & movie_roles
-- =============================================================================

INSERT INTO actors (first_name, last_name, birth_date, biography) VALUES
('Leonardo', 'DiCaprio', '1974-11-11', 'Oscar-winning actor'),
('Margot', 'Robbie', '1986-07-02', 'Australian actress'),
('Tom', 'Hardy', '1977-12-15', NULL);

INSERT INTO movies (title, release_year, genre, rating, box_office) VALUES
('Inception', 2010, 'Sci-Fi', 8.8, NULL),
('The Wolf of Wall Street', 2013, 'Comedy', 8.2, NULL),
('Django Unchained', 2012, 'Western', 8.5, NULL),
('Suicide Squad', 2016, 'Superhero', 6.0, NULL);

INSERT INTO movie_roles (actor_id, movie_id, character_name, salary) VALUES
(1, 1, 'Cobb', 20000000), (1, 2, 'Belfort', 15000000), (1, 3, 'Schultz', 10000000),
(2, 4, 'Harley Quinn', 2000000),
(3, 4, 'Enchantress', 1500000);

-- =============================================================================
-- DATA: order_versions (composite PK)
-- =============================================================================

INSERT INTO order_versions (order_id, version, customer_id, modified_date, notes) VALUES
(1, 1, 1, '2024-06-01', 'Initial order'),
(1, 2, 1, '2024-06-10', 'Updated shipping address'),
(1, 3, 1, '2024-06-15', 'Added express delivery'),
(2, 1, 2, '2024-06-12', 'First version');

-- =============================================================================
-- DATA: employees (self-ref) & employee_details (1:1)
-- =============================================================================

INSERT INTO employees (first_name, last_name, position, manager_id, hire_date, salary) VALUES
('Adam', 'Director', 'CEO', NULL, '2020-01-01', 50000),
('Barbara', 'Manager', 'VP Sales', 1, '2021-03-15', 30000),
('Czeslaw', 'Manager', 'VP Engineering', 1, '2021-04-01', 32000),
('Dorota', 'Developer', 'Senior Engineer', 3, '2022-02-01', 20000),
('Edward', 'Developer', 'Engineer', 3, '2023-06-01', 15000),
('Frania', 'Sales', 'Account Manager', 2, '2023-01-15', 12000);

INSERT INTO employee_details (employee_id, address, emergency_contact, iban) VALUES
(1, '123 Main St', 'Wife', 'PL1234567890'),
(2, '456 Oak Ave', 'Husband', 'PL9876543210'),
(3, '789 Pine Rd', NULL, NULL);

-- =============================================================================
-- DATA: documents & configuration (special types)
-- =============================================================================

INSERT INTO documents (title, content, metadata, tags, signature) VALUES
('Policy Document', 'Company privacy policy...', '{"version": "1.0", "author": "legal"}', ARRAY['policy', 'legal'], NULL),
('Employee Handbook', 'Welcome to our company...', NULL, ARRAY['hr', 'onboarding'], NULL),
('Technical Spec', '{"api": "/v1/users", "method": "GET"}', '{"version": "2.0"}', ARRAY['tech', 'api'], 'binarydata'),
('Meeting Notes', NULL, NULL, NULL, NULL);

INSERT INTO configuration (key_name, value, is_active) VALUES
('feature_flags', '{"dark_mode": true, "beta": false}', true),
('api_endpoints', '{"users": "/api/v1", "orders": "/api/v2"}', true),
('deprecated_settings', '{"old_key": "value"}', false);

-- =============================================================================
-- DATA: projects & project_tasks (NULL handling)
-- =============================================================================

INSERT INTO projects (name, description, start_date, end_date, budget, status) VALUES
('Website Redesign', NULL, '2024-01-01', '2024-06-30', 50000, 'COMPLETED'),
('Mobile App', 'Development of mobile application', '2024-03-01', NULL, 75000, 'IN_PROGRESS'),
('Cloud Migration', 'Cloud infrastructure upgrade', '2024-07-01', '2024-12-31', 100000, 'PLANNING'),
('AI Integration', NULL, NULL, NULL, NULL, 'IDEA');

INSERT INTO project_tasks (project_id, task_name, assignee_id, due_date, completed_at) VALUES
(1, 'Design phase', 4, '2024-03-01', '2024-03-15'),
(1, 'Implementation', 5, '2024-03-16', NULL),
(2, 'UI Design', NULL, '2024-04-01', NULL),
(2, 'Backend API', 5, '2024-05-01', NULL),
(3, 'Planning', 1, '2024-07-15', NULL),
(4, 'Research', NULL, NULL, NULL);

-- =============================================================================
-- DATA: reserved words
-- =============================================================================

INSERT INTO "user" (name, "select", "from") VALUES
('Test User 1', 'admin', '2024-01-01'),
('Test User 2', 'viewer', '2024-02-15'),
('Test User 3', NULL, NULL);

INSERT INTO user_groups (name, is_active) VALUES
('Administrators', true),
('Editors', true),
('Viewers', false),
('Guests', NULL);

-- =============================================================================
-- SUMMARY
-- =============================================================================

SELECT '=== DATABASE LOADED ===' AS info;
SELECT COUNT(*) AS total_tables FROM information_schema.tables WHERE table_schema = 'public';

COMMIT;

CREATE TYPE grage_type AS ENUM ('junior', 'middle', 'senior');
CREATE TYPE score_type AS ENUM ('A', 'B', 'C', 'D', 'E');
CREATE TYPE job_title AS ENUM ('Data_Analist', 'Data_Engineer', 'Data_Analist_Team_Lead', 'Data_Engeneer_Team_Lead', 'General_Manager');
CREATE TYPE department AS ENUM ('Analitics', 'Development', 'Support', 'Administration')

CREATE TABLE IF NOT EXISTS employees (
    id INT GENERATED AS IDENTITY PRIMARY KEY,
    surname VARCHAR (255) NOT NULL,
    birthday DATA NOT NULL,
    working_start_date DATA NOT NULL,
    job_title job_title NOT NULL,
    grade grade_type NOT NULL,
    salary INT,
    department_id INT,
    driver_license BOOLEAN
    CONSTRAINT department_fk
        FOREIGN KEY (department_id)
        REFERENCES departments(id)
        ON DELETE CASCADE)

CREATE TABLE IF NOT EXISTS departments (
    id INT GENERATED AS IDENTITY PRIMARY KEY,
    name department NOT NULL,
    director_name VARCHAR (255),
    employee_count SMALLINT)

CREATE TABLE IF NOT EXISTS scores (
    score_id INT GENERATED AS IDENTITY PRIMARY KEY,
    employee_id INT,
    q1 score_type,
    q2 score_type,
    q3 score_type,
    q4 score_type,
    q5 score_type
    CONSTRAINT employee_fk
        FOREIGN KEY (employee_id)
        REFERENCES employees(id)
        ON DELETE CASCADE)

INSERT INTO departments (
    name,
    director_name,
    employee_count
)
VALUES 
    ('Development', 'Ivanov', 3),
    ('Analitics', 'Petrov', 2),
    ('Support', 'Sidorov', 2),
    ('Administration', 'Lushkov', 1);

INSERT INTO employees (
    surname,
    birthday,
    working_start_date,
    job_title,
    grade,
    salary,
    department_id,
    driver_license
)
VALUES
    ('Ivanov', '1995-02-31', '2018', 'Data_Engineer_Team_Lead', 'senior', 350000, 1, True),
    ('Petrov', '1985-03-24', '2019', 'Data_Analist_Team_Lead', 'senior', 350000, 2, True),
    ('Sidorov', '1975-06-04', '2020', 'Data_Engeneer_Team_Lead', 'senior', 350000, 3, True),
    ('Lushkov', '1989-04-01', '2001', 'General_Manager', 'senior', 450000, 4, True),
    ('Poliaykov', '2001-01-01', '2005', 'Data_Engineer', 'middle', 120000, 1, False),
    ('Smirnova', '1999-07-29', '2006', 'Data_Engineer', 'senior', 150000, 1, True),
    ('Gilmanov', '1995-08-27', '2007', 'Data_Engineer', 'junior', 80000, 1, False),
    ('Vorobieva', '2002-02-11', '2008', 'Data_Analist', 'middle', 150000, 2, True),
    ('Sinitsina', '1997-09-21', '2009', 'Data_Analist', 'junior', 90000, 2, False),
    ('Galkina', '1987-07-14', '2010', 'Data_Engineer', 'middle', 100000, 3, True),
    ('Voronov', '2003-01-17', '2011', 'Data_Engineer', 'middle', 90000, 3, False),
    ('Yastrebov', '1977-07-23', '2012', 'Data_Engineer', 'junior', 80000, 3, False),

    
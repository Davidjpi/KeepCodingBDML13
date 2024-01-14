-- Crear la tabla alumno
CREATE TABLE alumno (
    alumno_id SERIAL PRIMARY KEY,
    name VARCHAR(255),
    surname VARCHAR(255),
    email VARCHAR(255),
    phone VARCHAR(20)
);

ALTER TABLE alumno
ADD CONSTRAINT email_unique UNIQUE (email);

ALTER TABLE alumno
ALTER COLUMN email SET NOT NULL;

ALTER TABLE alumno
ALTER COLUMN name SET NOT NULL;

-- Crear la tabla profesor
CREATE TABLE profesor (
    profesor_id SERIAL PRIMARY KEY,
    name VARCHAR(50)
);

-- Crear la tabla bootcamp
CREATE TABLE bootcamp (
    bootcamp_id SERIAL PRIMARY KEY,
    name VARCHAR(50),
    director_id INT,
    FOREIGN KEY (director_id) REFERENCES profesor(profesor_id)
);

-- Crear la tabla modulo
CREATE TABLE modulo (
    modulo_id SERIAL PRIMARY KEY,
    name VARCHAR(50),
    profesor_id INT,
    bootcamp_id INT,
    FOREIGN KEY (profesor_id) REFERENCES profesor(profesor_id),
    FOREIGN KEY (bootcamp_id) REFERENCES bootcamp(bootcamp_id)
);

-- Crear la tabla evaluacion
CREATE TABLE evaluacion (
    evaluacion_id SERIAL PRIMARY KEY,
    nota INT,
    alumno_id INT,
    modulo_id INT,
    FOREIGN KEY (alumno_id) REFERENCES alumno(alumno_id),
    FOREIGN KEY (modulo_id) REFERENCES modulo(modulo_id)
);

-- Crear la tabla alumno_bootcamp
CREATE TABLE alumno_bootcamp (
    alumno_bootcamp_id SERIAL PRIMARY KEY,
    alumno_id INT,
    bootcamp_id INT,
    FOREIGN KEY (alumno_id) REFERENCES alumno(alumno_id),
    FOREIGN KEY (bootcamp_id) REFERENCES bootcamp(bootcamp_id)
);

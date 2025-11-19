-- IV.2
CREATE TABLE students (
    id SERIAL PRIMARY KEY,
    name VARCHAR(50) NOT NULL,
    age INT,
    gender TEXT CHECK (gender IN ('男', '女'))
);
-- IV.3
CREATE TABLE courses (
    course_id INT PRIMARY KEY,
    course_name VARCHAR(50) NOT NULL,
    teacher_id INT
);
-- IV.4
CREATE TABLE student_courses (
    student_id INT,
    course_id INT,
    PRIMARY KEY (student_id, course_id),
    score DECIMAL(5, 2),
    FOREIGN KEY (student_id) REFERENCES students(id) ON UPDATE CASCADE ON DELETE CASCADE,
    FOREIGN KEY (course_id) REFERENCES courses(course_id) ON UPDATE CASCADE ON DELETE CASCADE
);
-- V.1
INSERT INTO students (name, age, gender)
VALUES ('张三', 20, '男');
-- V.2
INSERT INTO courses (course_id, course_name, teacher_id)
VALUES (1, '数学', 101);
-- V.3
INSERT INTO student_courses (student_id, course_id, score)
VALUES (1, 1, 85.00);
-- VI.2
SELECT *
FROM students;
-- VI.3
SELECT *
FROM students
WHERE age BETWEEN 18 AND 22
    AND gender = '女';
SELECT *
FROM students
WHERE name LIKE '%张%'
ORDER BY age DESC;
SELECT gender,
    COUNT(*) AS cnt
FROM students
GROUP BY gender;
SELECT *
FROM students
WHERE age = (
        SELECT MAX(age)
        FROM students
    );
SELECT name
FROM students
WHERE name IN (
        SELECT name
        FROM students
        WHERE age = 20
    )
    OR name IN (
        SELECT name
        FROM students
        WHERE age = 21
    );
-- VI.4
SELECT s.name,
    COUNT(sc.course_id) AS courses_cnt
FROM students s
    LEFT JOIN student_courses sc ON s.id = sc.student_id
GROUP BY s.name;
SELECT s.name,
    s.age
FROM students s
    JOIN student_courses sc ON s.id = sc.student_id
    JOIN courses c ON sc.course_id = c.course_id
WHERE c.course_name = '数学';
SELECT s.name,
    AVG(sc.score) AS avg_score
FROM students s
    JOIN student_courses sc ON s.id = sc.student_id
GROUP BY s.name
HAVING AVG(sc.score) > 80;
-- VII.1
UPDATE students
SET id = 2
WHERE name = '张三';
SELECT *
FROM student_courses;
DELETE FROM students
WHERE id = 2;
SELECT *
FROM student_courses;

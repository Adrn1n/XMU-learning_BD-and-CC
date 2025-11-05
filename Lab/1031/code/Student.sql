-- 1.(1)
CREATE TABLE Student (
    Name VARCHAR(64),
    English INT,
    Math INT,
    Computer INT
);
INSERT INTO Student
VALUES ('zhangsan', 69, 86, 77),
    ('lisi', 55, 100, 88);
-- 1.(2)
SELECT *
FROM Student;
-- 1.(3)
SELECT Computer
FROM Student
WHERE Name = 'zhangsan';
-- 1.(4)
UPDATE Student
SET Math = 95
WHERE Name = 'lisi';
-- 2.(1)
INSERT INTO Student (Name, English, Math, Computer)
VALUES ('scofield', 45, 89, 100);
-- 2.(2)
SELECT English
FROM Student
WHERE Name = 'scofield';
-- Check
SELECT *
FROM Student;
-- Demo script for BashDB
DROP TABLE users;
CREATE TABLE users (id INT, name TEXT, age INT);
INSERT INTO users (id, name, age) VALUES (1, 'Ada Lovelace', 36);
INSERT INTO users (id, name, age) VALUES (2, 'Linus Torvalds', 54);
INSERT INTO users (id, name, age) VALUES (3, 'Grace Hopper', 85);

-- Basic selects
SELECT * FROM users;
SELECT id, name FROM users WHERE age > 40 ORDER BY id DESC LIMIT 2;

-- Update and reselect
UPDATE users SET age = 55 WHERE name = 'Linus Torvalds';
SELECT * FROM users WHERE name LIKE 'Linus%';

-- Delete and final select
DELETE FROM users WHERE id = 1;
SELECT * FROM users ORDER BY age ASC;



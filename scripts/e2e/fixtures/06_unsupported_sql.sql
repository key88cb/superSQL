CREATE TABLE e2e_unsupp(id int, primary key(id));
INSERT INTO e2e_unsupp VALUES (1);
UPDATE e2e_unsupp SET id = 2 WHERE id = 1;
ALTER TABLE e2e_unsupp ADD COLUMN c int;
TRUNCATE TABLE e2e_unsupp;
DROP TABLE e2e_unsupp;
exit;

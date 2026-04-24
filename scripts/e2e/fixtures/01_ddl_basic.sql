CREATE TABLE e2e_ddl(id int, name char(20), primary key(id));
INSERT INTO e2e_ddl VALUES (1, 'alice');
INSERT INTO e2e_ddl VALUES (2, 'bob');
SELECT * FROM e2e_ddl;
DROP TABLE e2e_ddl;
exit;

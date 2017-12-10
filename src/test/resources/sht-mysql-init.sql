
# mysql -u root -p < sht-mysql-init.sql
DROP DATABASE IF EXISTS sht_test;
CREATE DATABASE sht_test;
GRANT ALL PRIVILEGES ON sht_test.* TO 'sht'@'%' IDENTIFIED BY 'shttester';



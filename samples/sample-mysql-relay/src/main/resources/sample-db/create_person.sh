#!/bin/bash
script_dir=`dirname $0`
#Setup this to point appropriately to MySQL instance
MYSQL='mysql'

${MYSQL} -uroot -e 'CREATE DATABASE IF NOT EXISTS or_test;';
${MYSQL} -uroot -e "CREATE USER 'or_test'@'localhost' IDENTIFIED BY 'or_test';";
${MYSQL} -uroot -e "GRANT ALL ON or_test.* TO 'or_test'@'localhost';"
${MYSQL} -uroot -e "GRANT ALL ON *.* TO 'or_test'@'localhost';"
${MYSQL} -uroot -e "GRANT ALL ON *.* TO 'or_test'@'127.0.0.1';"

${MYSQL} -uor_test -por_test -Dor_test < create_person.sql
${MYSQL} -uroot -e 'RESET MASTER;'

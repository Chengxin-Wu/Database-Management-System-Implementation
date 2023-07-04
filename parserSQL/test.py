from mySqlParser import MySqlParser
from parserSQL import SQLParser

myParser = MySqlParser()
# myParser = SQLParser()
sql_0 = 'SELECT    AAA FROM    BBB WHERE  CCC=DDD'
sql_1 = 'SELECT'
sql_2 = 'SLEEET AAA FROM BBB'
sql_3 = 'DROP TABLE AAA'
sql_4 = 'DROP TABLE'
sql_4 = 'USE BBB'
sql_5 = "CREATE TABLE student (id int PRIMARY, name String)"
sql_6 = "CREATE DATABASE school"
sql_7 = "CREATE INDEX student ON school (id)"
sql_8 = "CREATE dfg a egre  reg aer"
sql_9 = 'SELECT    AAA, EEE FROM    BBB ORDER BY id LIMIT 1 WHERE  CCC = "DDD"'
sql_10 = 'SELECT    AAA, EEE FROM    BBB LIMIT 1 WHERE  CCC = DDD'
sql_11 = 'SELECT    AAA, EEE FROM    BBB LIMIT 1  group by id WHERE  CCC = 111'
sql_12 = "INSERT INTO Websites (url, alexa, country) VALUES ('https://www.baidu.com/',4,'CN')"
sql_13 = "INSERT INTO Websites VALUES ('https://www.baidu.com/',4,'CN')"
sql_14 = "UPDATE Websites SET alexa='5000', country='USA' WHERE name='aaa'"
sql_15 = 'SELECT    AAA, EEE FROM    BBB WHERE  CCC = "DDD" OR fff = "rrr" OR kkk = "ooo" ORDER BY id GROUP BY ID LIMIT 1'

sql = 'select NAME from Student where ID > 4'
sql1 = 'SELECT AVG(COL2), MIN(COL2) FROM TABLE2 WHERE COL2 > 18 GROUP BY COL3 having AVG(COL2) > 1'
sql2 = 'SELECT AVG(COL2), MIN(COL2) FROM TABLE2 WHERE COL2 > 18 GROUP BY COL3 having min(COL2) > 1'
sql3 = 'SELECT ID, COURSE FROM Course LEFT JOIN Student ON Course.ID = Student.ID WHERE Course.ID > 3'
sql4 = 'CREATE INDEX i ON TABLE2 (COL2)'
sql5 = 'DROP INDEX i ON TABLE2'
print(myParser.parse(sql5))

# {'type': 'search join', 'join type': 'LEFT', 'tables': 'Course', 'fields': ['ID', 'COURSE'],
# 'join fields': {'Student': 'ID', 'Course': 'ID'}, 'conditions': [{'field': 'Course.ID',
# 'cond': {'operation': '>', 'value': '3'}}]}
# ret:  ('SELECT', 'ID, COURSE', 'FROM', 'Course LEFT JOIN Student ON Course.ID = Student.ID')
# join_field ['Student.ID', 'Course.ID']

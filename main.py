from executorSQL import *
from executorSQL.myExecutorSQL import MySQLExecuter


def main():
    # e1 = executorSQL.SQLExecuter()
    # e1.run()
    e2 = MySQLExecuter()
    e2.run()
    # select * from Student where ID > 2 AND ID < 6


if __name__ == '__main__':
    main()

# select avg(ID) from Student group by NAME having avg(ID) > 1.4
# SELECT ID, COURSE FROM Course LEFT JOIN Student ON Course.ID = Student.ID WHERE Course.ID > 3

# | ID |  COURSE |
# +----+---------+
# | 1  |   math  |
# | 2  | english |
# | 3  |   tech  |
# | 4  |    C    |
# | 5  |   C++   |
# | 6  |   JAVA  |
# | 7  |  Python |

# | ID |    NAME   |
# +----+-----------+
# | 1  |  huaqiang |
# | 2  |   maigua  |
# | 3  |   51fan   |
# | 4  | madongmei |
# | 5  |   xialuo  |
# | 6  |   zhang3  |
# | 7  |   wang5   |
# | 8  |    li4    |

# CREATE INDEX index1 ON people (ID)
# DROP INDEX index1 ON people

# +----+-------+----------+-------+
# | ID |  name |  class   | grade |
# +----+-------+----------+-------+
# | 4  |  Bob  |   java   |   90  |
# | 5  |  Bob  |   C++    |   70  |
# | 6  |  Cart | database |   80  |
# | 7  |  Tim  |   C++    |   90  |
# | 10 |  Tim  |    C     |   60  |
# | 1  | Alice |   math   |   60  |
# | 2  | Alice |   java   |   70  |
# | 3  | Alice |  python  |   40  |
# | 15 |  Tom  |   java   |  100  |
# +----+-------+----------+-------+

# +----+--------+
# | ID | course |
# +----+--------+
# | 1  |  math  |
# | 2  |  java  |
# | 3  | music  |
# | 4  | python |
# | 5  |  C++   |
# +----+--------+

# create table grade (ID int primary, name string, class string, grade int)
# insert into grade values (1, Alice, math, 60)
# insert into grade values (1, 'Alice', 'math', 60)
# insert into grade values (2, 'Alice', 'java', 70)
# insert into grade values (2, 'Alice', 'python', 40)
# select * from grade where class = 'java' AND grade > 80 AND grade <= 90
# select * from grade order by grade
# select * from grade inner join course on grade.ID = course.ID
# select * from grade inner join course on grade.ID = course.ID where course.ID < 3
# select avg(grade) from grade
# select avg(grade) from grade group by name`
# select avg(grade) from grade group by name having avg(grade) >= 80
# select avg(grade) from grade group by class
# select avg(grade) from grade where grade >= 60 group by name
# select count(class) from grade group by name
# select count(class) from grade where grade >= 80 group by name
# select sum(grade) from grade
# select sum(grade) from grade group by name
# select min(grade) from grade group by name
# select max(grade) from grade group by name
# delete from grade where name = 'Alice' AND class = 'math'
# delete from grade where name = 'Alice'
# UPDATE grade set grade = 100 WHERE name = 'Alice' AND class = 100

# create index index1 on index10000 (COL2)
# drop index index1 on index10000

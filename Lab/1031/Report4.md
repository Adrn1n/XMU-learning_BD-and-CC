---
title: NoSQL和关系数据库的操作比较
author: 刘行逸
date: 20251031
---

# Environments
- M4 MacBook Air
- VMware Fusion Professional Version 13.6.3 (24585314)
- `Linux ubuntuserver 6.14.0-35-generic #35~24.04.1-Ubuntu SMP PREEMPT_DYNAMIC Tue Oct 14 13:30:46 UTC 2 aarch64 aarch64 aarch64 GNU/Linux`
- `psql (PostgreSQL) 16.10 (Ubuntu 16.10-0ubuntu0.24.04.1)`
- 
    ```text
    java version "11.0.24" 2024-07-16 LTS
    Java(TM) SE Runtime Environment 18.9 (build 11.0.24+7-LTS-271)
    Java HotSpot(TM) 64-Bit Server VM 18.9 (build 11.0.24+7-LTS-271, mixed mode)
    ```
- 
    ```text
    Hadoop 3.4.2
    Source code repository https://github.com/apache/hadoop.git -r e1c0dee881820a4d834ec4a4d2c70d0d953bb933
    Compiled by ahmar on 2025-08-07T15:32Z
    Compiled on platform linux-aarch_64
    Compiled with protoc 3.23.4
    From source with checksum fa94c67d4b4be021b9e9515c9b0f7b6
    This command was run using /usr/local/hadoop/hadoop-3.4.2/share/hadoop/common/hadoop-common-3.4.2.jar
    ```
- 
    ```text
    HBase 2.5.11-hadoop3
    Source code repository git://buildbox.localdomain/home/apurtell/tmp/RM/hbase revision=505b485e462c9cbd318116155b3e37204469085a
    Compiled by apurtell on Mon Feb 17 17:03:36 PST 2025
    From source with checksum 6332bbda9ee95c46b73d497bdc8a045a611f5f5e97439d4a786fe1df08b2d9748a33f8dce5bcf509cac15bcae1d6bc8de0e40fe890d9c477a165f9aa68df1294
    ```

# Contents & completion
## Contents
### A. MySQL Database Operations
The Student table is shown in Table 14-7.
- Table 14-7 Student Table
    | Name | English | Math | Computer |
    | - | - | - | - |
    | zhangsan | 69 | 86 | 77 |
    | lisi | 55 | 100 | 88 |

1. Based on the Student table provided above, complete the following operations in a MySQL database:
    - (1) Create the Student table in MySQL and insert the data
    - (2) Use SQL statements to output all records from the Student table
    - (3) Query zhangsan's Computer score
    - (4) Modify lisi's Math score to 95
2. Based on the Student table already designed above, use MySQL's JAVA client programming to implement the following operations:
    - (1) Add the following record to the Student table:
        | Name | English | Math | Computer |
        | - | - | - | - |
        | scofield | 45 | 89 | 100 |
    - (2) Retrieve scofield's English score information

### B. HBase Database Operations
The Student table is shown in Table 14-8.
- Table 14-8 Student Table
    | Name | score:English | score:Math | score:Computer |
    | - | - | - | - |
    | zhangsan | 69 | 86 | 77 |
    | lisi | 55 | 100 | 88 |

1. Based on the Student table information provided above, perform the following operations:
    - (1) Create the Student table using HBase Shell commands.
    - (2) Browse the relevant information of the Student table using the `scan` command.
    - (3) Query zhangsan's Computer score.
    - (4) Modify lisi's Math score to 95.
2. Based on the Student table already designed above, use HBase API programming to implement the following operations:
    - (1) Add the following data:
        | Name | score:English | score:Math | score:Computer |
        | - | - | - | - |
        | scofield | English:45 | Math:89 | Computer:100 |
    - (2) Retrieve scofield's English score information.

## Completion
### A.
#### Code
1. Create table
    ```shell
    sudo -u postgres psql -c "CREATE DATABASE school;"

    ```
2. 
    ```sql
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

    ```
3. Excecute and drop database
    ```shell
    sudo -u postgres psql -d school -f Lab/1031/code/Student.sql
    sudo -u postgres dropdb school
    ```

#### Results
```shell
hadoop@ubuntuserver:~/XMU-learning_BD-and-CC$ sudo -u postgres psql -d school -f Lab/1031/code/Student.sql 
CREATE TABLE
INSERT 0 2
   name   | english | math | computer 
----------+---------+------+----------
 zhangsan |      69 |   86 |       77
 lisi     |      55 |  100 |       88
(2 rows)

 computer 
----------
       77
(1 row)

UPDATE 1
INSERT 0 1
 english 
---------
      45
(1 row)

   name   | english | math | computer 
----------+---------+------+----------
 zhangsan |      69 |   86 |       77
 lisi     |      55 |   95 |       88
 scofield |      45 |   89 |      100
(3 rows)

hadoop@ubuntuserver:~/XMU-learning_BD-and-CC$
```

### B.
#### Code
```shell
# 1.(1)
disable 'Student'
drop 'Student'
create 'Student', 'score'
put 'Student', 'Zhangsan', 'score:English', '69'
put 'Student', 'Zhangsan', 'score:Math', '86'
put 'Student', 'Zhangsan', 'score:Computer', '77'
put 'Student', 'lisi', 'score:English', '55'
put 'Student', 'lisi', 'score:Math', '100'
put 'Student', 'lisi', 'score:Computer', '88'

# 1.(2)
scan 'Student'

# 1.(3)
get 'Student', 'Zhangsan', 'score:Computer'

# 1.(4)
put 'Student', 'lisi', 'score:Math', '95'

# 2.(1)
put 'Student', 'scofield', 'score:English', '45'
put 'Student', 'scofield', 'score:Math', '89'
put 'Student', 'scofield', 'score:Computer', '100'

# 2.(2)
get 'Student', 'scofield', 'score:English'

# Check
scan 'Student'

```

#### Results
```shell
hadoop@ubuntuserver:~/XMU-learning_BD-and-CC$ ${HBASE_HOME}/bin/hbase shell < Lab/1031/code/Student.hbasse 
HBase Shell
Use "help" to get list of supported commands.
Use "exit" to quit this interactive shell.
For Reference, please visit: http://hbase.apache.org/2.0/book.html#shell
Version 2.5.11-hadoop3, r505b485e462c9cbd318116155b3e37204469085a, Mon Feb 17 17:03:36 PST 2025
Took 0.0007 seconds                                                                                                                                                                                   
hbase:001:0> # 1.(1)
hbase:002:0> disable 'Student'
Took 18.3477 seconds                                                                                                                                                                                  
hbase:003:0> drop 'Student'
Took 0.1402 seconds                                                                                                                                                                                   
hbase:004:0> create 'Student', 'score'
Created table Student
Took 2.1407 seconds                                                                                                                                                                                   
=> Hbase::Table - Student
hbase:005:0> put 'Student', 'Zhangsan', 'score:English', '69'
Took 0.0412 seconds                                                                                                                                                                                   
hbase:006:0> put 'Student', 'Zhangsan', 'score:Math', '86'
Took 0.0019 seconds                                                                                                                                                                                   
hbase:007:0> put 'Student', 'Zhangsan', 'score:Computer', '77'
Took 0.0115 seconds                                                                                                                                                                                   
hbase:008:0> put 'Student', 'lisi', 'score:English', '55'
Took 0.0046 seconds                                                                                                                                                                                   
hbase:009:0> put 'Student', 'lisi', 'score:Math', '100'
Took 0.0033 seconds                                                                                                                                                                                   
hbase:010:0> put 'Student', 'lisi', 'score:Computer', '88'
Took 0.0019 seconds                                                                                                                                                                                   
hbase:011:0> 
hbase:012:0> # 1.(2)
hbase:013:0> scan 'Student'
ROW                                                COLUMN+CELL                                                                                                                                        
 Zhangsan                                          column=score:Computer, timestamp=2025-11-05T12:25:56.919, value=77                                                                                 
 Zhangsan                                          column=score:English, timestamp=2025-11-05T12:25:56.904, value=69                                                                                  
 Zhangsan                                          column=score:Math, timestamp=2025-11-05T12:25:56.911, value=86                                                                                     
 lisi                                              column=score:Computer, timestamp=2025-11-05T12:25:56.950, value=88                                                                                 
 lisi                                              column=score:English, timestamp=2025-11-05T12:25:56.939, value=55                                                                                  
 lisi                                              column=score:Math, timestamp=2025-11-05T12:25:56.944, value=100                                                                                    
2 row(s)
Took 0.0256 seconds                                                                                                                                                                                   
hbase:014:0> 
hbase:015:0> # 1.(3)
hbase:016:0> get 'Student', 'Zhangsan', 'score:Computer'
COLUMN                                             CELL                                                                                                                                               
 score:Computer                                    timestamp=2025-11-05T12:25:56.919, value=77                                                                                                        
1 row(s)
Took 0.0037 seconds                                                                                                                                                                                   
hbase:017:0> 
hbase:018:0> # 1.(4)
hbase:019:0> put 'Student', 'lisi', 'score:Math', '95'
Took 0.0042 seconds                                                                                                                                                                                   
hbase:020:0> 
hbase:021:0> # 2.(1)
hbase:022:0> put 'Student', 'scofield', 'score:English', '45'
Took 0.0023 seconds                                                                                                                                                                                   
hbase:023:0> put 'Student', 'scofield', 'score:Math', '89'
Took 0.0014 seconds                                                                                                                                                                                   
hbase:024:0> put 'Student', 'scofield', 'score:Computer', '100'
Took 0.0016 seconds                                                                                                                                                                                   
hbase:025:0> 
hbase:026:0> # 2.(2)
hbase:027:0> get 'Student', 'scofield', 'score:English'
COLUMN                                             CELL                                                                                                                                               
 score:English                                     timestamp=2025-11-05T12:25:57.011, value=45                                                                                                        
1 row(s)
Took 0.0028 seconds                                                                                                                                                                                   
hbase:028:0> 
hbase:029:0> # Check
hbase:030:0> scan 'Student'
ROW                                                COLUMN+CELL                                                                                                                                        
 Zhangsan                                          column=score:Computer, timestamp=2025-11-05T12:25:56.919, value=77                                                                                 
 Zhangsan                                          column=score:English, timestamp=2025-11-05T12:25:56.904, value=69                                                                                  
 Zhangsan                                          column=score:Math, timestamp=2025-11-05T12:25:56.911, value=86                                                                                     
 lisi                                              column=score:Computer, timestamp=2025-11-05T12:25:56.950, value=88                                                                                 
 lisi                                              column=score:English, timestamp=2025-11-05T12:25:56.939, value=55                                                                                  
 lisi                                              column=score:Math, timestamp=2025-11-05T12:25:57.005, value=95                                                                                     
 scofield                                          column=score:Computer, timestamp=2025-11-05T12:25:57.018, value=100                                                                                
 scofield                                          column=score:English, timestamp=2025-11-05T12:25:57.011, value=45                                                                                  
 scofield                                          column=score:Math, timestamp=2025-11-05T12:25:57.015, value=89                                                                                     
3 row(s)
Took 0.0052 seconds                                                                                                                                                                                   
hbase:031:0> 
hadoop@ubuntuserver:~/XMU-learning_BD-and-CC$
```
  
# Problems
None.

# Solutions
None.

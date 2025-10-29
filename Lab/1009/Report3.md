---
title: 熟悉常用的Hbase操作
author: 刘行逸
date: 20251009
---

# Environments
- M4 MacBook Air
- VMware Fusion Professional Version 13.6.3 (24585314)
- `Linux ubuntuserver 6.14.0-33-generic #33~24.04.1-Ubuntu SMP PREEMPT_DYNAMIC Fri Sep 19 16:19:58 UTC 2 aarch64 aarch64 aarch64 GNU/Linux`
- 
    ```shell
    java version "11.0.24" 2024-07-16 LTS
    Java(TM) SE Runtime Environment 18.9 (build 11.0.24+7-LTS-271)
    Java HotSpot(TM) 64-Bit Server VM 18.9 (build 11.0.24+7-LTS-271, mixed mode)
    ```
- 
    ```shell
    Hadoop 3.4.2
    Source code repository https://github.com/apache/hadoop.git -r e1c0dee881820a4d834ec4a4d2c70d0d953bb933
    Compiled by ahmar on 2025-08-07T15:32Z
    Compiled on platform linux-aarch_64
    Compiled with protoc 3.23.4
    From source with checksum fa94c67d4b4be021b9e9515c9b0f7b6
    This command was run using /usr/local/hadoop/hadoop-3.4.2/share/hadoop/common/hadoop-common-3.4.2.jar
    ```
- 
    ```shell
    HBase 2.5.11-hadoop3
    Source code repository git://buildbox.localdomain/home/apurtell/tmp/RM/hbase revision=505b485e462c9cbd318116155b3e37204469085a
    Compiled by apurtell on Mon Feb 17 17:03:36 PST 2025
    From source with checksum 6332bbda9ee95c46b73d497bdc8a045a611f5f5e97439d4a786fe1df08b2d9748a33f8dce5bcf509cac15bcae1d6bc8de0e40fe890d9c477a165f9aa68df1294
    ```

# Contents & completion
## Contents
### Programmatic Implementation and HBase Shell Tasks
Implement the following specified functionalities programmatically and accomplish the same tasks using HBase Shell commands:
1. List all HBase tables and their related information, such as table names
2. Print all record data of a specified table to the terminal
3. Add and delete specified column families or columns to/from an already created table
4. Clear all record data from a specified table
5. Count the number of rows in a table

### HBase Database Operations
1. Data Conversion: Convert the following relational database tables and data (see Table 14-3 to Table 14-5) into a schema suitable for HBase storage and insert the data:
    - Table 14-3 Student
    
        | S_No | S_Name | S_Sex | S_Age |
        | - | - | - | - |
        | 2015001 | Zhangsan | male | 23 |
        | 2015002 | Mary | female | 22 |
        | 2015003 | Lisi | male | 24 |

    - Table 14-4 Course
    
        | C_No | C_Name | C_Credit |
        | - | - | - |
        | 123001 | Math | 2.0 |
        | 123002 | Computer Science | 5.0 |
        | 123003 | English | 3.0 |

    - Table 14-5 SC
    
        | SC_Sno | SC_Cno | SC_Score |
        | - | - | - |
        | 2015001 | 123001 | 86 |
        | 2015001 | 123003 | 69 |
        | 2015002 | 123002 | 77 |
        | 2015002 | 123003 | 99 |
        | 2015003 | 123001 | 98 |
        | 2015003 | 123002 | 95 |

2. Programmatic Implementation: Implement the following functions:
    - (1) `createTable(String tableName, String[] fields)`
    
        Creates a table. `tableName` is the name of the table, and `fields` is a string array storing the names of the column families. If a table with the specified `tableName` already exists in HBase, it should be deleted first, and then a new table should be created.

    - (2) `addRecord(String tableName, String row, String[] fields, String[] values)`
    
        Adds corresponding data `values` to the cells specified by `tableName`, `row` (represented by `S_Name`), and the string array `fields`. Each element in `fields` should be represented as "columnFamily:column" if there is a column qualifier under the corresponding column family. For example, when adding scores for "Math", "Computer Science", and "English" simultaneously, the `fields` array would be `{"Score:Math", "Score:Computer Science", "Score:English"}`, and the `values` array would store the scores for these three courses.

    - (3) `scanColumn(String tableName, String column)`
    
        Browses data for a specific column in `tableName`. If the data for that column does not exist in a particular row, it should return `null`. If the `column` parameter is a column family name and it has several column qualifiers, all data for each qualifier should be listed. If `column` is a specific column name (e.g., "Score:Math"), only the data for that specific column should be listed.

    - (4) `modifyData(String tableName, String row, String column, String newValue)`
    
        Modifies the data in the cell specified by `tableName`, `row` (which can be represented by the student's name `S_Name`), and `column`. A `newValue` parameter should be added to specify the new data.

    - (5) `deleteRow(String tableName, String row)`
    
        Deletes the record for the row specified by `row` in `tableName`.

## Completion
HBase_schooling.hbase
```shell
# A.(1)
list

# B.1
## define tables (B.2.(1))
### tab14.3
disable 'Student'
drop 'Student'
create 'Student', 'info'

### tab14.4
disable 'Course'
drop 'Course'
create 'Course', 'info'

### tab14.5
disable 'SC'
drop 'SC'
create 'SC', 'score'

## insert data (B.2.(2))
### tab14.3
put 'Student', '2015001', 'info:S_Name', 'Zhangsan'
put 'Student', '2015001', 'info:S_Sex', 'male'
put 'Student', '2015001', 'info:S_Age', '23'

put 'Student', '2015002', 'info:S_Name', 'Mary'
put 'Student', '2015002', 'info:S_Sex', 'female'
put 'Student', '2015002', 'info:S_Age', '22'

put 'Student', '2015003', 'info:S_Name', 'Lisi'
put 'Student', '2015003', 'info:S_Sex', 'male'
put 'Student', '2015003', 'info:S_Age', '24'

### tab14.4
put 'Course', '123001', 'info:C_Name', 'Math'
put 'Course', '123001', 'info:C_Credit', '2.0'

put 'Course', '123002', 'info:C_Name', 'Computer Science'
put 'Course', '123002', 'info:C_Credit', '5.0'

put 'Course', '123003', 'info:C_Name', 'English'
put 'Course', '123003', 'info:C_Credit', '3.0'

### tab14.5
put 'SC', '2015001_123001', 'score:SC_Score', '86'
put 'SC', '2015001_123003', 'score:SC_Score', '69'
put 'SC', '2015002_123002', 'score:SC_Score', '77'
put 'SC', '2015002_123003', 'score:SC_Score', '99'
put 'SC', '2015003_123001', 'score:SC_Score', '98'
put 'SC', '2015003_123002', 'score:SC_Score', '95'

# A.(2)
scan 'Student'
scan 'Course'
scan 'SC'

# A.(3)
## A.(3).1
disable 'Student'
alter 'Student', {NAME => 'contact'}
enable 'Student'
describe 'Student'

disable 'Student'
alter 'Student', 'delete' => 'contact'
enable 'Student' describe 'Student'

## A.(3).2
put 'Student', '2015001', 'info:S_Contact', '123456'
get 'Student', '2015001'
delete 'Student', '2015001', 'info:S_Contact'
get 'Student', '2015001'

# A.(4)
truncate 'Student'
scan 'Student'

# A.(5)
count 'Student'
count 'Course'
count 'SC'

# B.2.(3)
scan 'Course', {COLUMNS => 'info'}
scan 'Course', {COLUMNS => 'info:C_Name'}

# B.2.(4)
get 'Course', '123001', {COLUMNS => 'info:C_Credit'}
put 'Course', '123001', 'info:C_Credit', '3.0'
get 'Course', '123001', {COLUMNS => 'info:C_Credit'}

# B.2.(5)
deleteall 'Course', '123001'
scan 'Course'

```
  
# Problems
## 1
SLF4J warning
```shell
SLF4J: Class path contains multiple SLF4J bindings.
SLF4J: Found binding in [jar:file:/usr/local/hbase/hbase-2.5.11-hadoop3/lib/client-facing-thirdparty/log4j-slf4j-impl-2.17.2.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: Found binding in [jar:file:/usr/local/hadoop/hadoop-3.4.2/share/hadoop/common/lib/slf4j-reload4j-1.7.36.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: See http://www.slf4j.org/codes.html#multiple_bindings for an explanation.
SLF4J: Actual binding is of type [org.apache.logging.slf4j.Log4jLoggerFactory]
```

## 2
HBase can't stop.

# Solutions
## 1
Uncomment the line `export HBASE_DISABLE_HADOOP_CLASSPATH_LOOKUP="true"` in `hbase-env.sh` to avoid classpath conflicts between HBase and Hadoop.

## 2
Uninstall [hbase-2.5.11-bin](https://archive.apache.org/dist/hbase/stable/hbase-2.5.11-bin.tar.gz) and reinstall with [hbase-2.5.11-hadoop3-bin](https://archive.apache.org/dist/hbase/stable/hbase-2.5.11-hadoop3-bin.tar.gz).

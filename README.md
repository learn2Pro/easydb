# Background

### Coarse To View
* [6.830](http://db.lcs.mit.edu/6.830/assign.php)
### Exercise To Complete
* [lab1](https://github.com/MIT-DB-Class/simple-db-hw-2021/blob/master/lab1.md)
* [lab2](https://github.com/MIT-DB-Class/simple-db-hw-2021/blob/master/lab2.md)
* [lab3](https://github.com/MIT-DB-Class/simple-db-hw-2021/blob/master/lab3.md)
* [lab4](https://github.com/MIT-DB-Class/simple-db-hw-2021/blob/master/lab4.md)
* [lab5](https://github.com/MIT-DB-Class/simple-db-hw-2021/blob/master/lab5.md)
* [lab6](https://github.com/MIT-DB-Class/simple-db-hw-2021/blob/master/lab6.md)

# setup
### dependency
1. jdk
2. maven
### installation
* mac
  * brew install jdk maven
* linux
  * apt-get install jdk maven
  * yum install jdk maven
* windows
  * hands on and good luck!

# Architecture

![archi](https://upload-images.jianshu.io/upload_images/26736638-632aea609b28f523.png)

# Usage

1. load table and use sql to query
   1. `import data`: create data.txt like 
    ```
   1,10
   2,20
   3,30
   4,40
   5,50
   5,50
    ```
   3. `load data:`  
      1. cd easydb-storage/
      2. java -jar target/easydb-storage-1.0.0_20220115.jar convert src/main/resources/data.txt 2 "int,int"
   4. `catalog:` create catalog.txt like
    ```
    data (f1 int, f2 int)
    ```
   5. `start query:` java -jar target/easydb-storage-1.0.0_20220115.jar parser src/main/resources/catalog.txt

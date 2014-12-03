Berimbolo (Stephen Porter & Ryan Peachey)
p4 - MapReduce Projects
CS 430
Updated 12/XX/2014

=============
General Info
=============
To Compile: make

To Run:
 - Requirements:
 -- Hadoop 1.2.1
 -- Root access
 -- ssh to localhost without password
 -- A jar of the java code
 
[HADOOP_INSTALL_PATH]/local-scripts/create-hadoop-cluster.sh
[HADOOP_INSTALL_PATH]/hadoop/bin/hadoop fs -put [INPUT] input/
[HADOOP_INSTALL_PATH]/hadoop/bin/hadoop jar [JAR_FILE] input/ output/
[HADOOP_INSTALL_PATH]/hadoop/bin/hadoop fs -get output/ .

======
Files
======
src
  |_
	
README.txt

=====
Data
=====


===========
Discussion
===========


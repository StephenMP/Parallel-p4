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
	MovieDataMiner.java
	MovieData.java
MovieDataMiner.jar
README.txt

=====
Data
=====
Trend 1: We found that the decade with the highest average ratings was the 1980's with an average rating of 4.33
Trend 2: We found that the decade which released the most amount of movies was the 1990's with 6412 releases

===========
Discussion
===========
After deciding on which version of the project we wanted to use. The movie data miner. We arranged our meetings
For this project we once again used screen sharing to discuss how this program was set up.
We figured that the two interesting data sets we would use would be comparing the average rating per decade. and the number of movies released per decade. This would be found by taking the data set in with the average rating of a movie.
Our data set was stored using two different hashtables one to store the data by year. and then one that would later take in the data sets from the previous hashtable, and translate it to the correct decade we needed.
When we stored the data in the hashtables we would calculate the average of the passed value with the current and store that for our value as the rating.
The map reduce function in turn was very similar to what we set up for our inverted index the major difference being what we needed to use in this case was an integer for the key value rather than a text.
Making these changes brought about our final given result.
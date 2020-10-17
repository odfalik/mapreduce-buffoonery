## CS 4371.501 Introduction to Big Data Management and Analytics
# Homework 2

Before running, make sure hdfs is set up:
```
start-all.sh
hdfs dfs -mkdir /hw2
hdfs dfs -mkdir /hw2/input
hdfs dfs -put /path/to/soc-LiveJournal1Adj.txt /path/to/userdata.txt /hw2/input
hdfs dfs -rm -r /hw2/p*
```


### Problem 1: Mutual Friends

```
hadoop jar hw2.jar hw2.MutualFriends /hw2/input/soc-LiveJournal1Adj.txt /hw2/p1
```


### Problem 3: Average of Friends Age
```
hadoop jar hw2.jar hw2.AvgFriendAge /hw2/input/soc-LiveJournal1Adj.txt /hw2/input/userdata.txt /hw2/p2
```


### Problem 4: Sorting Friends by Age

```
hadoop jar hw2.jar hw2.FriendSort /hw2/input/soc-LiveJournal1Adj.txt /hw2/input/userdata.txt /hw2/p3
```




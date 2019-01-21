
Used Virtual Box and Cloudera

## Useful Links:
* Virtual Box: https://www.virtualbox.org/wiki/Downloads (Download and Install)
* Cloudera QuickStart VM https://www.cloudera.com/downloads/quickstart_vms/5-13.html (Download only, no installation)

## Reference: 
Maheshwari, Anil. Big Data : Made Accessible - Kindle Edition.

## Some useful commands:
* hadoop fs -mkdir -p user/cloudera/input
* hadoop fs -put /home/cloudera/Desktop/TestWC.txt /user/cloudera/input
* hadoop jar /home/cloudera/workspace/myproject.jar WordCount /user/cloudera/input /user/cloudera/output
* hadoop fs -get /user/cloudera/output
* hdfs dfs -ls
* hdfs dfs -rmr /user/cloudera/output   #to delete

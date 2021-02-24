# Cluster-monitoring-benchmark

This benchmark emulates a cluster management scenario. 
It uses a trace of timestamped tuples collected from an 
[11,000-machine compute cluster at Google.](https://github.com/google/cluster-data/blob/master/ClusterData2011_2.md) 
Each tuple is a monitoring event related to the tasks of compute jobs that execute on the cluster, 
such as the successful completion of a task, the failure of a task, 
or the submission of a high-priority task for a production job.
  
It has two queries, Query1 and Query2, that express common cluster monitoring tasks: 
Query1 combines a projection and an aggregation with a GROUP-BY clause to compute the sum of the requested
share of CPU utilisation per job category; 
and Query2 combines a projection, a selection, and an aggregation with a GROUP-BY to report
the average requested CPU utilisation of submitted tasks.
  
If you need more information, then you have to check [this paper.](https://dl.acm.org/doi/10.1145/2882903.2882906)
  
## Build Process
  
Benchmark and data-generator are built using Apache Maven. To build benchmark and data-generator, run: 
  
<code>mvn clean package</code>

## Running Benchmark

To run Query1, run:  
<code>./bin/spark-submit --class edu.sogang.benchmark.RunBench ASSEMBLED_JAR_PATH 
--query-name q1 --config-filename config.properties</code> 

To run Query2, run:  
<code>./bin/spark-submit --class edu.sogang.benchmark.RunBench ASSEMBLED_JAR_PATH
--query-name q2 --config-filename config.properties</code> 

## Running Data-Generator
  
To run data-generator, run:  
<code>java -jar ASSEMBLED_JAR_PATH --config-filename FILE_NAME</code>
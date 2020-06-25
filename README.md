# BigSpa

A framework for large-sacle static program analysis. The master branch is in version 1.0-SNAPSHOT.

## Prerequisites

As BigSpa is built on the big data processing platform [Spark](https://github.com/apache/spark), distributed file system [HDFS](https://github.com/apache/hadoop) and distributed in-memory database [Redis](https://github.com/antirez/redis), you need to get these installed first.

## Building BigSpa from Source

We use Maven to build our project. To build BigSpa you need:

- Unix-like environment
- Git
- Maven
- Java 8 or 11

``` bash

git clone https://github.com/PasaLab/BigSpa.git
cd BigSpa
mvn clean install -DskipTests
```

## Run BigSpa

To perform offline batch or online incremental static program analysis using BigSpa, you can write scripts in the following format.

``` bash

spark-submit \
--master yarn \
--deploy-mode client \
--name TASK_NAME \
--class Redis_pt \ # main function
--num-executors 16 \
--executor-cores 24 \
--executor-memory 16G \
--conf spark.storage.unrollMemoryThreshold=10000000 \
--conf spark.locality.wait.time=1ms \
--conf spark.locality.wait.node=1ms \
--files data/pasa.conf.prop \
--driver-memory 16G \
--driver-class-path /home/user/class/path \
 \
BigSpa-1.0-SNAPSHOT-jar-with-dependencies.jar \ # run the jar package
 \
islocal,false \
master,hdfs://master:9001/ \
input_graph,/path/to/graph/data/InputGraph/hdfs_pointsto \ # the input graph data
input_grammar,/path/to/grammar/data/alias-complete.grammar \ # grammar
output,/BigSpa_Output/result/hdfs_pt_Redis \
checkpoint_output,hdfs://master:9001/BigSpa/checkpoint1 \
updateRedis_interval,500000 \
queryRedis_interval,50000 \
 \
defaultpar,1152 \
clusterpar,384 \
newnum_interval,100000000 \
checkpoint_interval,20 \
 \
file_index_f,0 \ # for Linux data input
file_index_b,12 \
 \
check_edge,false \
outputdetails,false \
output_Par_INFO,false \
 \
Split_Threshold,1000000 # threshold of node split

```

The script can be run in the directory where the JAR package is located. For a description of the importent parameters, see the table below.

|                                | Parameter             | Description                                                                                                                                        | Value for Reference                  |
|--------------------------------|-----------------------|----------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------|
| Spark script params            | master                | yarn                                                                                                                                               | yarn                                 |
|                                | deploy-mode           | running mode                                                                                                                                       | client                               |
|                                | name                  | Spark APP Name                                                                                                                                     | BigSpa.offline.psql.df               |
|                                | class                 | main function                                                                                                                                      | OFFLINE.Redis_pt                     |
|                                | num-executors         | number of executors                                                                                                                                | 16                                   |
|                                | executor-cores        | number of cores of each executor                                                                                                                   | 24                                   |
|                                | executor-memory       | memory of each executor                                                                                                                            | 16G                                  |
|                                | conf                  | params of Spark or Java                                                                                                                            |                                      |
|                                | files                 | file path for Redis                                                                                                                                | data/pasa.conf.prop                       |
|                                | driver-memory         |                                                                                                                                                    | 16G                                  |
|                                | driver-class-path     |                                                                                                                                                    |                                      |
| General parameters             | islocal               | whether to perform local debugging                                                                                                                 | FALSE                                |
|                                | master                | HDFS address                                                                                                                                       | hdfs://master:9001/                  |
|                                | input_graph           | file path of the input graph                                                                                                                       | /data/linux.pt                       |
|                                | input_grammar         | file path of the grammar                                                                                                                           | /data/grammar                        |
|                                | output                | output path                                                                                                                                        | /BigSpa/result/hdfs_pt_Redis         |
|                                | checkpoint_output     | save path for checkpoint files                                                                                                                     | hdfs://master:9001/BigSpa/checkpoint |
|                                | updateRedis_interval  | batch size for updating Redis                                                                                                                      | 500000                               |
|                                | queryRedis_interval   | batch size for querying Redis                                                                                                                      | 50000                                |
|                                | defaultpar            | default number of partitions                                                                                                                       | 384/768/1152                         |
|                                | clusterpar            | number of partitions in the cluster(num-executors*executor-cores)                                                                                  |                                      |
|                                | newnum_interval       | threshold for automatically add partitions                                                                                                         |                                      |
|                                | checkpoint_interval   | cut off the lineage after how many iterations                                                                                                      |                                      |
|                                | file_index_f          | for Linux database, used to decide which files to merge and execute                                                                                | 0                                    |
|                                | file_index_b          | for Linux database, used to decide which files to merge and execute                                                                                | 12                                   |
|                                | check_edge            | whether to output edge information                                                                                                                 | FALSE                                |
|                                | output_Par_INFO       | whether to perform automatic partition adjustment                                                                                                  | TRUE                                 |
| param for node split           | Split_Threshold       | when the number of predicted collars exceeds$*16. split the node                                                                                   |                                      |
| params for computation closure | input_e               | as E described in the paper                                                                                                                        |                                      |
|                                | input_n               | as N described in the paper                                                                                                                        |                                      |
|                                | is_complete_loop      | whether to perform local closure operations                                                                                                        |  true                                |
|                                | original_loop_turn    | number of small local closure execution rounds                                                                                                     | 5                                    |
|                                | max_loop_turn         | number of large local closure execution rounds                                                                                                     | 100                                  |
|                                | convergence_threshold | conditions for executing large local closures: when the number of new edges generated per round is less than $, large local closures are performed | 10000                                |
| params for incremental updates | changemode_interval   | calculation mode update threshold: when the number of new edges per round reaches $, switch from stand-alone to distributed                        | 50000                                |
|                                | add                   | path of input batches                                                                                                                              | data/httpd.pt.batch/batches          |

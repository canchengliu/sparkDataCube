#!/bin/bash
# shell script to run the hadoop job

# judge whether the running method is legal or not
if [ $# -lt 2 -o $# -gt 3 ];then
    echo "Usage: ./run.sh [input_path] [out_path] [num_partitions]"
    exit
elif [ $# -eq 3 ];then
    # specify the number of partitions in the second mapreduce task
    num_partitions=$3
else
    num_partitions=10
fi
# get the input/output path and remove the '/' after the path names if it exists
input_path=$1
if [ ${input_path:`expr ${#input_path} - 1`} = '/' ]
then
    input_path=${input_path:0:-1}
fi
output_path=$2
if [ ${output_path:`expr ${#output_path} - 1`} = '/' ]
then
    output_path=${output_path:0:-1}
fi

# caculate the total number of lines of the input files
num_lines=`awk 'END{print NR}' $input_path/* | tail -n1`

# initilize and start the hadoop cluster
$HADOOP_HOME/bin/hadoop namenode -format
$HADOOP_HOME/bin/hadoop datanode -format
$HADOOP_HOME/bin/start-all.sh
# wait for the hadoop cluster to be prepared
echo "waiting for the hadoop cluster to be prepared............."
sleep 20s
# upload all the input files to HDFS
$HADOOP_HOME/bin/hadoop fs -put $1/* /Users/liucancheng/Hadoop/input

# 1. run the first mapreduce task --Estimate
$HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/contrib/streaming/hadoop-streaming-1.2.1.jar \
 -D mapred.job.name="Estimate" \
 -D mapred.reduce.tasks=1 \
 -input /Users/liucancheng/Hadoop/input \
 -output /Users/liucancheng/Hadoop/output_Estimate \
 -mapper ./src/map_Estimate.py \
 -reducer ./src/reduce_Estimate.py \
 -file ./src/map_Estimate.py \
 -file ./src/reduce_Estimate.py \
 -cmdenv num_lines=${num_lines} \
 -cmdenv num_partitions=${num_partitions} \
&& \
$HADOOP_HOME/bin/hadoop fs -get /Users/liucancheng/Hadoop/output_Estimate/part-* ./ \
&& \
# 2. run the second mapreduce task --Materialize
$HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/contrib/streaming/hadoop-streaming-1.2.1.jar \
 -D mapred.job.name="Materialize" \
 -D mapred.reduce.tasks=${num_partitions} \
 -D map.output.key.field.separator='.' \
 -D num.key.fields.for.partirion=1 \
 -input /Users/liucancheng/Hadoop/input \
 -output /Users/liucancheng/Hadoop/output_Materialize \
 -mapper ./src/map_Materialize.py \
 -reducer ./src/reduce_Materialize.py \
 -file ./src/map_Materialize.py \
 -file ./src/reduce_Materialize.py \
 -file ./part-* \
 -partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner \
 -cmdenv num_partitions=${num_partitions} \
&& \
# 3. run the third mapreduce task --Postprocess
$HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/contrib/streaming/hadoop-streaming-1.2.1.jar \
 -D mapred.job.name="Postprocess" \
 -D mapred.reduce.tasks=1 \
 -input /Users/liucancheng/Hadoop/output_Materialize/part-* \
 -output /Users/liucancheng/Hadoop/output \
 -mapper ./src/map_Postprocess.py \
 -reducer ./src/reduce_Postprocess.py \
 -file ./src/map_Postprocess.py \
 -file ./src/reduce_Postprocess.py \

#----------------------------------
# this segment of code is used to copy the intermediate files to local machine to check the job process
#$HADOOP_HOME/bin/hadoop fs -get /Users/liucancheng/Hadoop/output_Estimate ./tmp_hadoop/output_Estimate
#$HADOOP_HOME/bin/hadoop fs -get /Users/liucancheng/Hadoop/output_Materialize ./tmp_hadoop/output_Materialize
#----------------------------------

# copy the final result files to the local machine
$HADOOP_HOME/bin/hadoop fs -get /Users/liucancheng/Hadoop/output/part-* $output_path
# remove the intermediate files
###$HADOOP_HOME/bin/hadoop fs -rmr /Users/liucancheng/Hadoop/output_Estimate
###$HADOOP_HOME/bin/hadoop fs -rmr /Users/liucancheng/Hadoop/output_Materialize
rm ./part-*0
# stop the hadoop cluster, job completed!
$HADOOP_HOME/bin/stop-all.sh

#!/bin/bash
# 重启 抽取日志脚本

# 案例 ./AccessLogToKafka.sh "/home/dwadmin/app/recommend/recommend-2.0/target/scala-2.10/recommend-2.0.jar"

# jar 的地址
JAR_PATH=$1

# 原来的 进程
ps -aux | grep ExtractFileToKafkaAccessLog | awk '{print $2}' | while read pid;
  do
    echo "old pid: ${pid}"
    kill -9 $pid;
  done


# 提交任务
java -Xms2048M -Xmx2048M -DAPP_NAME=ExtractFileToKafkaAccessLog \
-cp ${JAR_PATH} com.angejia.dw.recommend.extract.ExtractFileToKafka "uhadoop-ociicy-master1:2181" "bi4:9092" "accessLog" "0" "accessLogBase" "/data/log/real_time/logs/access_log" "2000" >> /data/log/real_time/logs/access_log_run 2>&1  & 


# 新的进程
ps -aux | grep ExtractFileToKafkaAccessLog | awk '{print $2}' | while read pid;
  do
    echo "new pid: ${pid}"
  done


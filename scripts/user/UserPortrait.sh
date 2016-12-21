#!/bin/bash
# 重启 用户画像 程序

# 案例 ./UserPortrait.sh "/home/dwadmin/app/recommend/recommend-2.0/target/scala-2.10/recommend-2.0.jar"

# jar 的地址
JAR_PATH=$1

# 删除 spark 提交时的临时文件
rm -rf /tmp/spark-*

# 原来的 进程
ps -aux | grep 'com.angejia.dw.recommend.user.portrait.UserPortrait' | awk '{print $2}' | while read pid;
  do
    echo "old pid: ${pid}"
    kill -9 $pid;
  done


# 提交任务
spark-submit \
--name UserPortrait \
--class com.angejia.dw.recommend.user.portrait.UserPortrait \
--master yarn \
--deploy-mode client \
--driver-cores 2 \
--driver-memory 4096M \
--executor-memory 2048M \
--executor-cores 2 \
--num-executors 2 \
${JAR_PATH} \
--env "online" \
--kafka-topic "accessLog" \
--kafka-consumer-gid "userPortrait"  >> /data/log/recommend/UserPortrait 2>&1  &


# 新的进程
ps -aux | grep 'com.angejia.dw.recommend.user.portrait.UserPortrait' | awk '{print $2}' | while read pid;
  do
    echo "new pid: ${pid}"
  done


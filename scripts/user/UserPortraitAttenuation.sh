#!/bin/bash
# 衰减用户画像

# 案例 ./UserPortraitAttenuation.sh "/home/dwadmin/app/recommend/recommend-2.0/target/scala-2.10/recommend-2.0.jar"

# jar 的地址
JAR_PATH=$1

# 原来的 进程
ps -aux | grep 'com.angejia.dw.recommend.user.portrait.UserPortraitAttenuation' | awk '{print $2}' | while read pid;
  do
    echo "old pid: ${pid}"
    kill -9 $pid;
  done


# 提交任务
java -DAPP_NAME=UserPortraitAttenuation \
-cp ${JAR_PATH} com.angejia.dw.recommend.user.portrait.UserPortraitAttenuation "online" "" >> /data/log/recommend/UserPortraitAttenuation 2>&1 &


# 新的进程
ps -aux | grep 'com.angejia.dw.recommend.user.portrait.UserPortraitAttenuation' | awk '{print $2}' | while read pid;
  do
    echo "new pid: ${pid}"
  done


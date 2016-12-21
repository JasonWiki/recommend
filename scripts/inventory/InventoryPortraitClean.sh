#!/bin/bash
# 清理无用房源画像

# 案例 ./InventoryPortraitClean "/home/dwadmin/app/recommend/recommend-2.0/target/scala-2.10/recommend-2.0.jar"

# jar 的地址
JAR_PATH=$1

# 原来的 进程
ps -aux | grep 'com.angejia.dw.recommend.inventory.portrait.InventoryPortraitClean' | awk '{print $2}' | while read pid;
  do
    echo "old pid: ${pid}"
    kill -9 $pid;
  done


# 提交任务
java -DAPP_NAME=InventoryPortraitClean \
-cp ${JAR_PATH} com.angejia.dw.recommend.inventory.portrait.InventoryPortraitClean "online" "" >> /data/log/recommend/InventoryPortraitClean 2>&1 &


# 新的进程
ps -aux | grep 'com.angejia.dw.recommend.inventory.portrait.InventoryPortraitClean' | awk '{print $2}' | while read pid;
  do
    echo "new pid: ${pid}"
  done


#!/bin/bash
# 房源索引服务

# 案例 ./PropertyInventoryIndex.sh "/home/hadoop/app/recommend/recommend-2.0/target/scala-2.10/recommend-2.0.jar"

# jar 的地址
JAR_PATH=$1


# 原来的 进程
ps -aux | grep 'com.angejia.dw.service.property.PropertyInventoryService' | awk '{print $2}' | while read pid;
  do
    echo "old pid: ${pid}"
    kill -15 $pid;
  done


java -DAPP_NAME=PropertyInventoryIndexService \
-cp ~/app/recommend/recommend-2.0/target/scala-2.10/recommend-2.0.jar \
com.angejia.dw.service.property.PropertyInventoryService \
"online" \
"/data/log/service/property/service_property_date_point" \
>> /data/log/service/property/service_property_extract 2>&1


# 新的进程
ps -aux | grep 'com.angejia.dw.service.property.PropertyInventoryService' | awk '{print $2}' | while read pid;
  do
    echo "new pid: ${pid}"
  done


#!/bin/bash
# 重启 CBCF 所有服务 ./CbcfService.sh "/home/dwadmin/app/recommend/recommend-2.0"

# 项目路径
PROJECT_HOME=$1 


# 远程重启 用户画像
echo "UserPortrait"
ssh -q -t dwadmin@bi4 "bash -i ${PROJECT_HOME}/scripts/user/UserPortrait.sh \"${PROJECT_HOME}/target/scala-2.11/recommend-2.0.jar\" "

# 远程重启 抽取日志脚本
echo "AccessLogToKafka"
ssh -q -t dwadmin@bi0 "bash -i ${PROJECT_HOME}/scripts/extract/AccessLogToKafka.sh \"${PROJECT_HOME}/target/scala-2.11/recommend-2.0.jar\" "

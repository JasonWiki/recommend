#!/bin/bash
# 重启 IBCF 所有服务 ./UbcfService.sh "/home/hadoop/app/recommend/recommend-2.0"

# 推荐系统项目路径
PROJECT_HOME=$1

# 在 task3 节点执行这个 sh
echo "UserUbcf"
ssh -q -t hadoop@uhadoop-ociicy-task3 "bash -i ${PROJECT_HOME}/scripts/user/UserUbcf.sh \"${PROJECT_HOME}/target/scala-2.10/recommend-2.0.jar\" "

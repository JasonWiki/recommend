#!/bin/bash
# 重启 IBCF 所有服务 ./InventoryIbcfService.sh "/home/hadoop/app/recommend/recommend-2.0"

# 推荐系统项目路径
PROJECT_HOME=$1

# 在 task3 节点执行这个 sh
echo "CommunityIBCF"
ssh -q -t hadoop@uhadoop-ociicy-task3 "bash -i ${PROJECT_HOME}/scripts/community/CommunityIBCF.sh \"${PROJECT_HOME}/target/scala-2.10/recommend-2.0.jar\" "


# dw_etl 项目路径
#DW_ETL_HOME=/home/dwadmin/app/dw_etl
# 把结果数据通过 HBASE 保存在 Hive 中
#${DW_ETL_HOME}/dw_service/index.py  --service task --mo hive_task --par '{"sql":"source/real_time/rt_recommend_inventroy_ibcf_result.sql", "date":"today", "runEnv":"local"}'
#!/bin/bash
# 衰减用户画像 ./UserPortraitAttenuationService.sh "/home/dwadmin/app/recommend/recommend-2.0"

# 项目路径
PROJECT_HOME=$1 

# 衰减用户画像
echo "UserPortraitAttenuation"
ssh -q -t dwadmin@bi4 "bash -i ${PROJECT_HOME}/scripts/user/UserPortraitAttenuation.sh \"${PROJECT_HOME}/target/scala-2.10/recommend-2.0.jar\" "

#
# Copyright (c) 2016 Intel Corporation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
#!/bin/bash
set -e
APP_URL=http://`cf app hdfs-hive-demo|grep urls| awk '{print $2}'`
echo -n "please enter directory name to create>" 
read DIRECTORY_NAME

CSV_FILE=`cat sample.csv`

JSON=`./tools/curlo.sh -X POST $APP_URL/rest/directory/$DIRECTORY_NAME`
echo "$JSON" | jq .
HDFS_DIR_PATH=`echo $JSON | jq .hdfsPath | sed -e 's/\"//g'`
printf " --- directory $HDFS_DIR_PATH created ---\n"

echo -n "please enter file name>$DIRECTORY_NAME/" 
read FILE_NAME

JSON=`./tools/curlo.sh -X POST $APP_URL/rest/file/$DIRECTORY_NAME/$FILE_NAME --form "text=$CSV_FILE"`
echo "$JSON" | jq .
HDFS_FILE_PATH=`echo $JSON | jq .hdfsPath | sed -e 's/\"//g'`
printf " --- file $HDFS_FILE_PATH created ---\n"


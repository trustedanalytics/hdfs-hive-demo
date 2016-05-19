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
echo -n "Please enter hdfs path to directory of the dataset>"
read DIRECTORY
echo -n "Please enter hdfs path to file with header>" 
read FILE_NAME
echo -n "Please enter hive table name>"
read TABLE_NAME
echo -n "Please enter hive table column name>"
read COLUMN_NAME
./tools/curlo.sh -X POST "$APP_URL/rest/hive/$TABLE_NAME?fullHdfsDirPath=$DIRECTORY&headerFilePath=$FILE_NAME" | jq .
echo "Selecting from table:"
./tools/curlo.sh -X GET $APP_URL/rest/hive/$TABLE_NAME
echo "Selecting column from table:"
./tools/curlo.sh -X GET $APP_URL/rest/hive/$TABLE_NAME/$COLUMN_NAME
echo "Deleting table:"
./tools/curlo.sh -X DELETE $APP_URL/rest/hive/$TABLE_NAME
echo "Table was deleted."
echo "Selecting again from table:"
./tools/curlo.sh -X GET $APP_URL/rest/hive/$TABLE_NAME

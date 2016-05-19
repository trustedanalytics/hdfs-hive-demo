/**
 * Copyright (c) 2016 Intel Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.trustedanalytics.samples.services;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.trustedanalytics.hadoop.config.client.helper.Hive;
import org.trustedanalytics.samples.OauthUtils;
import org.apache.commons.lang3.StringUtils;

import javax.security.auth.login.LoginException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Provides basic Hive access.
 *
 */

@Service
@SuppressWarnings("checkstyle:javadocmethod")
public class HiveService {

    private static final Logger LOGGER = LoggerFactory.getLogger(HiveService.class);
    private static final String DELIMITER = ",";

    @Autowired
    HdfsService hdfsService;

    /**
     *   Creates external hive table from hdfs directory. The assumption is that directory contains
     *   CSV file(s), delimited by ","
     *
     *
     *   @param hdfsDirectory absolute path to drirectory on hdfs
     *   @param tableId       name for table, must be unique
     *   @param header        first(header) row of CSV file -
     *
     *
     * @throws LoginException login exception
     * @throws URISyntaxException uri syntax exception
     * @throws InterruptedException interrupted exception
     * @throws IOException io exception
     * @throws SQLException sqlexception
     */
    public void createExternalTable(String hdfsDirectory, String tableId, String header) throws LoginException, URISyntaxException, InterruptedException, IOException, SQLException {

        LOGGER.info("Creating table from file {} ", hdfsDirectory);
        try ( Connection hiveConenction = Hive.newInstance().getConnection(OauthUtils.getJwtToken()) ) {

            String tableHeader = columnsFromFileHeader(header);
            String sql = "create external table "
                    + tableId + " (" + tableHeader + ") row format delimited fields terminated by '"
                    + DELIMITER + "' stored as TEXTFILE location '" + hdfsDirectory + "'";

            LOGGER.info("Executing Hive Sql statement {} ", sql);
            Statement stmt = hiveConenction.createStatement();
            stmt.executeUpdate(sql);
        }
    }

    /**
     *   Removes hive table
     *
     *   @param tableId       name for table, must be unique
     *
     *
     * @throws LoginException login exception
     * @throws URISyntaxException uri syntax exception
     * @throws InterruptedException interrupted exception
     * @throws IOException io exception
     * @throws SQLException sqlexception
     */
    public void deleteTable(String tableId) throws LoginException, URISyntaxException, InterruptedException, IOException, SQLException {

        LOGGER.info("Deleting table of id {} ", tableId);
        try ( Connection hiveConenction = Hive.newInstance().getConnection(OauthUtils.getJwtToken()) ) {

            String sql = "drop table if exists " + tableId;

            LOGGER.info("Executing Hive Sql statement {} ", sql);
            Statement stmt = hiveConenction.createStatement();
            stmt.executeUpdate(sql);
        }
    }

    /**
     *   Fetching whole hive table or column.
     *
     *   @param tableId table id
     *   @param columnName column name
     *   @return String containing formatted table
     *
     *
     * @throws IOException io exception
     * @throws LoginException login exception
     * @throws InterruptedException interrupted exception
     * @throws URISyntaxException uri syntax exception
     * @throws SQLException sql exception
     */
    public String selectFromHiveTable(String tableId, String columnName) throws IOException, LoginException, InterruptedException, URISyntaxException, SQLException {
        String selectColumns = "*";
        if(StringUtils.isNotEmpty(columnName)) {
            selectColumns = columnName;
        }
        LOGGER.info("Selecting {} from table of id {} ", selectColumns, tableId);
        Hive hive = Hive.newInstance();
        try (Connection hiveConenction = hive.getConnection(OauthUtils.getJwtToken()) ) {

            try (Statement stmt = hiveConenction.createStatement()) {
                String sql = "select " + selectColumns + " from " + tableId;

                LOGGER.info("Executing Hive Sql statement {}", sql);
                try (ResultSet rs = stmt.executeQuery(sql)) {
                    String result = "";
                    while (rs.next()) {
                        if (rs != null) {
                            for (int i = 1; i <= rs.getMetaData().getColumnCount(); ++i) {
                                result += rs.getString(i) + " ";
                            }
                            result += "\n";
                        }
                    }
                    return result;
                }
            }
        }
    }

    /**
     *  Converts CSV file row into SQL readable string
     *
     * @param header first(header) row of CSV file, may look like: val1, val2, val3
     * @return hive sql readable file header, like: colval1 string, colval2 string, colval3 string
     *
     *
     * @throws IOException io exception
     * @throws LoginException login exception
     * @throws InterruptedException interrupted exception
     * @throws URISyntaxException uri syntax exception
     */
    private String columnsFromFileHeader(String header) throws IOException, LoginException, InterruptedException, URISyntaxException {
        String tableHeader = header.replaceAll("([0-9a-zA-Z])( )([0-9a-zA-Z])","$1_$3"); // replace spaces with underscores
        tableHeader = tableHeader.replaceAll(DELIMITER, " string" + DELIMITER);
        tableHeader += " string";
        tableHeader = tableHeader.replaceAll("([0-9] string)","col$1"); //make sure colum name does not start with digit
        tableHeader = tableHeader.replaceAll("[-.]","_");
        return tableHeader;
    }
}

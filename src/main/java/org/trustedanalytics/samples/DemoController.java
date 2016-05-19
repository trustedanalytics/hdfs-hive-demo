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
package org.trustedanalytics.samples;

import io.swagger.annotations.ApiOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.HandlerMapping;
import org.trustedanalytics.samples.model.HdfsObject;
import org.trustedanalytics.samples.model.HiveTable;
import org.trustedanalytics.samples.services.HdfsService;
import org.trustedanalytics.samples.services.HiveService;

import javax.security.auth.login.LoginException;
import javax.servlet.http.HttpServletRequest;
import javax.xml.ws.Response;
import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.SQLException;

@RestController
public class DemoController {

    private static final Logger LOGGER = LoggerFactory.getLogger(DemoController.class);
    public static final String ENDPOINT_REST_HIVE_TABLE_ID = "/rest/hive/{tableId}";
    public static final String ENDPOINT_REST_HIVE_TABLE_ID_COLUMN_NAME = "/rest/hive/{tableId}/{columnName}";
    public static final String ENDPOINT_REST_FILE = "/rest/file/";
    public static final String ENDPOINT_REST_DIRECTORY = "/rest/directory/";


    private final HdfsService hdfsService;
    private final HiveService hiveService;

    @Autowired
    public DemoController(HdfsService hdfsService, HiveService hiveService) {
        this.hdfsService = hdfsService;
        this.hiveService = hiveService;
    }

    @ApiOperation(
            value = "Creating file on hdfs with content specified in 'text' parameter",
            notes = "Using relative path is recommended"
    )
    @RequestMapping(method = RequestMethod.POST, value = ENDPOINT_REST_FILE + "**")
    public HdfsObject createFile(
                     @RequestParam("text") String text, HttpServletRequest request)
        throws IOException, LoginException, InterruptedException, URISyntaxException {
        return new HdfsObject(request.getRequestURL().toString(), hdfsService.createFile(extractFilePathFromRequest(request, ENDPOINT_REST_FILE), text).toString(), false);
    }

    @ApiOperation(
            value = "Creating directory on hdfs",
            notes = "Using relative path is recommended to ensure access permissions"
    )
    @RequestMapping(method = RequestMethod.POST, value = ENDPOINT_REST_DIRECTORY + "**")
    public HdfsObject createDirectory(
            HttpServletRequest request)
            throws IOException, LoginException, InterruptedException, URISyntaxException {
        return new HdfsObject(request.getRequestURL().toString(), hdfsService.createDirectory(extractFilePathFromRequest(request, ENDPOINT_REST_DIRECTORY)).toString(), true);
    }

    @ApiOperation(
            value = "Reading file from hdfs",
            notes = "Using relative path is recommended to ensure access permissions"
    )
    @RequestMapping(method = RequestMethod.GET, value = ENDPOINT_REST_FILE + "**")
    public String readFile(HttpServletRequest request)
            throws IOException, LoginException, InterruptedException, URISyntaxException {
        return hdfsService.readFile(extractFilePathFromRequest(request, ENDPOINT_REST_FILE));
    }

    @ApiOperation(
            value = "Creating hive table",
            notes = "Parameters: unique tableId id, absolute hdfs path to directory, path to CSV" +
                    "file with header"
    )
    @RequestMapping(method = RequestMethod.POST, value = ENDPOINT_REST_HIVE_TABLE_ID)
    public HiveTable createHiveTable(@PathVariable("tableId") String tableId,
                                  @RequestParam("fullHdfsDirPath") String fullHdfsDirPath,
                                  @RequestParam("headerFilePath") String headerFilePath, HttpServletRequest request)
            throws IOException, LoginException, InterruptedException, URISyntaxException, SQLException {
        LOGGER.info("Creating table from file {} ", fullHdfsDirPath);
        String header = hdfsService.readFileHeader(headerFilePath);
        hiveService.createExternalTable(fullHdfsDirPath, tableId, header);
        return new HiveTable(tableId, request.getRequestURL().toString());
    }

    @ApiOperation(
            value = "Reading table from hive",
            notes = "Table must exist in database associated with hive-shared plan"
    )
    @RequestMapping(method = RequestMethod.GET, value = ENDPOINT_REST_HIVE_TABLE_ID)
    public String fetchHiveTable(@PathVariable("tableId") String tableId)
            throws IOException, LoginException, InterruptedException, URISyntaxException, SQLException {
        return hiveService.selectFromHiveTable(tableId, null);
    }

    @ApiOperation(
            value = "Reading column from hive table",
            notes = "Table and column must exist in database associated with hive-shared plan"
    )
    @RequestMapping(method = RequestMethod.GET, value = ENDPOINT_REST_HIVE_TABLE_ID_COLUMN_NAME)
    public String fetchColumnFromHiveTable(@PathVariable("tableId") String tableId, @PathVariable("columnName") String columnName)
            throws IOException, LoginException, InterruptedException, URISyntaxException, SQLException {
        return hiveService.selectFromHiveTable(tableId, columnName);
    }

    @ApiOperation(
            value = "Removes hive table",
            notes = "Table must exist in database associated with hive-shared plan"
    )
    @RequestMapping(method = RequestMethod.DELETE, value = ENDPOINT_REST_HIVE_TABLE_ID)
    public void deleteHiveTable(@PathVariable("tableId") String tableId)
            throws IOException, LoginException, InterruptedException, URISyntaxException, SQLException {
        hiveService.deleteTable(tableId);
    }

    /**
     * Extracting argument from rest path. The purpose is to make API interface more natural to use.
     * For example, you can refer to my_directory/my_file path on hdfs in the form /rest/file/my_directory/my_file
     *
     * @param request
     * @param restPath
     * @return
     */
    private String extractFilePathFromRequest(HttpServletRequest request, String restPath) {
        return request.getAttribute(
                HandlerMapping.PATH_WITHIN_HANDLER_MAPPING_ATTRIBUTE).toString().replaceFirst(restPath, "");
    }

}

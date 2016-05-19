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

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.trustedanalytics.hadoop.config.client.*;
import org.trustedanalytics.hadoop.config.client.helper.Hdfs;
import org.trustedanalytics.samples.OauthUtils;
import org.trustedanalytics.samples.utils.FsPermissionHelper;

import javax.security.auth.login.LoginException;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.URISyntaxException;
import java.nio.charset.Charset;

@Service
@SuppressWarnings("checkstyle:javadocmethod")
public class HdfsService {

    private static final Logger LOGGER = LoggerFactory.getLogger(HdfsService.class);

    /**
     *  Creates file on hdfs.
     *
     *  @param filePath relative path to the file
     *  @param text     text to be written into the file
     *
     *  @return full hdfs path to file
     *
     *
     * @throws IOException io exception
     * @throws LoginException login exception
     * @throws InterruptedException interrupted exception
     * @throws URISyntaxException uri syntax exception
     */

    public Path createFile(String filePath, String text) throws IOException, LoginException, InterruptedException, URISyntaxException {
        FileSystem fs = initializeFileSystemWithContext();
        return createFile(fs, filePath, text);
    }

    /**
     *  Creates directory on hdfs.
     *
     *  @param directoryPath relative path to the directory
     *
     *  @return full hdfs path to directory
     *
     *
     * @throws IOException io exception
     * @throws LoginException login exception
     * @throws InterruptedException interrupted exception
     * @throws URISyntaxException uri syntax exception
     */

    public Path createDirectory(String directoryPath) throws IOException, LoginException, InterruptedException, URISyntaxException {
        FileSystem fs = initializeFileSystemWithContext();
        return createDirectory(fs, directoryPath);
    }

    /**
     * Reads file from hdfs.
     *
     * @param filePath relative path to file
     *
     * @return String containing whole file text
     *
     * @throws IOException io exception
     * @throws LoginException login exception
     * @throws InterruptedException interrupted exception
     * @throws URISyntaxException uri syntax exception
     */
    public String readFile(String filePath) throws IOException, LoginException, InterruptedException, URISyntaxException {
        FileSystem fs = initializeFileSystemWithContext();
        return readFileFromHdfs(fs, filePath);
    }

    /**
     * Reads first line from file.
     *
     * @param filePath relative path to hdfs file
     *
     * @return String containing first line of file
     *
     * @throws IOException io exception
     * @throws LoginException login exception
     * @throws InterruptedException interrupted exception
     * @throws URISyntaxException uri syntax exception
     */
    public String readFileHeader(String filePath) throws IOException, LoginException, InterruptedException, URISyntaxException {
        FileSystem fs = initializeFileSystemWithContext();
        Path path = new Path(filePath);
        String header ="";
        try (BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(path))) ){
            header = br.readLine();
        }
        return header;
    }

    /**
     * Create file.
     *
     * @param fs configured Hadoop FileSystem
     * @param filePath path to the file
     * @param text file content
     * @return full hdfs path to a file
      @throws IOException io exception
     */

    private Path createFile(FileSystem fs, String filePath, String text) throws IOException {
        Path path = new Path(filePath);
        try ( OutputStream os = fs.create(path) ) {
            os.write(text.getBytes(Charset.forName("UTF-8")));
            fs.setPermission(path, FsPermission.valueOf("-rwxrwxrwx"));
            return fs.getFileStatus(path).getPath();
        }
    }

    /**
     * Create directory inside file system.
     *
     * @param fs configured Hadoop FileSystem
     * @param filePath path to the directory
     * @return path to the directory
      @throws IOException io exception
     */

    private Path createDirectory(FileSystem fs, String filePath) throws IOException {
        Path path = new Path(filePath);
        fs.mkdirs(path);
        fs.setPermission(path, FsPermission.valueOf("drwxrwxrwx"));
        fs.modifyAclEntries(path, FsPermissionHelper.getDefaultAclsForTechnicalUsers(FsPermissionHelper.getToolUsers(), FsAction.ALL));
        fs.modifyAclEntries(path, FsPermissionHelper.getAclsForTechnicalUsers(FsPermissionHelper.getToolUsers(), FsAction.ALL));
        return fs.getFileStatus(path).getPath();
    }

    /**
     * Reads file from hdfs
     *
     * @param fs configured Hadoop FileSystem
     * @param filePath path to the file
     * @return String containing file
     *
     * @throws io exception
     */
    private String readFileFromHdfs(FileSystem fs, String filePath) throws IOException {
        try (InputStream is = fs.open(new Path(filePath))) {
            return IOUtils.toString(is);
        }
    }

    /**
     * Create hadoop File System object with jwtToken. This object will have permissions specific to the
     * TAP user represented by jwtToken. Working directory will be set to the hdfs-shared plan instance folder.
     *
     * @return
     *
     *
     * @throws IOException io exception
     * @throws LoginException login exception
     * @throws InterruptedException interrupted exception
     * @throws URISyntaxException uir syntax exception
     */
    private FileSystem initializeFileSystemWithContext() throws IOException, LoginException, InterruptedException, URISyntaxException {
        FileSystem fs = Hdfs.newInstance().createFileSystem(OauthUtils.getJwtToken());
        setupWorkingDirectory(fs);
        return fs;
    }

    /**
     * Setup FileSystem working directory for FileSystem object.
     *
     * @param fs FileSystem object will be modified so specific directory
     *           with access rights will be set as working directory
     *
     * @return Path to working directory specifed by hdfs-shared instance
     *
     * @throws IOException io exception
     */
    private Path setupWorkingDirectory(FileSystem fs) throws IOException {
        AppConfiguration appEnvConf = Configurations.newInstanceFromEnv();
        ServiceInstanceConfiguration hdfsConf = appEnvConf.getServiceConfig(ServiceType.HDFS_TYPE);

        // Fetching home directory URI from application environment - ServiceType.HDFS_TYPE
        // the URI is specific for hdfs-shared instance

        Path path = new Path(hdfsConf.getProperty(Property.HDFS_URI).get());

        // We will have read/write access to directory described in path variable
        fs.setWorkingDirectory(path);

        // returning a path just in case someone want to use it
        return path;
    }

}

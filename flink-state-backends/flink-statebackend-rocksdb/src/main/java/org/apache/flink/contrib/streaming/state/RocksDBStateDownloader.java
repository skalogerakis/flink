/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.StateHandleID;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.filesystem.FileStateHandle;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.function.ThrowingRunnable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.*;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Help class for downloading RocksDB state files. */
public class RocksDBStateDownloader extends RocksDBStateDataTransfer {

    private static final Logger logger = LoggerFactory.getLogger(RocksDBStateDownloader.class);

    public RocksDBStateDownloader(int restoringThreadNum) {
        super(restoringThreadNum);
    }

    /**
     * Transfer all state data to the target directory using specified number of threads.
     *
     * @param restoreStateHandle Handles used to retrieve the state data.
     * @param dest The target directory which the state data will be stored.
     * @throws Exception Thrown if can not transfer all the state data.
     */
    public void transferAllStateDataToDirectory(
            IncrementalRemoteKeyedStateHandle restoreStateHandle,
            Path dest,
            CloseableRegistry closeableRegistry)
            throws Exception {

        final Map<StateHandleID, StreamStateHandle> sstFiles = restoreStateHandle.getSharedState();
        final Map<StateHandleID, StreamStateHandle> miscFiles =
                restoreStateHandle.getPrivateState();

        int newPolicyFlag = 1;
        HashMap<String, HashMap<String, String>> hdfsPathBlockRegistry =
                new HashMap<String, HashMap<String, String>>();

        Instant start_time_transfer_complete = Instant.now();

        if (newPolicyFlag == 0) {
            // Execute the HDFS command in case we want the new policy
            Optional<Map.Entry<StateHandleID, StreamStateHandle>> firstKey =
                    sstFiles.entrySet().stream().findFirst();

            if (firstKey.isPresent()) {
                StreamStateHandle remoteFileHandle = firstKey.get().getValue();

                String hdfsFileInfo = ((FileStateHandle) remoteFileHandle).getFilePath().toString();

                String hdfsFilePath = hdfsFileInfo.substring(0, hdfsFileInfo.lastIndexOf("shared"));

                /** * HDFS FSCK CMD ** */
                try {
                    // Build the hdfs info command to find all the blocks for a given file
                    String[] hdfsInfoCmd = {"hdfs", "fsck", hdfsFilePath, "-files", "-blocks"};

                    ProcessBuilder hdfs_pb = new ProcessBuilder(hdfsInfoCmd);
                    Process hdfs_proc = hdfs_pb.start();

                    // Wait for the command to complete and check status
                    CommandExecutionStatus(hdfs_proc.waitFor(), 9);

                    // Regex that matches the desired input
                    String regexPattern = ".*(\\d+)\\. (BP-\\w.+).* len=(\\d*) Live_repl=(\\d*).*";
                    Pattern pattern = Pattern.compile(regexPattern);

                    BufferedReader hdfs_cmd_output =
                            new BufferedReader(new InputStreamReader(hdfs_proc.getInputStream()));

                    // read the output from the command
                    String s = null;

                    String key_path = null;
                    HashMap<String, String> internal = new HashMap<String, String>();

                    while ((s = hdfs_cmd_output.readLine()) != null) {

                        if (s.startsWith("/")) {
                            // Updates the file path that is used as key
                            key_path = s.split(" ")[0];
                        } else if (s.isEmpty() && !internal.isEmpty()) {
                            // Update the hashmap, key as file_path
                            hdfsPathBlockRegistry.put("hdfs:" + key_path, internal);
                            internal = new HashMap<String, String>();
                        }

                        // We don't need extra information from that point on
                        if (s.startsWith("Status")) break;

                        Matcher matcher = pattern.matcher(s);

                        // When the pattern finds a match
                        if (matcher.find()) {

                            // Your regex matched the line, take values from the regex groups
                            // String block_num = matcher.group(1); //The number of the block
                            String info_loc = matcher.group(2); // The information about location

                            String[] split_info = info_loc.split(":");
                            String block_pool_id = split_info[0]; // Block Pool Id (used in path)
                            String block_id = split_info[1]; // Specific Block

                            String block_id_path = block_id.substring(0, block_id.lastIndexOf("_"));

                            // Place all the blocks to the internal hashmap
                            internal.put(block_id_path, block_pool_id);
                        }
                    }
                } catch (IOException | InterruptedException io) {
                    io.printStackTrace();
                }
            }
        }

        Instant start_time_transfer = Instant.now();

        /*
           remoteFileHandle categories -> FileState & ByteStreamStateHandle
           FileState -> Actual file and can perform the new logic with local copy
           ByteStreamStateHandle -> Not actual files seems to be state stored in memory,
           so use the old method
        */

        // Mode 1 -> For the default behavior, Mode 0 -> For updated version of .sst files
        downloadDataForAllStateHandles(
                sstFiles, dest, closeableRegistry, newPolicyFlag, hdfsPathBlockRegistry);
        downloadDataForAllStateHandles(
                miscFiles, dest, closeableRegistry, 1, hdfsPathBlockRegistry);

        Instant end_time_transfer = Instant.now();
        logger.info(
                "transferAllStateDataToDirectory Duration Full "
                        + Duration.between(start_time_transfer_complete, end_time_transfer)
                                .toMillis()
                        + "\t Duration No HDFS: "
                        + +Duration.between(start_time_transfer, end_time_transfer).toMillis());
    }

    /**
     * Copies all the files from the given stream state handles to the given path, renaming the
     * files w.r.t. their {@link StateHandleID}. mode -> 0 for sst files and 1 for the rest
     */
    private void downloadDataForAllStateHandles(
            Map<StateHandleID, StreamStateHandle> stateHandleMap,
            Path restoreInstancePath,
            CloseableRegistry closeableRegistry,
            int mode,
            HashMap<String, HashMap<String, String>> hdfsPathBlockRegistry)
            throws Exception {

        try {
            List<Runnable> runnables =
                    createDownloadRunnables(
                            stateHandleMap,
                            restoreInstancePath,
                            closeableRegistry,
                            mode,
                            hdfsPathBlockRegistry);

            List<CompletableFuture<Void>> futures = new ArrayList<>(runnables.size());
            for (Runnable runnable : runnables) {
                futures.add(CompletableFuture.runAsync(runnable, executorService));
            }
            FutureUtils.waitForAll(futures).get();
        } catch (ExecutionException e) {
            Throwable throwable = ExceptionUtils.stripExecutionException(e);
            throwable = ExceptionUtils.stripException(throwable, RuntimeException.class);
            if (throwable instanceof IOException) {
                throw (IOException) throwable;
            } else {
                throw new FlinkRuntimeException("Failed to download data for state handles.", e);
            }
        }
    }

    private List<Runnable> createDownloadRunnables(
            Map<StateHandleID, StreamStateHandle> stateHandleMap,
            Path restoreInstancePath,
            CloseableRegistry closeableRegistry,
            int mode,
            HashMap<String, HashMap<String, String>> hdfsPathBlockRegistry) {
        // THIS IS THE FULL PATH (NO SST)
        // logger.info("createDownloadRunnables {}", restoreInstancePath.toString());

        List<Runnable> runnables = new ArrayList<>(stateHandleMap.size());

        /** * MODE 0 -> Updated version, MODE 1 -> Default implementation** */
        if (mode == 0) {
            //            int counter = 0;
            //            HashMap<String, HashMap<String, String>> hdfsPathBlockRegistry =
            //                    new HashMap<String, HashMap<String, String>>();

            for (Map.Entry<StateHandleID, StreamStateHandle> entry : stateHandleMap.entrySet()) {
                StateHandleID stateHandleID = entry.getKey();
                StreamStateHandle remoteFileHandle = entry.getValue();

                Path path = restoreInstancePath.resolve(stateHandleID.toString());

                logger.info(
                        "stateHandleID -> {}, remoteFileHandle -> {}",
                        stateHandleID.toString(),
                        remoteFileHandle.toString());
                /*
                StateHandleID.toString -> .sst file name (000016.sst)
                remoteFileHandle.toString() -> Full Hashed HDFS path (hdfs:/flink-checkpoints/aa022317926ec50f619ab032b8fa7bb9/shared/fc61fd22-9864-4d4e-82c3-be40df310e5f)
                path.toString() -> Full path with .sst
                 */

                //                if (counter == 0) {
                //
                //                    String hdfsFileInfo =
                //                            ((FileStateHandle)
                // remoteFileHandle).getFilePath().toString();
                //
                //                    String hdfsFilePath =
                //                            hdfsFileInfo.substring(0,
                // hdfsFileInfo.lastIndexOf("shared"));
                //
                //                    /** * HDFS FSCK CMD ** */
                //                    try {
                //                        // Build the hdfs info command to find all the blocks for
                // a given file
                //                        String[] hdfsInfoCmd = {"hdfs", "fsck", hdfsFilePath,
                // "-files", "-blocks"};
                //
                //                        ProcessBuilder hdfs_pb = new ProcessBuilder(hdfsInfoCmd);
                //                        Process hdfs_proc = hdfs_pb.start();
                //
                //                        // Wait for the command to complete and check status
                //                        CommandExecutionStatus(hdfs_proc.waitFor(), 9);
                //
                //                        // Regex that matches the desired input
                //                        String regexPattern =
                //                                ".*(\\d+)\\. (BP-\\w.+).* len=(\\d*)
                // Live_repl=(\\d*).*";
                //                        Pattern pattern = Pattern.compile(regexPattern);
                //
                //                        BufferedReader hdfs_cmd_output =
                //                                new BufferedReader(
                //                                        new
                // InputStreamReader(hdfs_proc.getInputStream()));
                //
                //                        // read the output from the command
                //                        String s = null;
                //
                //                        String key_path = null;
                //                        HashMap<String, String> internal = new HashMap<String,
                // String>();
                //
                //                        while ((s = hdfs_cmd_output.readLine()) != null) {
                //
                //                            if (s.startsWith("/")) {
                //                                // Updates the file path that is used as key
                //                                key_path = s.split(" ")[0];
                //                            } else if (s.isEmpty() && !internal.isEmpty()) {
                //                                // Update the hashmap, key as file_path
                //                                hdfsPathBlockRegistry.put("hdfs:" + key_path,
                // internal);
                //                                internal = new HashMap<String, String>();
                //                            }
                //
                //                            // We don't need extra information from that point on
                //                            if (s.startsWith("Status")) break;
                //
                //                            Matcher matcher = pattern.matcher(s);
                //
                //                            // When the pattern finds a match
                //                            if (matcher.find()) {
                //
                //                                // Your regex matched the line, take values from
                // the regex groups
                //                                // String block_num = matcher.group(1); //The
                // number of the block
                //                                String info_loc =
                //                                        matcher.group(2); // The information about
                // location
                //
                //                                String[] split_info = info_loc.split(":");
                //                                String block_pool_id =
                //                                        split_info[0]; // Block Pool Id (used in
                // path)
                //                                String block_id = split_info[1]; // Specific Block
                //
                //                                String block_id_path =
                //                                        block_id.substring(0,
                // block_id.lastIndexOf("_"));
                //
                //                                // Place all the blocks to the internal hashmap
                //                                internal.put(block_id_path, block_pool_id);
                //                            }
                //                        }
                //                    } catch (IOException | InterruptedException io) {
                //                        io.printStackTrace();
                //                    }
                //                }

                String hdfsFilePath = ((FileStateHandle) remoteFileHandle).getFilePath().toString();

                runnables.add(
                        ThrowingRunnable.unchecked(
                                () ->
                                        downloadDataForStateHandle(
                                                path,
                                                remoteFileHandle,
                                                closeableRegistry,
                                                hdfsPathBlockRegistry.get(hdfsFilePath))));
            }
        } else {

            for (Map.Entry<StateHandleID, StreamStateHandle> entry : stateHandleMap.entrySet()) {
                StateHandleID stateHandleID = entry.getKey();
                StreamStateHandle remoteFileHandle = entry.getValue();

                Path path = restoreInstancePath.resolve(stateHandleID.toString());

                logger.info(
                        "stateHandleID -> {}, remoteFileHandle -> {}",
                        stateHandleID.toString(),
                        remoteFileHandle.toString());

                /*
                StateHandleID.toString -> .sst file name (000016.sst)
                remoteFileHandle.toString() -> Full Hashed HDFS path (hdfs:/flink-checkpoints/aa022317926ec50f619ab032b8fa7bb9/shared/fc61fd22-9864-4d4e-82c3-be40df310e5f)
                path.toString() -> Full path with .sst
                 */

                runnables.add(
                        ThrowingRunnable.unchecked(
                                () ->
                                        downloadDataForStateHandle_LEGACY(
                                                path, remoteFileHandle, closeableRegistry)));
            }
        }

        return runnables;
    }

    /**
     * Copies the file from a single state handle to the given path. NOTE: This is the old version
     * of the implementation. In case you wish to roll back to old version rename
     * downloadDataForStateHandle_LEGACY to downloadDataForStateHandle
     */
    private void downloadDataForStateHandle_LEGACY(
            Path restoreFilePath,
            StreamStateHandle remoteFileHandle,
            CloseableRegistry closeableRegistry)
            throws IOException {

        Instant start_time_default = Instant.now();
        // THIS IS THE FULL PATH CONTAINING THE .SST FILE
        // logger.info("hdfsFile PATH -> {}, local -> {}", remoteFileHandle.toString(),
        // restoreFilePath.toString());

        FSDataInputStream inputStream = null;
        OutputStream outputStream = null;

        try {
            inputStream = remoteFileHandle.openInputStream();
            closeableRegistry.registerCloseable(inputStream);

            Files.createDirectories(restoreFilePath.getParent());
            outputStream = Files.newOutputStream(restoreFilePath);
            closeableRegistry.registerCloseable(outputStream);

            byte[] buffer = new byte[8 * 1024];
            while (true) {
                int numBytes = inputStream.read(buffer);
                if (numBytes == -1) {
                    break;
                }

                outputStream.write(buffer, 0, numBytes);
            }
        } finally {
            if (closeableRegistry.unregisterCloseable(inputStream)) {
                inputStream.close();
            }

            if (closeableRegistry.unregisterCloseable(outputStream)) {
                outputStream.close();
            }
            Instant end_time_default = Instant.now();
            logger.info(
                    "DEFAULT Duration "
                            + Duration.between(start_time_default, end_time_default).toMillis()
                            + "\tFILE "
                            + remoteFileHandle.toString());
        }
    }

    private void downloadDataForStateHandle(
            Path restoreFilePath,
            StreamStateHandle remoteFileHandle,
            CloseableRegistry closeableRegistry,
            HashMap<String, String> hdfsFullFileInfo)
            throws IOException {
        /*
           restoreFilePath.toString() -> FULL PATH CONTAINING THE .SST FILE
           remoteFileHandle.getFilePath().toString() -> The HDFS path to the file
        */

        Instant start_time_upd = Instant.now();

        String localOutputFilePath =
                restoreFilePath.toString(); // The local path where the file will be moved

        // Create all the subdirectories that don't already exist in the local FS
        Files.createDirectories(Paths.get(localOutputFilePath).getParent());

        //        String hdfsFilePath = ((FileStateHandle)
        // remoteFileHandle).getFilePath().toString();

        // Init the cat cmd
        List<String> concat_cmd =
                new ArrayList<String>() {
                    {
                        add("cat");
                    }
                };

        try {
            /** * FIND CMD ** */
            //            HashMap<String, String> hdfsFullFileInfo =
            //                    complete_registry.get(hdfsFilePath.replace("hdfs:", ""));

            //            HashMap<String, String> hdfsFullFileInfo =
            // complete_registry.get(hdfsFilePath);

            for (Map.Entry<String, String> entry : hdfsFullFileInfo.entrySet()) {
                String block_id_path = entry.getKey();
                String block_pool_id = entry.getValue();

                String find_search_path =
                        "/tmp/hadoop-fs-tmp/current/" + block_pool_id + "/current/finalized";

                String[] find_command = {"find", find_search_path, "-name", block_id_path};

                // Execute the command
                ProcessBuilder find_pb = new ProcessBuilder(find_command);
                Process find_proc = find_pb.start();

                // Wait for the command to complete
                CommandExecutionStatus(find_proc.waitFor(), 1);

                BufferedReader find_cmd_output =
                        new BufferedReader(new InputStreamReader(find_proc.getInputStream()));

                //                Instant end_time_find = Instant.now();
                //                logger.info(
                //                        "FIND "
                //                                + Duration.between(start_time_upd,
                // end_time_find).toMillis()
                //                                + "\tFILE "
                //                                + remoteFileHandle.toString());

                String line = null;
                // We are expecting to find one path only. In case no path Error
                if ((line = find_cmd_output.readLine()) != null) concat_cmd.add(line);
                else
                    logger.error(
                            "ERROR -> Could not find "
                                    + find_search_path
                                    + ", with BlockID: "
                                    + block_id_path);
            }

            /** * CONCAT CMD ** */
            // Finish with the concat process after finding all the blocks
            ProcessBuilder concat_pb = new ProcessBuilder(concat_cmd);
            concat_pb.redirectOutput(ProcessBuilder.Redirect.to(new File(localOutputFilePath)));
            Process concat_proc = concat_pb.start();

            //            Instant end_time_cat = Instant.now();
            //            logger.info(
            //                    "CAT Duration "
            //                            + Duration.between(start_time_upd,
            // end_time_cat).toMillis()
            //                            + "\tFILE "
            //                            + remoteFileHandle.toString());

            CommandExecutionStatus(concat_proc.waitFor(), 3);

            Instant end_time_upd = Instant.now();
            logger.info(
                    "CAT + FIND Duration "
                            + Duration.between(start_time_upd, end_time_upd).toMillis()
                            + "\tFILE "
                            + remoteFileHandle.toString());

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    //    private void downloadDataForStateHandle(
    //            Path restoreFilePath,
    //            StreamStateHandle remoteFileHandle,
    //            CloseableRegistry closeableRegistry)
    //            throws IOException {
    //        /*
    //           restoreFilePath.toString() -> FULL PATH CONTAINING THE .SST FILE
    //           remoteFileHandle.getFilePath().toString() -> The HDFS path to the file
    //        */
    //
    //        // logger.info("downloadDataForStateHandle restore PATH -> {}",
    //        // restoreFilePath.toString());
    //
    //        Instant start_time = Instant.now();
    //
    //        String localOutputFilePath =
    //                restoreFilePath.toString(); // The local path where the file will be moved
    //
    //        // Create all the subdirectories that don't already exist in the local FS
    //        Files.createDirectories(Paths.get(localOutputFilePath).getParent());
    //
    //        String hdfsFilePath = null;
    //
    //        /*
    //           remoteFileHandle categories -> FileState & ByteStreamStateHandle
    //           FileState -> Actual file and can perform the new logic with local copy
    //           ByteStreamStateHandle -> Not actual files seems to be state stored in memory,
    //           so use the old method
    //        */
    //        //            if (remoteFileHandle instanceof FileStateHandle) {
    //        hdfsFilePath = ((FileStateHandle) remoteFileHandle).getFilePath().toString();
    //        // logger.info("is FileStateHandle -> {}", hdfsFilePath);
    //        //            } else if (remoteFileHandle instanceof ByteStreamStateHandle) {
    //        //                // logger.info("is ByteStreamStateHandle -> {}", hdfsFilePath);
    //        //                downloadDataForStateHandle_LEGACY(restoreFilePath, remoteFileHandle,
    //        //     closeableRegistry);
    //        //                return;
    //        //            }
    //
    //        // logger.info("hdfsFile PATH -> {}, local -> {}", hdfsFilePath, localOutputFilePath);
    //
    //        // Init the cat cmd
    //        List<String> concat_cmd =
    //                new ArrayList<String>() {
    //                    {
    //                        add("cat");
    //                    }
    //                };
    //
    //        try {
    //            /** * HDFS FSCK CMD ** */
    //
    //            // Build the hdfs info command to find all the blocks for a given file
    //            String[] hdfsInfoCmd = {"hdfs", "fsck", hdfsFilePath, "-files", "-blocks"};
    //
    //            ProcessBuilder hdfs_pb = new ProcessBuilder(hdfsInfoCmd);
    //            Process hdfs_proc = hdfs_pb.start();
    //
    //            // Wait for the command to complete and check status
    //            CommandExecutionStatus(hdfs_proc.waitFor(), 0);
    //
    //            // Regex that matches the desired input
    //            String regexPattern = ".*(\\d+)\\. (BP-\\w.+).* len=(\\d*) Live_repl=(\\d*).*";
    //            Pattern pattern = Pattern.compile(regexPattern);
    //
    //            BufferedReader hdfs_cmd_output =
    //                    new BufferedReader(new InputStreamReader(hdfs_proc.getInputStream()));
    //
    //            Instant end_time_fsck = Instant.now();
    //            logger.info(
    //                    "HDFS FSCK Duration "
    //                            + Duration.between(start_time, end_time_fsck).toMillis()
    //                            + "\tFILE "
    //                            + hdfsFilePath);
    //
    //            // read the output from the command
    //            String out;
    //
    //            while ((out = hdfs_cmd_output.readLine()) != null) {
    //
    //                if (out.startsWith("Status"))
    //                    break; // We don't need extra information from that point on
    //
    //                Matcher matcher = pattern.matcher(out);
    //
    //                // When the pattern finds a match
    //                if (matcher.find()) {
    //
    //                    // Your regex matched the line, take values from the regex groups
    //                    // String block_num = matcher.group(1); //The number of the block
    //                    String info_loc = matcher.group(2); // The information about location
    //                    // System.out.println("Value 2: " + info_loc);
    //
    //                    String[] split_info = info_loc.split(":");
    //                    String block_pool_id = split_info[0]; // Block Pool Id (used in path)
    //                    String block_id = split_info[1]; // Specific Block
    //
    //                    String block_id_path = block_id.substring(0, block_id.lastIndexOf("_"));
    //                    // System.out.println("Path Info "+ block_pool_id + " Value Info " +
    //                    // block_id_path);
    //
    //                    Instant before_end_time_find = Instant.now();
    //                    logger.info(
    //                            "BEFORE FIND "
    //                                    + Duration.between(start_time,
    // before_end_time_find).toMillis()
    //                                    + "\tFILE "
    //                                    + hdfsFilePath);
    //
    //                    /** * FIND CMD ** */
    //                    // THIS is the goal command. However we don't seem to need *
    //                    // find
    //                    //
    //                    //
    //                    //
    // /tmp/hadoop-fs-tmp/current/BP-798034145-127.0.0.1-1690967214498/current/finalized -name
    //                    //     'blk_1073741832*'
    //                    // String fin_path = "/tmp/hadoop-fs-tmp/current/" + path_info +
    //                    // "/current/finalized -name '" + sub_block_info + "*'";
    //                    String find_search_path =
    //                            "/tmp/hadoop-fs-tmp/current/" + block_pool_id +
    // "/current/finalized";
    //
    //                    String[] find_command = {"find", find_search_path, "-name",
    // block_id_path};
    //                    // logger.info(Arrays.toString(find_command));
    //                    // Execute the command
    //
    //                    ProcessBuilder find_pb = new ProcessBuilder(find_command);
    //                    Process find_proc = find_pb.start();
    //
    //                    // Wait for the command to complete
    //                    CommandExecutionStatus(find_proc.waitFor(), 1);
    //
    //                    BufferedReader find_cmd_output =
    //                            new BufferedReader(new
    // InputStreamReader(find_proc.getInputStream()));
    //
    //                    Instant end_time_find = Instant.now();
    //                    logger.info(
    //                            "FIND "
    //                                    + Duration.between(start_time, end_time_find).toMillis()
    //                                    + "\tFILE "
    //                                    + hdfsFilePath);
    //
    //                    String line = null;
    //                    // We are expecting to find one path only. In case no path Error
    //                    if ((line = find_cmd_output.readLine()) != null) concat_cmd.add(line);
    //                    else
    //                        logger.error(
    //                                "ERROR -> Could not find "
    //                                        + find_search_path
    //                                        + ", with BlockID: "
    //                                        + block_id_path);
    //                }
    //            }
    //
    //            /** * CONCAT CMD ** */
    //            // Finish with the concat process after finding all the blocks
    //            ProcessBuilder concat_pb = new ProcessBuilder(concat_cmd);
    //            concat_pb.redirectOutput(ProcessBuilder.Redirect.to(new
    // File(localOutputFilePath)));
    //            Process concat_proc = concat_pb.start();
    //
    //            Instant end_time_cat = Instant.now();
    //            logger.info(
    //                    "CAT Duration "
    //                            + Duration.between(start_time, end_time_cat).toMillis()
    //                            + "\tFILE "
    //                            + hdfsFilePath);
    //
    //            CommandExecutionStatus(concat_proc.waitFor(), 3);
    //
    //            Instant end_time_full = Instant.now();
    //            logger.info(
    //                    "downloadDataForStateHandle Duration "
    //                            + Duration.between(start_time, end_time_full).toMillis()
    //                            + "\tFILE "
    //                            + hdfsFilePath);
    //
    //        } catch (IOException e) {
    //            e.printStackTrace();
    //        } catch (InterruptedException e) {
    //            throw new RuntimeException(e);
    //        }
    //    }

    public void CommandExecutionStatus(int exitCode, int mode) {
        if (exitCode != 0) logger.error("Failed. Exit code: " + exitCode + ", mode: " + mode);
    }
}

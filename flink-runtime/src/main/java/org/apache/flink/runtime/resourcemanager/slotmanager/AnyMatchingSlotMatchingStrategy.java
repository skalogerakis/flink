/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.resourcemanager.slotmanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.instance.InstanceID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** {@link SlotMatchingStrategy} which picks the first matching slot. */
public enum AnyMatchingSlotMatchingStrategy implements SlotMatchingStrategy {
    INSTANCE;

    private static final Logger LOG =
            LoggerFactory.getLogger(AnyMatchingSlotMatchingStrategy.class);

    // Dictionary to store the total size by ip node
    static Map<String, Long> hdfsStats = new HashMap<>();

    public static List<String> SortedHostingList() {

        // Create a List to store the sorted keys
        List<String> sortedKeys = new ArrayList<>();

        // Sort the HashMap entries by values using a custom Comparator
        List<Map.Entry<String, Long>> entryList = new ArrayList<>(hdfsStats.entrySet());
        entryList.sort(Collections.reverseOrder(Map.Entry.comparingByValue()));

        // Add the keys to the sortedKeys List
        for (Map.Entry<String, Long> entry : entryList) {
            sortedKeys.add(entry.getKey());
            LOG.info("IP KEY " + entry.getKey() + "\tDATA VALUE: " + entry.getValue());
        }

        return sortedKeys;
    }

    @Override
    public <T extends TaskManagerSlotInformation> Optional<T> findMatchingSlot(
            ResourceProfile requestedProfile,
            Collection<T> freeSlots,
            Function<InstanceID, Integer> numberRegisteredSlotsLookup,
            JobID jobID) {

        if (jobID != null) {

            freeSlots.stream().forEach(s -> LOG.info("SLOTIDs CHECK {}", s.getSlotId().toString()));

            // The checkpointed directory uses the checkpoint path + jobId to store its checkpoints
            String path = "hdfs:/flink-checkpoints/" + jobID.toString();

            try {
                // HDFS command to execute
                String[] hdfsInfoCmd = {"hdfs", "fsck", path, "-files", "-blocks", "-locations"};

                ProcessBuilder processBuilder = new ProcessBuilder(hdfsInfoCmd);
                Process process = processBuilder.start();
                BufferedReader reader =
                        new BufferedReader(new InputStreamReader(process.getInputStream()));

                String line;
                while ((line = reader.readLine()) != null) {

                    // Only lines containing DatanodeInfoWithStorage are valuable
                    if (line.contains("DatanodeInfoWithStorage")) {

                        // Regex to get the length and replication factor of a HDFS block
                        String regexLenRep = ".* len=(\\d*) Live_repl=(\\d*).*";
                        Pattern patternLenRep = Pattern.compile(regexLenRep);
                        Matcher matcherLenRep = patternLenRep.matcher(line);

                        if (matcherLenRep.find()) {
                            /** BLOCK SIZE AND REPLICATION FACTOR * */
                            int blockSize = Integer.parseInt(matcherLenRep.group(1));
                            int replicationFactor = Integer.parseInt(matcherLenRep.group(2));
                            // System.out.println("Block Size: " + blockSize + "\tRF: " +
                            // replicationFactor);

                            /*
                               Regex to get all the ips on which a HDFS block is present. In the case of more than one replicas
                               DatanodeInfoWithStorage appears multiple times with the desired IP
                            */

                            String regexIps =
                                    ".*"
                                            + String.format("%0" + replicationFactor + "d", 0)
                                                    .replace(
                                                            "0",
                                                            ".*DatanodeInfoWithStorage\\[([\\w.]*)");

                            Pattern patternIps = Pattern.compile(regexIps);
                            Matcher matcherIps = patternIps.matcher(line);

                            if (matcherIps.find()) {

                                // Add all the replicas to the IP dictionary
                                for (int i = 1; i <= replicationFactor; i++) {
                                    String ip = matcherIps.group(i);

                                    // Calculate per ip size and get the corresponding load
                                    hdfsStats.put(ip, hdfsStats.getOrDefault(ip, 0L) + blockSize);
                                }
                            }
                        }
                    }
                }

                List<String> hostingList = SortedHostingList();
                LOG.info("SK INFO " + hostingList);

                for (int i = 0; i < hostingList.size(); i++) {
                    if (hostingList.get(i).trim().isEmpty()) continue;

                    LOG.info("SK IP: {}", hostingList.get(i));

                    Pattern currIpPattern = Pattern.compile("^(" + hostingList.get(i) + "):(.*)$");

                    try {
                        Optional<T> result =
                                freeSlots.stream()
                                        .filter(
                                                p ->
                                                        currIpPattern
                                                                .matcher(p.getSlotId().toString())
                                                                .find())
                                        .filter(
                                                slot ->
                                                        slot.isMatchingRequirement(
                                                                requestedProfile))
                                        .findAny();

                        if (result.isPresent()) {
                            return result;
                        } else {
                            throw new NoSuchElementException();
                        }

                    } catch (NoSuchElementException e) {
                        LOG.info("THE IP {} NOT FOUND IN THE RESOURCES", hostingList.get(i));
                    }
                }

            } catch (IOException e) {
                LOG.info("ERROR OCCURED");
            }
        }

        LOG.info("SANITY CHECK DEFAULT BEHAVIOR.");

        // Default execution plan
        return freeSlots.stream()
                .filter(slot -> slot.isMatchingRequirement(requestedProfile))
                .findAny();
    }

    //    @Override
    //    public <T extends TaskManagerSlotInformation> Optional<T> findMatchingSlot(
    //            ResourceProfile requestedProfile,
    //            Collection<T> freeSlots,
    //            Function<InstanceID, Integer> numberRegisteredSlotsLookup) {
    //
    //        LOG.info("SANITY CHECK FIRST MATCHING SLOT. ");
    //        //        freeSlots.stream().forEach(s -> LOG.info("EEEE TST {}", s));
    //        //
    //        //        freeSlots.stream().forEach(s -> LOG.info("SLOT IDS TST {}", s.getSlotId()));
    //
    //        freeSlots.stream().forEach(s -> LOG.info("SLOTIDs CHECK {}",
    // s.getSlotId().toString()));
    //
    //        //        LOG.info("HARDCODED");
    //        //        String right = "127.0.1.1";
    //        //        String tester = "198.0.1.1";
    //        //        // Compile regex as predicate
    //        //        Pattern pattern = Pattern.compile("^(" + right + "):(.*)$");
    //        //
    //        //        // THIS WORKS AS EXPECTED!! TODO CHECK FIND FIRST FOR THE FINAL IMPL
    //        //        freeSlots.stream()
    //        //                .filter(p -> pattern.matcher(p.getSlotId().toString()).find())
    //        //                .forEach(s -> LOG.info("YOOO REG {}", s));
    //        //                        .collect(Collectors.<String>toList());
    //
    //        //        desiredIps.forEach(s -> LOG.info("YOOO REG {}", s));
    //
    //        //        LOG.info("REAL DEAL");
    //        //
    //        //        List<String> ips = Arrays.asList("198.0.1.1", "127.0.1.1", "199.0.1.1");
    //
    //        // TODO CREATE A LIST OF IPS to see what to do and how to parse them next
    //
    //        //        for (int i = 0; i < ips.size(); i++) {
    //        //            LOG.info("FOR THE IP {}", ips.get(i));
    //        //
    //        //            Pattern currIpPattern = Pattern.compile("^(" + ips.get(i) + "):(.*)$");
    //        //
    //        //            try {
    //        //                freeSlots.stream()
    //        //                        .filter(p ->
    //        // currIpPattern.matcher(p.getSlotId().toString()).find())
    //        //                        .findFirst()
    //        //                        .ifPresent(s -> LOG.info("YOOO TESTER {}", s));
    //        //
    //        //            } catch (NoSuchElementException e) {
    //        //                LOG.info("THE IP DOES NOT EXIST {}", ips.get(i));
    //        //                continue;
    //        //            }
    //        //        }
    //        //
    //        //        LOG.info("REAL DEAL VOL 2");
    //        //
    //        //        List<String> ips3 = Arrays.asList("198.0.1.1", "127.0.1.1", "199.0.1.1");
    //        //
    //        //        // TODO CREATE A LIST OF IPS to see what to do and how to parse them next
    //        //
    //        //        for (int i = 0; i < ips3.size(); i++) {
    //        //            LOG.info("FOR THE IP V2 {}", ips3.get(i));
    //        //
    //        //            Pattern currIpPattern3 = Pattern.compile("^(" + ips3.get(i) +
    // "):(.*)$");
    //        //
    //        //            try {
    //        //                freeSlots.stream()
    //        //                        .filter(p ->
    //        // currIpPattern3.matcher(p.getSlotId().toString()).find())
    //        //                        .findAny()
    //        //                        //                        .ifPresent(s -> LOG.info("YOOO
    // REAL
    //        // DEALS {}", s))
    //        //                        .orElseThrow(() -> new NoSuchElementException());
    //        //
    //        //                LOG.info("FOR THE IP found something {}", ips3.get(i));
    //        //                break;
    //        //            } catch (NoSuchElementException e) {
    //        //                LOG.info("THE IP DOES NOT EXIST EXCEPTION {}", ips3.get(i));
    //        //                continue;
    //        //            }
    //        //        }
    //
    //        //        String localityDirURI =
    //        // ReadableConfig.get(CheckpointingOptions.JOBMANAGER_LOCALITY);
    //        //        ReadableConfig configuration = new Configuration();
    //        //        String homeDir = System.getenv("HOME");
    //        //
    //        //        LOG.info("HOME PATH CHECK {}", homeDir);
    //        //        LOG.info("FLINK PATH CHECK {}", System.getenv("FLINK_HOME"));
    //        //        LOG.info("FLINK CONF CHECK {}", System.getenv("FLINK_CONF_DIR"));
    //
    //        //        String path = CheckpointingOptions.JOBMANAGER_LOCALITY.key();
    //        //        String path = configuration.get(CheckpointingOptions.JOBMANAGER_LOCALITY);
    //        //        String path2 = CheckpointingOptions.JOBMANAGER_LOCALITY;
    //
    //        //        LOG.info("FIRST PATH CHECK {}", path);
    //        //        LOG.info("FIRST PATH CHECK {}", abstr_test.getCheckpointPath());
    //        //        LOG.info("FIRST PATH2 CHECK {}", path2);
    //
    //        String homeDir = System.getenv("HOME");
    //
    //        String fileDir = homeDir + "/jobLocality.txt";
    //        LOG.info("HOME PATH CHECK {}", homeDir);
    //        LOG.info("FILE PATH CHECK {}", fileDir);
    //
    //        try {
    //            List<String> ipList =
    //                    Files.readAllLines(new File(fileDir).toPath(), Charset.defaultCharset());
    //            //            int elementCounter = 0;
    //            System.out.println("SIZE " + ipList.size());
    //            for (int i = 0; i < ipList.size(); i++) {
    //                if (ipList.get(i).trim().isEmpty()) continue;
    //
    //                LOG.info("IP: {}", ipList.get(i));
    //
    //                Pattern currIpPattern = Pattern.compile("^(" + ipList.get(i) + "):(.*)$");
    //
    //                try {
    //                    Optional<T> result =
    //                            freeSlots.stream()
    //                                    .filter(
    //                                            p ->
    //                                                    currIpPattern
    //                                                            .matcher(p.getSlotId().toString())
    //                                                            .find())
    //                                    .filter(slot ->
    // slot.isMatchingRequirement(requestedProfile))
    //                                    .findAny();
    //
    //                    if (result.isPresent()) {
    //                        return result;
    //                    } else {
    //                        throw new NoSuchElementException();
    //                    }
    //
    //                } catch (NoSuchElementException e) {
    //                    LOG.info("THE IP {} NOT FOUND IN THE RESOURCES", ipList.get(i));
    //                    continue;
    //                }
    //            }
    //        } catch (IOException e) {
    //
    //            LOG.info("NO FILE WITH PATH {} FOUND. PROCEED WITH NORMAL EXECUTION", fileDir);
    //        }
    //
    //        // Default execution plan
    //        return freeSlots.stream()
    //                .filter(slot -> slot.isMatchingRequirement(requestedProfile))
    //                .findAny();
    //    }
}

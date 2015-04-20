/**
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
package com.pinterest.secor.uploader;

import com.pinterest.secor.common.*;
import com.pinterest.secor.io.FileReader;
import com.pinterest.secor.io.FileWriter;
import com.pinterest.secor.io.KeyValue;
import com.pinterest.secor.util.CompressionUtil;
import com.pinterest.secor.util.FileUtil;
import com.pinterest.secor.util.IdUtil;
import com.pinterest.secor.util.ReflectionUtil;

import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.minidev.json.JSONObject;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Uploader applies a set of policies to determine if any of the locally stored files should be
 * uploaded to s3.
 *
 * @author Pawel Garbacki (pawel@pinterest.com)
 */
public class Uploader {
    private static final Logger LOG = LoggerFactory.getLogger(Uploader.class);

    private static final ExecutorService executor = Executors.newFixedThreadPool(256);

    private SecorConfig mConfig;
    private OffsetTracker mOffsetTracker;
    private FileRegistry mFileRegistry;
    private ZookeeperConnector mZookeeperConnector;
    private String s3TopicDirSuffix = null;
    private TreeMap<String, JSONObject> archivedHours = new TreeMap<String, JSONObject>();
    private Set<String> archivedTopics = new HashSet<String>();


    public Uploader(SecorConfig config, OffsetTracker offsetTracker, FileRegistry fileRegistry) {
        this(config, offsetTracker, fileRegistry, new ZookeeperConnector(config));
        Object suffixDir = mConfig.getS3TopicDirSuffix();
        if (suffixDir != null) {
            s3TopicDirSuffix = suffixDir.toString();
        }
    }

    // For testing use only.
    public Uploader(SecorConfig config, OffsetTracker offsetTracker, FileRegistry fileRegistry,
                    ZookeeperConnector zookeeperConnector) {
        mConfig = config;
        mOffsetTracker = offsetTracker;
        mFileRegistry = fileRegistry;
        mZookeeperConnector = zookeeperConnector;

        Object suffixDir = mConfig.getS3TopicDirSuffix();
        if (suffixDir != null) {
            s3TopicDirSuffix = suffixDir.toString();
        }
    }

    private Future<?> upload(LogFilePath localPath, TopicPartition topicPartition) throws Exception {
        String s3Prefix = "s3n://" + mConfig.getS3Bucket() + "/" + mConfig.getS3Path();
        String topicValue;

        if (s3TopicDirSuffix != null) {
            if (localPath.getTopic().equals("tagdata")) {
                topicValue = "pageview" + s3TopicDirSuffix;
            } else {
                topicValue = localPath.getTopic() + s3TopicDirSuffix;
            }
        } else {
            topicValue = localPath.getTopic();
        }

        LogFilePath s3Path = new LogFilePath(s3Prefix, topicValue,
                                             localPath.getPartitions(),
                                             localPath.getGeneration(),
                                             localPath.getKafkaPartition(),
                                             localPath.getOffset(),
                                             localPath.getExtension());
        final String localLogFilename = localPath.getLogFilePath();
        final String s3LogFilename = s3Path.getLogFilePath();

        ArrayList<String> elements = new ArrayList<String>();
        for (String partition : s3Path.getPartitions()) {
            elements.add(partition);
        }
        String hoursTouched = StringUtils.join(elements, "").replaceAll("/", "");

        LOG.info("uploading file " + localLogFilename + " to " + s3LogFilename);
        LOG.info("uploading file, hours_touched=" + hoursTouched + " partition=" + topicPartition.getPartition());
        String updateHours = mConfig.getUpdateHoursArchived();
        if ( updateHours != null && updateHours.equals("true")) {
            JSONObject jsonObj = new JSONObject();
            jsonObj.put("hours_touched", hoursTouched);
            jsonObj.put("sts", System.currentTimeMillis()/1000);
            jsonObj.put("topics",  topicValue);
            jsonObj.put("partition",  topicPartition.getPartition());
            jsonObj.put("filepath",  localPath.getLogBasenameWithExt());
            archivedHours.put(hoursTouched, jsonObj);
        }
        return executor.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    FileUtil.moveToS3(localLogFilename, s3LogFilename);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }

    private void uploadFiles(TopicPartition topicPartition) throws Exception {
        long committedOffsetCount = mOffsetTracker.getTrueCommittedOffsetCount(topicPartition);
        long lastSeenOffset = mOffsetTracker.getLastSeenOffset(topicPartition);
        final String lockPath = "/secor/locks/" + topicPartition.getTopic() + "/" +
                                topicPartition.getPartition();
        // Deleting writers closes their streams flushing all pending data to the disk.
        mFileRegistry.deleteWriters(topicPartition);
        mZookeeperConnector.lock(lockPath);
        try {
            // Check if the committed offset has changed.
            long zookeeperComittedOffsetCount = mZookeeperConnector.getCommittedOffsetCount(
                    topicPartition);
            if (zookeeperComittedOffsetCount == committedOffsetCount) {
                LOG.info("uploading topic " + topicPartition.getTopic() + " partition " +
                         topicPartition.getPartition());
                Collection<LogFilePath> paths = mFileRegistry.getPaths(topicPartition);
                List<Future<?>> uploadFutures = new ArrayList<Future<?>>();
                for (LogFilePath path : paths) {
                    uploadFutures.add(upload(path, topicPartition));
                }
                for (Future<?> uploadFuture : uploadFutures) {
                    uploadFuture.get();
                }
                mFileRegistry.deleteTopicPartition(topicPartition);
                mZookeeperConnector.setCommittedOffsetCount(topicPartition, lastSeenOffset + 1);
                mOffsetTracker.setCommittedOffsetCount(topicPartition, lastSeenOffset + 1);
            }
        } finally {
            mZookeeperConnector.unlock(lockPath);
        }
    }

    /**
     * This method is intended to be overwritten in tests.
     * @throws Exception
     */
    protected FileReader createReader(LogFilePath srcPath, CompressionCodec codec) throws Exception {
        return ReflectionUtil.createFileReader(
                mConfig.getFileReaderWriterFactory(),
                srcPath,
                codec
        );
    }

    private void trim(LogFilePath srcPath, long startOffset) throws Exception {
        if (startOffset == srcPath.getOffset()) {
            return;
        }
        FileReader reader = null;
        FileWriter writer = null;
        LogFilePath dstPath = null;
        int copiedMessages = 0;
        // Deleting the writer closes its stream flushing all pending data to the disk.
        mFileRegistry.deleteWriter(srcPath);
        try {
            CompressionCodec codec = null;
            String extension = "";
            if (mConfig.getCompressionCodec() != null && !mConfig.getCompressionCodec().isEmpty()) {
                codec = CompressionUtil.createCompressionCodec(mConfig.getCompressionCodec());
                extension = codec.getDefaultExtension();
            }
            reader = createReader(srcPath, codec);
            KeyValue keyVal;
            while ((keyVal = reader.next()) != null) {
                if (keyVal.getKey() >= startOffset) {
                    if (writer == null) {
                        String localPrefix = mConfig.getLocalPath() + '/' +
                            IdUtil.getLocalMessageDir();
                        dstPath = new LogFilePath(localPrefix, srcPath.getTopic(),
                                                  srcPath.getPartitions(), srcPath.getGeneration(),
                                                  srcPath.getKafkaPartition(), startOffset,
                                                  extension);
                        writer = mFileRegistry.getOrCreateWriter(dstPath,
                        		codec);
                    }
                    writer.write(keyVal);
                    copiedMessages++;
                }
            }
        } finally {
            if (reader != null) {
                reader.close();
            }
        }
        mFileRegistry.deletePath(srcPath);
        if (dstPath == null) {
            LOG.info("removed file " + srcPath.getLogFilePath());
        } else {
            LOG.info("trimmed " + copiedMessages + " messages from " + srcPath.getLogFilePath() + " to " +
                    dstPath.getLogFilePath() + " with start offset " + startOffset);
        }
    }

    private void trimFiles(TopicPartition topicPartition, long startOffset) throws Exception {
        Collection<LogFilePath> paths = mFileRegistry.getPaths(topicPartition);
        for (LogFilePath path : paths) {
            trim(path, startOffset);
        }
    }

    private void checkTopicPartition(TopicPartition topicPartition) throws Exception {
        final long size = mFileRegistry.getSize(topicPartition);
        final long modificationAgeSec = mFileRegistry.getModificationAgeSec(topicPartition);
        if (size >= mConfig.getMaxFileSizeBytes() ||
                modificationAgeSec >= mConfig.getMaxFileAgeSeconds()) {
            long newOffsetCount = mZookeeperConnector.getCommittedOffsetCount(topicPartition);
            long oldOffsetCount = mOffsetTracker.setCommittedOffsetCount(topicPartition,
                    newOffsetCount);
            long lastSeenOffset = mOffsetTracker.getLastSeenOffset(topicPartition);
            if (oldOffsetCount == newOffsetCount) {
                uploadFiles(topicPartition);
            } else if (newOffsetCount > lastSeenOffset) {  // && oldOffset < newOffset
                LOG.debug("last seen offset " + lastSeenOffset +
                          " is lower than committed offset count " + newOffsetCount +
                          ".  Deleting files in topic " + topicPartition.getTopic() +
                          " partition " + topicPartition.getPartition());
                // There was a rebalancing event and someone committed an offset beyond that of the
                // current message.  We need to delete the local file.
                mFileRegistry.deleteTopicPartition(topicPartition);
            } else {  // oldOffsetCount < newOffsetCount <= lastSeenOffset
                LOG.debug("previous committed offset count " + oldOffsetCount +
                          " is lower than committed offset " + newOffsetCount +
                          " is lower than or equal to last seen offset " + lastSeenOffset +
                          ".  Trimming files in topic " + topicPartition.getTopic() +
                          " partition " + topicPartition.getPartition());
                // There was a rebalancing event and someone committed an offset lower than that
                // of the current message.  We need to trim local files.
                trimFiles(topicPartition, newOffsetCount);
            }
        }
    }

    public void applyPolicy() throws Exception {
        archivedHours.clear();
        archivedTopics.clear();
        Collection<TopicPartition> topicPartitions = mFileRegistry.getTopicPartitions();
        for (TopicPartition topicPartition : topicPartitions) {
            checkTopicPartition(topicPartition);
        }
        // TODO: 1# archivedtopics should not be a hashset, it should just be a string of the topicname,
        // since each secor consumer is only going to consume one kafka topic.
        if (!archivedHours.isEmpty()) {
            KafkaProducer mKafkaProducer = new KafkaProducer(mConfig);
            for (String hour_entry : archivedHours.keySet()) {
                JSONObject jsonObj = archivedHours.get(hour_entry);
                String msg = jsonObj.toString();
                LOG.info("applyPolicy sending evt " + msg);
                mKafkaProducer.sendMessage(msg);
            }
        }

    }
}

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

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.error.DocumentAlreadyExistsException;
import com.couchbase.client.java.error.DocumentDoesNotExistException;
import com.couchbase.client.java.error.TemporaryFailureException;
import com.pinterest.secor.common.FileRegistry;
import com.pinterest.secor.common.KafkaProducer;
import com.pinterest.secor.common.LogFilePath;
import com.pinterest.secor.common.OffsetTracker;
import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.common.TopicPartition;
import com.pinterest.secor.common.ZookeeperConnector;
import com.pinterest.secor.io.FileReader;
import com.pinterest.secor.io.FileWriter;
import com.pinterest.secor.io.KeyValue;
import com.pinterest.secor.util.CompressionUtil;
import com.pinterest.secor.util.CouchbaseUtil;
import com.pinterest.secor.util.FileUtil;
import com.pinterest.secor.util.IdUtil;
import com.pinterest.secor.util.ReflectionUtil;

import net.minidev.json.JSONObject;
import net.minidev.json.JSONValue;
import rx.Observable;
import rx.functions.Func1;

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

    private Future<?> upload(final LogFilePath localPath, TopicPartition topicPartition) throws Exception {
        String s3Prefix = "s3n://" + mConfig.getS3Bucket() + "/" + mConfig.getS3Path();
        String topicValue;

        if (s3TopicDirSuffix != null) {
            if (localPath.getTopic().equals("tagdata")) {
                topicValue = "pageview" + s3TopicDirSuffix;
            } else if (localPath.getTopic().equals("aggstats-river")) {
                topicValue = "es-river-aggstats" + s3TopicDirSuffix;
            } else {
                topicValue = localPath.getTopic() + s3TopicDirSuffix;
            }
        } else {
            topicValue = localPath.getTopic();
        }

        String s3BackupPrefix = "s3n://" + mConfig.getS3Bucket() + "/lumpyspace-backup/" + mConfig.getS3Path();
        LogFilePath s3PathBackup = new LogFilePath(s3BackupPrefix, topicValue,
                                                   localPath.getPartitions(),
                                                   localPath.getGeneration(),
                                                   localPath.getKafkaPartition(),
                                                   localPath.getOffset(),
                                                   localPath.getExtension());
        final String s3BackupLogFilename = s3PathBackup.getLogFilePath();

        LogFilePath s3Path = new LogFilePath(s3Prefix, topicValue,
                                             localPath.getPartitions(),
                                             localPath.getGeneration(),
                                             localPath.getKafkaPartition(),
                                             localPath.getOffset(),
                                             localPath.getExtension());
        final String s3LogFilename = s3Path.getLogFilePath();

        ArrayList<String> elements = new ArrayList<String>();
        for (String partition : s3Path.getPartitions()) {
            elements.add(partition);
        }
        String hoursTouched = StringUtils.join(elements, "").replaceAll("/", "");
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
                    final String localLogFilename = getLocalFileToUpload(localPath);
                    LOG.info("uploading perm location file " + localLogFilename + " to " + s3LogFilename);
                    LOG.info("uploading backup file " + localLogFilename + " to " + s3BackupLogFilename);
                    FileUtil.copyToS3(localLogFilename, s3BackupLogFilename);
                    FileUtil.moveToS3(localLogFilename, s3LogFilename);
                    //                    FileUtil.copyWithinS3(s3LogFilename, s3BackupLogFilename);
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

    private FileWriter createWriter(LogFilePath path, CompressionCodec codec) throws Exception {
        return ReflectionUtil.createFileWriter(
                mConfig.getFileReaderWriterFactory(),
                path,
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
                LOG.info("last seen offset " + lastSeenOffset +
                          " is lower than committed offset count " + newOffsetCount +
                          ".  Deleting files in topic " + topicPartition.getTopic() +
                          " partition " + topicPartition.getPartition());
                // There was a rebalancing event and someone committed an offset beyond that of the
                // current message.  We need to delete the local file.
                mFileRegistry.deleteTopicPartition(topicPartition);
            } else {  // oldOffsetCount < newOffsetCount <= lastSeenOffset
                LOG.info("previous committed offset count " + oldOffsetCount +
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

    private String getLocalFileToUpload(LogFilePath srcPath) {
    	String dedupBucket = mConfig.getCouchbaseDedupBucketname();
    	if ( (dedupBucket == null) || (dedupBucket.isEmpty()) ) {
    		return srcPath.getLogFilePath(); //no deduping required, just return original file
    	}

    	try {
        	return getDedupedFile(srcPath);
    	}catch (Exception ex) {
    		throw new RuntimeException(ex);
    	}
    }

    private String getDedupedFile(LogFilePath srcPath) throws Exception {
    	LOG.info("getting deduped file for " + srcPath.getLogFilePath());
		FileReader reader = null;
        FileWriter writer = null;
        DedupFilter dedupFilter = new DedupFilter(mConfig);
        LogFilePath dedupPath = null;
        int rawCount = 0, dedupedCount = 0;
        try {
            CompressionCodec codec = null;
            if (mConfig.getCompressionCodec() != null && !mConfig.getCompressionCodec().isEmpty()) {
                codec = CompressionUtil.createCompressionCodec(mConfig.getCompressionCodec());
            }
            reader = createReader(srcPath, codec);
            KeyValue keyVal;
            while ((keyVal = reader.next()) != null) {
                if (writer == null) {
                  String localPrefix = mConfig.getLocalPath() + '/' + 
            		IdUtil.getLocalMessageDir();
                    	String[] srcPartitions = srcPath.getPartitions();
                        String[] dedupPartitions = new String[srcPartitions.length + 1];
                        for (int i = 0; i < srcPartitions.length; ++i) {
                        	dedupPartitions[i]  = srcPartitions[i];
                        }
                        dedupPartitions[dedupPartitions.length-1] = "dedup";
                        dedupPath = new LogFilePath(localPrefix, srcPath.getTopic(),
                        		dedupPartitions, srcPath.getGeneration(),
                                srcPath.getKafkaPartition(), srcPath.getOffset(),
                                "_dedup"+srcPath.getExtension());
                    writer = createWriter(dedupPath,codec);
                }
            	// (filter, batch and) get messages to write, if any, at this time
            	Collection<KeyValue> keysToWrite = dedupFilter.getMessagesToWrite(keyVal);
            	for (KeyValue kv : keysToWrite) {
            		writer.write(kv);
            	}
            	dedupedCount += keysToWrite.size();
                rawCount++;
            }
            // flush any remaining messages
            int retryCounter = 1;
            while (!dedupFilter.batch.isEmpty()) {
            	if (++retryCounter % 50 == 0) {
            		LOG.error("Unable to flush " + dedupFilter.batch.size() +" messages after " + retryCounter + " trys");
            	}
            	Collection<KeyValue> keysToWrite = dedupFilter.flushMessages();
            	for (KeyValue kv : keysToWrite) {
            		writer.write(kv);
            	}
            	dedupedCount += keysToWrite.size();
            }
        } finally {
            if (reader != null) {
                reader.close();
            }
            if (writer != null) {
            	writer.close();
            }
        }
    	LOG.info("DedupFilter : rawCount " + rawCount + " dedupCount " + dedupedCount + " : " + dedupPath.getLogFilePath());
        return (dedupPath == null ? null : dedupPath.getLogFilePath());
	}

	/**
	 * Tracks batching and deduping. Event ids for the last 24-hours are tracked in couchbase.
	 * DedupFilter batches the events and uses CB bulk operations to check/insert the eventIDs.
	 * The IDs that are not currently present in CB are inserted and passed through to be written to the logs.
	 * Any duplicate IDs that are already present are filtered out.
	 */
    public class DedupFilter {
		private SecorConfig config;
		private CouchbaseHelper cbHelper;	
	    private Map<String, KeyValue> batch;
		private int count;

    	public DedupFilter(SecorConfig config) {
			this.config = config;
			cbHelper = new CouchbaseHelper(config);
    		batch = new HashMap<String, KeyValue>();
    		count = 0;
    	}
    
		private String getTopic() {
			return config.getKafkaTopicFilter();
		}
		
		private int getBatchSize() {
			return config.getCouchbaseBatchSize();
		}

		/**
         * track the events for batching, 
         * if it is time to batch-process, filter duplicates and return new events to be written
         * @return messages to write
         */
    	public Collection<KeyValue> getMessagesToWrite(KeyValue newKeyValue) {
    		String id = getId(newKeyValue);
    		if ( (id == null) || (batch.containsKey(id)) ) {
    			// not a valid key or key already being tracked for further check of duplicates
    			return Collections.emptyList(); 
    		}
    		if (id.getBytes().length > 250) {
    			LOG.error("charset : " + Charset.defaultCharset() + "length : " + id.getBytes().length + " : " + id);
    			return Collections.emptyList(); 
    		}
    		// key not currently tracked for further check, track it so it can be batched for bulk-get/bulk-insert in couchbase
    		batch.put(id, newKeyValue); 
    		if (++count < getBatchSize()) {
    			return Collections.emptyList(); // not time to batch get/insert yet
    		}
    		
 			return flushMessages(); // time to batch-process, filter duplicates and return new events to be written
    	}
    	
		/**
         * filter those messages that exist in CB
         * insert the new messages into CB, and return so they can be written
         * @return messages to write
         */
    	public Collection<KeyValue> flushMessages() { 
    		// time to batch!
    		List<KeyValue> keyvaluesToWrite = new ArrayList<KeyValue>();
    		Set<String> ids = batch.keySet();
    		
    		try {
        		// Filter out ids already in couchbase. Although the next step uses insert, not upsert
        		// and duplicates would be filtered out, a very large number of duplicates increases chances  
        		// of potential deadlocks. Pre-filter to minimize duplicates.
        		Set<String> found = cbHelper.multiGet(ids);
    			for (String foundId : found) {
    				// log number of duplicates and a sample..for any cross-verification...
    				KeyValue kvDuplicate = batch.get(foundId);
    	            JSONObject jsonObject = (JSONObject) JSONValue.parse(kvDuplicate.getValue());
    				LOG.info("Duplicates : " + found.size() + " : ri : " + foundId + ", sts : "+ jsonObject.get("sts"));
    				break;
    			}
        		ids.removeAll(found);
        		
        		// multi-insert (with insert not upsert, this will ensure that even with multiple threads,
        		// events are processed only once)
        		List<Set<String>> results = cbHelper.multiInsert(ids); 
        		Set<String> insertedIds = results.get(0);
        		Set<String> toRetry = results.get(1);
        		
        		// messages corresponding to the newly inserted IDs, should be written to the log
    			for ( Entry<String, KeyValue> entry : batch.entrySet()) {
    				if (insertedIds.contains(entry.getKey())){
    					keyvaluesToWrite.add(entry.getValue());
    				}
        		}
    			
    			// start a new batch...if there are any to retry, retain those 
    			if (toRetry.isEmpty()) {
    				count = 0;
    	    		batch.clear();
    			} else {
    				count = toRetry.size();
    				HashMap<String, KeyValue> newMap = new HashMap<String, KeyValue>();
    				for (String retryId : toRetry) {
    					newMap.put(retryId, batch.get(retryId));
    				}
    	    		batch = newMap;
    			}
                try {
    				Thread.sleep(100);
    			} catch (InterruptedException e) {}
    		} catch (Exception ex) {
    			LOG.error("Exception while accessing couchbase : " + ex.getMessage());
    			LOG.info(ex + " : " + ex.getStackTrace());
    		}
			// these are new events ... write them!
			return keyvaluesToWrite;
    	}
        
    	/**
    	 * construct the event id to be inserted into CB
    	 * @param kv
    	 * @return id
    	 */
    	private String getId(KeyValue kv) {
            JSONObject jsonObject = (JSONObject) JSONValue.parse(kv.getValue());
            if (jsonObject == null) {
                return null;
            }
            
            // id mappings reflect es-mapbox. all events are in their own bucket.
            String topic = getTopic().toLowerCase();
    		if ("tagdata".equals(topic)) {
                Object pviValue = jsonObject.get("pvi");
                if (pviValue == null) {
                	LOG.warn("invalid pageview with null pvi : " + jsonObject.toString());
                }
    			return jsonObject.get("pvi").toString(); 
    		}
            Object riValue = jsonObject.get("ri");
            if (riValue == null) {
            	LOG.warn("invalid " + topic + " with null ri : " + jsonObject.toString());
            }
            if ("adimpression".equals(topic) ||
                "dsadimpression".equals(topic) ||
                "adserved".equals(topic) || 
                "dsadserved".equals(topic) ||
                "swipe".equals(topic) ) {
            	return riValue.toString();
            }
            Object stsValue = jsonObject.get("sts");
        	return riValue.toString() + "_" + stsValue.toString();
    	}
	}
	
	/**
	 * For couchbase bulk operations.
	 * Implements a multiInsert(..) method which 
	 * 		- will insert new IDs and 
	 * 		- will drop/filter out the IDs already in CB
	 * When using multiInsert(..) there is no need for a separate multiGet call to check for IDs that 
	 * already exist (majority of the time, all IDs would be new, duplicates are expected infrequently).
	 */
    public class CouchbaseHelper {
		private static final int EXPIRRY = 24 * 60 * 60; // 24 hour expiration
	    private Bucket bucket;

    	public CouchbaseHelper(SecorConfig config) {
    		bucket = CouchbaseUtil.getBucket(config.getCouchbaseDedupBucketname());
		}

    	/** Attempts to bulk insert the passed in set of IDs. 
    	 * Returns the list of IDs successfully inserted and the list of IDs to retry
    	 * An id will either be
    	 * 		- inserted, if not already present in couchbase
    	 * 		- ignored/filtered, if already present in couchbase
    	 * 		- marked for retry later, if an error occurs during insert
    	 * @return list of ids successfully inserted & list of ids to retry
    	 */
		public List<Set<String>> multiInsert(Set<String> ids) {
			final Set<String> inserted = new HashSet<String>();
			final Set<String> toRetry = new HashSet<String>();
			final Set<String> duplicates = new HashSet<String>();

			List<JsonDocument> documents = new ArrayList<JsonDocument>();
			for (String id : ids) {
			    documents.add(JsonDocument.create(id, EXPIRRY, null));
			}
			// Insert them in one batch, waiting until the last one is done.
			Iterable<JsonDocument> iter = Observable
		    .from(documents)
		    .flatMap(new Func1<JsonDocument, Observable<JsonDocument>>() {
		        @Override
		        public Observable<JsonDocument> call(final JsonDocument docToInsert) {
		            Observable<JsonDocument> observableDoc = 
		            		bucket.async().insert(docToInsert)
		            		.timeout(60000, TimeUnit.MILLISECONDS)
		            	    .onErrorReturn(new Func1<Throwable, JsonDocument>() {
		            	        @Override
		            	        public JsonDocument call(final Throwable t) {
		            	        	if (t instanceof DocumentAlreadyExistsException) {
		            	        		duplicates.add(docToInsert.id());
		            	        	} else if (t instanceof Exception){
		            	        		toRetry.add(docToInsert.id());
		            	        		if ( ! (t instanceof TemporaryFailureException) ) {
		            	        			LOG.warn("Dedup insert error : " + docToInsert.id() + " : " + t.getClass() + " : " + t.getMessage());
		            	        		}
		            	        	}
		            	            // simply return null on error..we will retry all errors except DocumentAlreadyExistsException
		            	            return null;
		            	        }});
		            return observableDoc;
		        }
		    })
		    .toBlocking()
		    .toIterable();
			for ( JsonDocument doc : iter) {
				if (doc != null) {
					inserted.add(doc.id());
				}
			}
			if ( (!duplicates.isEmpty()) || (!toRetry.isEmpty()) ){
				LOG.info("dedup multi-insert : ids " + ids.size() + ", duplicates "+ duplicates.size() + ", to retry " + toRetry.size() + ", inserted "+ inserted.size());
			}
			List<Set<String>> ret = new ArrayList<Set<String>>(2);
			ret.add(inserted); // new IDs...inserted
			ret.add(toRetry);
			return ret; 
		}

		public Set<String> multiGet(Set<String> ids) {
			final Set<String> found = new HashSet<String>();

			// Insert them in one batch, waiting until the last one is done.
			Iterable<JsonDocument> iter = Observable
		    .from(ids)
		    .flatMap(new Func1<String, Observable<JsonDocument>>() {
		        @Override
		        public Observable<JsonDocument> call(final String idToGet) {
		            Observable<JsonDocument> observableDoc = 
		            		bucket.async().get(idToGet)
		            		.timeout(60000, TimeUnit.MILLISECONDS)
		            	    .onErrorReturn(new Func1<Throwable, JsonDocument>() {
		            	        @Override
		            	        public JsonDocument call(final Throwable t) {
		            	        	if ( (t instanceof Exception) && (! (t instanceof DocumentDoesNotExistException)) ) {
		            	        		LOG.warn("Dedup get error : " + idToGet + " : " + t.getClass() + " : " + t.getMessage());
		            	        	}
		            	            // simply return null on error
		            	            return null;
		            	        }});
		            return observableDoc;
		        }
		    })
		    .toBlocking()
		    .toIterable();
			for ( JsonDocument doc : iter) {
				if (doc != null) {
					found.add(doc.id());
				}
			}
			return found; 
		}
    }
}

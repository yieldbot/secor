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
package com.pinterest.secor.util;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.pinterest.secor.common.SecorConfig;

/**
 * CouchbaseUtil manages (using couchbase-client java sdk) couchbase cluster and bucket resources 
 * shared across consumer threads.
 * Always create only one instance of a CouchbaseCluster and share it across threads (same with buckets).
 * CouchbaseCluster instance is able to open many Buckets while sharing the underlying resources very efficiently.  
 * The SDK is thread-safe, so no additional synchronization is needed when interacting with the SDK 
 *
 * @author atumkur (atumkur@yieldbot.com)
 */
public class CouchbaseUtil {
    private static final Logger LOG = LoggerFactory.getLogger(CouchbaseUtil.class);

	private static CouchbaseCluster mCouchbaseCluster = null; // @see class documentation
    private static Map<String, Bucket> mBucketMap; // the 9 event types being tracked, each have their own bucket

    synchronized public static void configure(SecorConfig config) {
    	if (mCouchbaseCluster == null) {
    		List<String> hosts = Arrays.asList(config.getCouchbaseHosts());
    		if (hosts == null || hosts.isEmpty()) {
    			return; // no couchbase functionality 
    		}
    		LOG.info("couchbase hosts : " + Arrays.toString(config.getCouchbaseHosts()));
    		mCouchbaseCluster = CouchbaseCluster.create(hosts);
    		mBucketMap = new HashMap<String, Bucket>();
    	}
    }

    synchronized public static Bucket getBucket(String bucketName) {
    	if (mCouchbaseCluster == null) {
    		return null; 
    	}
    	if (mBucketMap.containsKey(bucketName)) {
    		return mBucketMap.get(bucketName);
    	}
		LOG.info("opening bucket : " + bucketName + " : " + mBucketMap.size());
    	Bucket bucket = null;
    	while (bucket == null) {
    		try {
            	bucket = mCouchbaseCluster.openBucket(bucketName, 20, TimeUnit.SECONDS);
            	mBucketMap.put(bucketName, bucket);
    		} catch (Exception ex) {
    			LOG.info("retry opening bucket : " + bucketName + " : " + ex + " : " + ex.getMessage());
                try {
    				Thread.sleep(5000);
    			} catch (InterruptedException e) {}
    		}
    	}
    	return bucket;
    }
}

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
package com.pinterest.secor.parser;

import java.text.SimpleDateFormat;
import java.util.TimeZone;
import java.util.Date;

import net.minidev.json.JSONObject;
import net.minidev.json.JSONValue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.message.Message;
import com.pinterest.secor.message.ParsedMessage;


/**
 * CustomDateMessageParser extracts timestamp field (specified by 'message.timestamp.name')
 * and creates custom output dirs based on a custom formatter, defined by defaultFormatter
 * the output dir will look like s3://<bucket>/<topic>/yyyy/MM/dd/HH/<file_name>
 *
 * @see http://docs.oracle.com/javase/6/docs/api/java/text/SimpleDateFormat.html
 *
 * @author Kunal Nawale (nkunal@gmail.com)
 *
 */
public class CustomDateMessageParser extends TimestampedMessageParser {
    private static final Logger LOG = LoggerFactory.getLogger(CustomDateMessageParser.class);
    protected static final String defaultFormatter = "yyyy/MM/dd/HH";

    public CustomDateMessageParser(SecorConfig config) {
        super(config);
    }

        @Override
    public long extractTimestampMillis(final Message message) {
        JSONObject jsonObject = (JSONObject) JSONValue.parse(message.getPayload());
        if (jsonObject != null) {
            Object fieldValue = jsonObject.get(mConfig.getMessageTimestampName());
            if (fieldValue != null) {
                return toMillis(Double.valueOf(fieldValue.toString()).longValue());
            }
        }
        return 0;
    }

        @Override
    public String[] extractPartitions(Message message) throws Exception {
        // Date constructor takes milliseconds since epoch.
        long timestampMillis = extractTimestampMillis(message);
        Date date = new Date(timestampMillis);
        SimpleDateFormat outputFormatter = new SimpleDateFormat(defaultFormatter);
        outputFormatter.setTimeZone(TimeZone.getTimeZone("Etc/UTC"));
        String result[] = {outputFormatter.format(date)};
        return result;
    }

    public ParsedMessage parse(Message message) throws Exception {
        if (message.getTopic().equals("tagdata")) {
            JSONObject jsonObject = (JSONObject) JSONValue.parse(message.getPayload());
            if (jsonObject != null) {
                Object fieldValue = jsonObject.get("ua");
                if (fieldValue != null) {
                    if (fieldValue.toString().equals("yieldbot-internal-crawler/0.0.1")){
                        LOG.debug("dropping internal crawler event ua=" + fieldValue.toString());
                        return null;
                    }
                }
            }
        }
        return super.parse(message);
    }
}

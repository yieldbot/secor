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
 * PerfMetricsDateParser extracts timestamp field (specified by 'message.timestamp.name')
 * and creates custom output dirs based on a custom formatter, defined by defaultFormatter
 * the output dir will look like s3://<bucket>/<topic>/yyyy/MM/dd/HH/<file_name>
 *
 * @see http://docs.oracle.com/javase/6/docs/api/java/text/SimpleDateFormat.html
 *
 *
 *
 */
public class PerfMetricsDateParser extends MessageParser {
    private static final Logger LOG = LoggerFactory.getLogger(PerfMetricsDateParser.class);
    protected static final String defaultFormatter = "yyyy/MM/dd/HH";

    public PerfMetricsDateParser(SecorConfig config) {
        super(config);
    }

    @Override
    public String[] extractPartitions(Message message) {
        String defaultResult[] = {"1970/01/01/00"};
        JSONObject jsonObject = (JSONObject) JSONValue.parse(message.getPayload());
        if (jsonObject != null) {
            Object fieldValue;
            Object typeValue = jsonObject.get("type");
            if (typeValue.toString().contains("-counts")) {
                fieldValue = jsonObject.get("id");
            } else {
                JSONObject sourceValue = (JSONObject) jsonObject.get("source");
                JSONObject dimensionsValue = (JSONObject) sourceValue.get("dimensions");
                fieldValue = dimensionsValue.get("date");
            }
            Object inputPattern = mConfig.getMessageTimestampInputPattern();
            if (fieldValue != null && inputPattern != null) {
                try {
                    SimpleDateFormat inputFormatter = new SimpleDateFormat(inputPattern.toString());
                    SimpleDateFormat outputFormatter = new SimpleDateFormat(defaultFormatter);
                    Date dateFormat = inputFormatter.parse(fieldValue.toString());
                    String result[] = {outputFormatter.format(dateFormat)};
                    return result;
                    }
                catch (Exception e) {
                    LOG.warn("Impossible to convert date = " + fieldValue.toString()
                            + " for the input pattern = " + inputPattern.toString()
                             + "Exception = " + e);
                }
            }
        }
        return defaultResult;
    }
}

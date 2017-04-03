/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.siddhi.utils;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.contrib.siddhi.operator.KeyedSiddhiOperator;
import org.apache.flink.contrib.siddhi.operator.SiddhiOperatorContext;
import org.apache.flink.contrib.siddhi.operator.SiddhiStreamOperator;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Convert SiddhiCEPExecutionPlan to SiddhiCEP Operator and build output DataStream
 */
public class SiddhiStreamFactory {
	protected static final Logger LOGGER = LoggerFactory.getLogger(SiddhiStreamFactory.class);

	@SuppressWarnings("unchecked")
	public static <IN, OUT> DataStream<OUT> createDataStream(SiddhiOperatorContext context, DataStream<Tuple2<String, IN>> namedStream) {
		if (namedStream instanceof KeyedStream) {
			LOGGER.info("Calling createKeyedDataStream");
			return createKeyedDataStream(context, namedStream);
		} else {
			LOGGER.info("createDataStream non-keyed");
			return namedStream.transform(context.getName(), context.getOutputStreamType(), new SiddhiStreamOperator(context));
		}
	}

	@SuppressWarnings("unchecked")
	public static <KEY, IN, OUT> DataStream<OUT> createKeyedDataStream(SiddhiOperatorContext context, DataStream<Tuple2<String, IN>> namedStream) {
		KeyedStream<Tuple2<String, IN>, KEY> keyedStream = (KeyedStream<Tuple2<String, IN>, KEY>) namedStream;
		final TypeSerializer<Tuple2<String, IN>> inputSerializer = keyedStream.getType().createSerializer(keyedStream.getExecutionConfig());
		KeySelector<Tuple2<String, IN>, KEY> keySelector = keyedStream.getKeySelector();
		TypeSerializer<KEY> keySerializer = keyedStream.getKeyType().createSerializer(keyedStream.getExecutionConfig());
		return keyedStream.transform(context.getName(), context.getOutputStreamType(),
			new KeyedSiddhiOperator(context, inputSerializer, keySelector, keySerializer));
	}
}

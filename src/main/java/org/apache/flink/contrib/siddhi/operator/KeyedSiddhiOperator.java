package org.apache.flink.contrib.siddhi.operator;

import com.arcticwolf.flink.operators.EventTimeOrderKeyedOperator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.contrib.siddhi.utils.EmittedTimestampTracker;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;

import java.util.HashMap;
import java.util.Map;

public class KeyedSiddhiOperator<IN, KEY, OUT>
	extends EventTimeOrderKeyedOperator<Tuple2<String, IN>, KEY, OUT> {

	protected static final Logger LOGGER = LoggerFactory.getLogger(KeyedSiddhiOperator.class);

	private final SiddhiOperatorContext siddhiContext;
	private final String executionPlan;

	private transient SiddhiManager siddhiManager;
	private transient EmittedTimestampTracker emittedTimestampTracker;
	private transient Map<KEY, ExecutionPlanRuntime> runtimeMap;
	private transient Map<KEY, Map<String, InputHandler>> inputStreamHandlers;

	public KeyedSiddhiOperator(SiddhiOperatorContext siddhiContext,
				TypeSerializer<Tuple2<String, IN>> inputSerializer,
				KeySelector<Tuple2<String, IN>, KEY> keySelector,
				TypeSerializer<KEY> keySerializer) {
		super(inputSerializer, keySelector, keySerializer);
		this.siddhiContext = siddhiContext;
		this.executionPlan = siddhiContext.getFinalExecutionPlan();
	}

	@Override
	public void open() throws Exception {
		super.open();
		initializeTransientState();
	}

	private void initializeTransientState() {
		if (runtimeMap == null) {
			runtimeMap = new HashMap<>();
		}
		if (inputStreamHandlers == null) {
			inputStreamHandlers = new HashMap<>();
		}
		if (emittedTimestampTracker == null) {
			emittedTimestampTracker = new EmittedTimestampTracker();
		}

		siddhiManager = siddhiContext.createSiddhiManager();
		for (Map.Entry<String, Class<?>> entry : this.siddhiContext.getExtensions().entrySet()) {
			siddhiManager.setExtension(entry.getKey(), entry.getValue());
		}
	}

	@Override
	public void close() throws Exception {
		// Try to wait for Siddhi to finish emitting all results
		while (true) {
			long lastEmittedTimestamp = emittedTimestampTracker.getTimestamp();
			Thread.sleep(2000);
			if (lastEmittedTimestamp == emittedTimestampTracker.getTimestamp()) {
				break;
			}
		}
		super.close();

		runtimeMap.forEach((key, runtime) -> runtime.shutdown());
		siddhiManager.shutdown();
	}

	@Override
	public void processObservation(StreamRecord<Tuple2<String, IN>> record) throws Exception {
		// Start runtime for this key if not already done
		instantiateExecutionPlanRuntime();

		String streamId = record.getValue().f0;
		IN observation = record.getValue().f1;
		getSiddhiInputHandler(streamId).send(record.getTimestamp(),
			siddhiContext.getInputStreamSchema(streamId).getStreamSerializer().getRow(observation));
	}

	@Override
	public void advanceTime(long timestamp) {
	}

	@Override
	public void snapshotState(FSDataOutputStream out, long checkpointId, long timestamp) throws Exception {
		super.snapshotState(out, checkpointId, timestamp);

		DataOutputView ov = new DataOutputViewStreamWrapper(out);
		ov.writeInt(runtimeMap.size());
		for (Map.Entry<KEY, ExecutionPlanRuntime> entry : runtimeMap.entrySet()) {
			keySerializer.serialize(entry.getKey(), ov);
			byte[] snapshot = entry.getValue().snapshot();
			ov.writeInt(snapshot.length);
			ov.write(snapshot);
		}
	}

	@Override
	public void restoreState(FSDataInputStream state) throws Exception {
		super.restoreState(state);

		initializeTransientState();
		DataInputView inputView = new DataInputViewStreamWrapper(state);
		int numberEntries = inputView.readInt();
		for (int i = 0; i <numberEntries; i++) {
			KEY key = keySerializer.deserialize(inputView);
			setCurrentKey(key);
			int snapshotLength = inputView.readInt();
			byte[] snapshot = new byte[snapshotLength];
			inputView.read(snapshot);
			instantiateExecutionPlanRuntime().restore(snapshot);
		}
	}

	public InputHandler getSiddhiInputHandler(String streamId) {
		return inputStreamHandlers.get(getCurrentKey()).get(streamId);
	}

	private ExecutionPlanRuntime instantiateExecutionPlanRuntime() {
		KEY key = (KEY) getCurrentKey();

		ExecutionPlanRuntime runtime = runtimeMap.get(key);
		if (runtime != null) {
			return runtime;
		}

		runtime = siddhiManager.createExecutionPlanRuntime(executionPlan);
		AbstractDefinition definition = runtime.getStreamDefinitionMap().get(siddhiContext.getOutputStreamId());
		runtime.addCallback(siddhiContext.getOutputStreamId(),
			new StreamOutputHandler<>(siddhiContext.getOutputStreamType(),
				definition,
				this.output,
				emittedTimestampTracker));

		Map<String, InputHandler> perKeyInputStreamHandlers = inputStreamHandlers.get(key);
		if (perKeyInputStreamHandlers == null) {
			perKeyInputStreamHandlers = new HashMap<>();
			inputStreamHandlers.put(key, perKeyInputStreamHandlers);
		}
		for (String inputStreamId : siddhiContext.getInputStreams()) {
			perKeyInputStreamHandlers.put(inputStreamId, runtime.getInputHandler(inputStreamId));
		}

		runtime.start();
		runtimeMap.put(key, runtime);
		return runtime;
	}
}

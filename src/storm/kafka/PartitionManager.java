package storm.kafka;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import kafka.api.FetchRequestBuilder;
import kafka.api.OffsetRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.storm.guava.collect.ImmutableMap;

import storm.kafka.KafkaSpout.EmitState;
import storm.kafka.KafkaSpout.MessageAndRealOffset;
import storm.kafka.trident.KafkaUtils;
import storm.kafka.trident.MaxMetric;
import backtype.storm.Config;
import backtype.storm.metric.api.CombinedMetric;
import backtype.storm.metric.api.CountMetric;
import backtype.storm.metric.api.MeanReducer;
import backtype.storm.metric.api.ReducedMetric;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.utils.Utils;

public class PartitionManager {
	public static final Log LOG = LogFactory.getLog(PartitionManager.class);

	private final CombinedMetric _fetchAPILatencyMax;
	private final ReducedMetric _fetchAPILatencyMean;
	private final CountMetric _fetchAPICallCount;
	private final CountMetric _fetchAPIMessageCount;

	static class KafkaMessageId {
		public Partition partition;
		public long offset;

		public KafkaMessageId(Partition partition, long offset) {
			this.partition = partition;
			this.offset = offset;
		}
	}

	Long _emittedToOffset;
	SortedSet<Long> _pending = new TreeSet<Long>();
	Long _committedTo;
	public LinkedList<MessageAndRealOffset> _waitingToEmit = new LinkedList<MessageAndRealOffset>();
	Partition _partition;
	SpoutConfig _spoutConfig;
	String _topologyInstanceId;
	SimpleConsumer _consumer;
	DynamicPartitionConnections _connections;
	ZkState _state;
	Map _stormConf;
	String partitionZkNOde;

	public PartitionManager(DynamicPartitionConnections connections, String topologyInstanceId, ZkState state,
			Map stormConf, SpoutConfig spoutConfig, Partition id) {
		_partition = id;
		_connections = connections;
		_spoutConfig = spoutConfig;
		_topologyInstanceId = topologyInstanceId;
		_consumer = connections.register(id.host, id.partition);
		_state = state;
		_stormConf = stormConf;

		String jsonTopologyId = null;
		Long jsonOffset = null;
		try {
			partitionZkNOde = committedPath();
			Map<Object, Object> json = _state.readJSON(partitionZkNOde);
			if (json != null) {
				jsonTopologyId = (String) ((Map<Object, Object>) json.get("topology")).get("id");
				jsonOffset = (Long) json.get("offset");
			}
		} catch (Throwable e) {
			LOG.warn("Error reading and/or parsing at ZkNode: " + committedPath(), e);
		}

		if (!topologyInstanceId.equals(jsonTopologyId) && spoutConfig.forceFromStart) {
			_committedTo = KafkaUtils
					.getOffset(_consumer, spoutConfig.topic, id.partition, spoutConfig.startOffsetTime);
			LOG.info("Using startOffsetTime to choose last commit offset.");
		} else if (jsonTopologyId == null || jsonOffset == null) { // failed to
																	// parse
																	// JSON?
			_committedTo = KafkaUtils.getOffset(_consumer, spoutConfig.topic, id.partition,
					kafka.api.OffsetRequest.LatestTime());
			LOG.info("Setting last commit offset to HEAD.");
		} else {
			_committedTo = jsonOffset;
			LOG.info("Read last commit offset from zookeeper: " + _committedTo);
		}

		LOG.info("Starting Kafka " + _consumer.host() + ":" + id.partition + " from offset " + _committedTo);
		_emittedToOffset = _committedTo;

		_fetchAPILatencyMax = new CombinedMetric(new MaxMetric());
		_fetchAPILatencyMean = new ReducedMetric(new MeanReducer());
		_fetchAPICallCount = new CountMetric();
		_fetchAPIMessageCount = new CountMetric();
	}

	public Map getMetricsDataMap() {
		Map ret = new HashMap();
		ret.put(_partition + "/fetchAPILatencyMax", _fetchAPILatencyMax.getValueAndReset());
		ret.put(_partition + "/fetchAPILatencyMean", _fetchAPILatencyMean.getValueAndReset());
		ret.put(_partition + "/fetchAPICallCount", _fetchAPICallCount.getValueAndReset());
		ret.put(_partition + "/fetchAPIMessageCount", _fetchAPIMessageCount.getValueAndReset());
		return ret;
	}

	// returns false if it's reached the end of current batch
	public EmitState next(SpoutOutputCollector collector) {
		if (_waitingToEmit.isEmpty())
			fill();
		while (true) {
			MessageAndRealOffset toEmit = _waitingToEmit.pollFirst();
			if (toEmit == null) {
				return EmitState.NO_EMITTED;
			}
			Iterable<List<Object>> tups = _spoutConfig.scheme.deserialize(Utils.toByteArray(toEmit.msg.payload()));
			if (tups != null) {
				for (List<Object> tup : tups) {
					if (LOG.isDebugEnabled())
						LOG.debug("spout msg  " + _spoutConfig.streamId + ":" + tup);
					collector.emit(_spoutConfig.streamId, tup, new KafkaMessageId(_partition, toEmit.offset));
				}
				break;
			} else {
				ack(toEmit.offset);
			}
		}
		if (!_waitingToEmit.isEmpty()) {
			return EmitState.EMITTED_MORE_LEFT;
		} else {
			return EmitState.EMITTED_END;
		}
	}

	private void fill() {
		// LOG.info("Fetching from Kafka: " + _consumer.host() + ":" +
		// _partition.partition + " from offset " + _emittedToOffset);
		long start = System.nanoTime();
		ByteBufferMessageSet msgs = _consumer.fetch(
				new FetchRequestBuilder().addFetch(_spoutConfig.topic, _partition.partition, _emittedToOffset,
						_spoutConfig.fetchSizeBytes).build()).messageSet(_spoutConfig.topic, _partition.partition);
		long end = System.nanoTime();
		long millis = (end - start) / 1000000;
		_fetchAPILatencyMax.update(millis);
		_fetchAPILatencyMean.update(millis);
		_fetchAPICallCount.incr();
		int numMessages = countMessages(msgs);
		_fetchAPIMessageCount.incrBy(numMessages);

		if (numMessages > 0) {
			LOG.debug("Fetched " + numMessages + " messages from Kafka: " + _consumer.host() + ":"
					+ _partition.partition);
		}
		for (MessageAndOffset msg : msgs) {
			_pending.add(_emittedToOffset);
			_waitingToEmit.add(new MessageAndRealOffset(msg.message(), _emittedToOffset));
			_emittedToOffset = msg.nextOffset();
		}
		if (numMessages > 0) {
			LOG.debug("Added " + numMessages + " messages from Kafka: " + _consumer.host() + ":" + _partition.partition
					+ " to internal buffers");
		}
	}

	private int countMessages(ByteBufferMessageSet messageSet) {
		int counter = 0;
		for (MessageAndOffset messageAndOffset : messageSet) {
			counter = counter + 1;
		}
		return counter;
	}

	public void ack(Long offset) {
		_pending.remove(offset);
	}

	public void fail(Long offset) {
		// TODO: should it use in-memory ack set to skip anything that's been
		// acked but not committed???
		// things might get crazy with lots of timeouts
		if (_emittedToOffset > offset) {
			_emittedToOffset = offset;
			_pending.tailSet(offset).clear();
		}
	}

	public void commit() {
		LOG.debug("Committing offset for " + _partition);
		long committedTo;
		if (_pending.isEmpty()) {
			committedTo = _emittedToOffset;
		} else {
			committedTo = _pending.first();
		}
		if (committedTo != _committedTo) {
			LOG.debug("Writing committed offset to ZK: " + committedTo);

			Map<Object, Object> data = (Map<Object, Object>) ImmutableMap
					.builder()
					.put("topology",
							ImmutableMap.of("id", _topologyInstanceId, "name", _stormConf.get(Config.TOPOLOGY_NAME)))
					.put("offset", committedTo).put("partition", _partition.partition)
					.put("broker", ImmutableMap.of("host", _partition.host.host, "port", _partition.host.port))
					.put("topic", _spoutConfig.topic).build();
			_state.writeJSON(partitionZkNOde, data);

			LOG.debug("Wrote committed offset to ZK: " + committedTo);
			_committedTo = committedTo;
		}
		LOG.debug("Committed offset " + committedTo + " for " + _partition);
	}

	private String committedPath() {
		return _spoutConfig.zkRoot + "/" + _spoutConfig.clientId + "/" + _partition;
	}

	public long queryPartitionOffsetLatestTime() {
		return KafkaUtils.getOffset(_consumer, _spoutConfig.topic, _partition.partition, OffsetRequest.LatestTime());
	}

	public long lastCommittedOffset() {
		return _committedTo;
	}

	public long lastCompletedOffset() {
		if (_pending.isEmpty()) {
			return _emittedToOffset;
		} else {
			return _pending.first();
		}
	}

	public Partition getPartition() {
		return _partition;
	}

	public void close() {
		_connections.unregister(_partition.host, _partition.partition);
	}
}

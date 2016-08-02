package storm.kafka;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import storm.kafka.trident.GlobalPartitionInformation;
import backtype.storm.task.IMetricsContext;
import backtype.storm.utils.Utils;

public class ZkCoordinator implements PartitionCoordinator {
	public static final Log LOG = LogFactory.getLog(ZkCoordinator.class);

	SpoutConfig _spoutConfig;
	int _taskIndex;
	int _totalTasks;
	String _topologyInstanceId;
	Map<Partition, PartitionManager> _managers = new HashMap<Partition, PartitionManager>();
	List<PartitionManager> _cachedList;
	Long _lastRefreshTime = null;
	int _refreshFreqMs;
	DynamicPartitionConnections _connections;
	DynamicBrokersReader _reader;
	ZkState _state;
	Map _stormConf;
	IMetricsContext _metricsContext;

	public ZkCoordinator(DynamicPartitionConnections connections, Map stormConf, SpoutConfig spoutConfig, ZkState state, int taskIndex,
			int totalTasks, String topologyInstanceId) {
		_spoutConfig = spoutConfig;
		_connections = connections;
		_taskIndex = taskIndex;
		_totalTasks = totalTasks;
		_topologyInstanceId = topologyInstanceId;
		_stormConf = stormConf;
		_state = state;

		ZkHosts brokerConf = (ZkHosts) spoutConfig.hosts;
		_refreshFreqMs = brokerConf.refreshFreqSecs * 1000;
		_reader = new DynamicBrokersReader(stormConf, brokerConf.brokerZkStr, brokerConf.brokerZkPath, spoutConfig.topic);

	}

	@Override
	public List<PartitionManager> getMyManagedPartitions() {
		if (_lastRefreshTime == null || (System.currentTimeMillis() - _lastRefreshTime) > _refreshFreqMs) {
			long now = System.currentTimeMillis();
			while (!_spoutConfig.isPause) {
				try {
					refresh();
					break;
				} catch (Exception e) {
					if (System.currentTimeMillis() - now > 60000) {
						if (e instanceof RuntimeException) {
							throw (RuntimeException) e;
						} else {
							throw new RuntimeException(e);
						}
					}
					LOG.error("刷新Kafka消息[" + _spoutConfig.topic + "]管理分区错误，睡眠2秒重试", e);
					Utils.sleep(2000);

				}
			}
			_lastRefreshTime = System.currentTimeMillis();
		}
		return _cachedList;
	}

	void refresh() {
		try {
			LOG.debug("Refreshing partition manager connections");
			GlobalPartitionInformation brokerInfo = _reader.getBrokerInfo();
			Set<Partition> mine = new HashSet<Partition>();
			for (Partition partitionId : brokerInfo) {
				if (myOwnership(partitionId)) {
					mine.add(partitionId);
				}
			}
			Set<Partition> curr = _managers.keySet();
			Set<Partition> newPartitions = new HashSet<Partition>(mine);
			newPartitions.removeAll(curr);

			Set<Partition> deletedPartitions = new HashSet<Partition>(curr);
			deletedPartitions.removeAll(mine);

			LOG.debug("Deleted partition managers: " + deletedPartitions.toString());

			for (Partition id : deletedPartitions) {
				PartitionManager man = _managers.remove(id);
				man.close();
			}
			LOG.debug("New partition managers: " + newPartitions.toString());

			for (Partition id : newPartitions) {
				PartitionManager man = new PartitionManager(_connections, _topologyInstanceId, _state, _stormConf, _spoutConfig, id);
				_managers.put(id, man);
			}

		} catch (Exception e) {
			if (e instanceof RuntimeException) {
				throw (RuntimeException) e;
			} else {
				throw new RuntimeException(e);
			}
		}
		_cachedList = new ArrayList<PartitionManager>(_managers.values());
		LOG.debug("Finished refreshing");
	}

	@Override
	public PartitionManager getManager(Partition partition) {
		return _managers.get(partition);
	}

	private boolean myOwnership(Partition id) {
		int val = Math.abs(id.host.hashCode() + 23 * id.partition);
		return val % _totalTasks == _taskIndex;
	}
}

package com.ery.estorm.client.store;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import backtype.storm.utils.Utils;

import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.Expression;
import com.ery.estorm.client.node.StormNodeConfig;
import com.ery.estorm.client.node.BoltNode.BufferRow;
import com.ery.estorm.config.EStormConstant;
import com.ery.estorm.daemon.topology.NodeInfo.DataStorePO;
import com.ery.estorm.daemon.topology.NodeInfo.InOutPO;
import com.ery.estorm.daemon.topology.NodeInfo.NodePO;
import com.ery.estorm.exceptions.PushException;
import com.ery.estorm.util.HasThread;
import com.ery.estorm.util.Threads;

public class StoreManage extends HasThread {
	private static final Log LOG = LogFactory.getLog(StoreManage.class);
	// 根据节点ID存储缓存数据
	static HashMap<Long, HashMap<String, BufferData>> dataBuffers = new HashMap<Long, HashMap<String, BufferData>>();
	// 存储推送目标
	static HashMap<Long, StoreConfig> batchPushs = new HashMap<Long, StoreConfig>();
	// 缓存数据推送次数 0：已Push目标数 1：需要Push的目标数
	static HashMap<Long, HashMap<String, int[]>> bufDataPushCounts = new HashMap<Long, HashMap<String, int[]>>();
	int retryTimes = 3;

	static class BufferData {
		public BufferData(Object[] data, long currentTimeMillis) {
			this.data = data;
			this.time = currentTimeMillis;
		}

		Object[] data;
		long time;
	}

	static class StoreSize {
		public NodePO npo;
		public Map<Long, Boolean> stored = new HashMap<Long, Boolean>();

		StoreSize(NodePO npo) {
			this.npo = npo;
		}
	}

	static class StoreConfig {
		long lastPutTime = System.currentTimeMillis();
		public NodePO npo;
		public DataStorePO dspo;
		public StoreConnection conn;
		LinkedList<String> bufs = new LinkedList<String>();// 每个PUSHPO的缓存列表
		public HashMap<String, BufferData> bufferData;
		public HashMap<String, int[]> bufferPushCounts;
		boolean isRunning = false;
		long errorTime = 0;
		Expression termExp = null;

		StoreConfig(NodePO npo, DataStorePO ppo) {
			this.npo = npo;
			this.dspo = ppo;
		}

		void close() {
			if (conn != null)
				conn.close();
		}

		boolean pushData() throws PushException {
			if (conn == null) {
				conn = StoreConnection.createConnection(this);
				if (conn == null)
					return false;
			}
			if (!conn.isConnected) {
				conn.open();
			}
			Map<String, Integer> _bufs = new HashMap<String, Integer>();
			try {
				while (true) {
					String key = null;
					synchronized (bufs) {
						if (bufs.size() > 0) {
							key = bufs.pollFirst();
							_bufs.put(key, 0);
						}
					}
					if (key == null)
						break;
					conn.putData(key);
				}
				conn.commit();
				// 提交成功，判断是否还有需要推送的
				for (String key : _bufs.keySet()) {
					synchronized (bufferPushCounts) {
						int[] pushSize = bufferPushCounts.get(key);
						if (pushSize[0] == pushSize[1]) {
							bufDataPushCounts.remove(key);
							bufferData.remove(key);
						} else {
							pushSize[0]++;
						}
					}
				}
			} catch (PushException e) {
				synchronized (bufs) {
					bufs.addAll(0, _bufs.keySet());
				}
				throw e;
			}
			return true;
		}

		byte inited = 0;

		void init() {
			if (inited > 0)
				return;
			try {
				bufferData = dataBuffers.get(npo.NODE_ID);
				bufferPushCounts = bufDataPushCounts.get(npo.NODE_ID);
				inited = 2;
				if (this.dspo.STORE_EXPR != null && !this.dspo.STORE_EXPR.equals("")) {// 编译计算表达式
					termExp = AviatorEvaluator.compile(this.dspo.STORE_EXPR, true);
					this.dspo.colMacs = termExp.getVariableNames().toArray(new String[0]);
					if (this.dspo.colMacs.length > 0) {
						this.dspo.termIndex = new short[this.dspo.colMacs.length];
						for (int i = 0; i < this.dspo.colMacs.length; i++) {
							Integer index = this.npo.fieldNameRel.get(this.dspo.colMacs[i]);
							this.dspo.termIndex[i] = index.shortValue();
						}
					}
				}
				inited = 1;
			} catch (Exception e) {
			}
		}
	}

	static Map<String, Object> calcEnv = new HashMap<String, Object>();

	public static void add(NodePO npo, BufferRow data) {
		synchronized (dataBuffers) {
			HashMap<String, int[]> cbufs = bufDataPushCounts.get(npo.NODE_ID);
			if (cbufs == null) {
				cbufs = new HashMap<String, int[]>();
				bufDataPushCounts.put(npo.NODE_ID, cbufs);
			}
			cbufs.put(data.key, new int[] { 0, 0 });
			HashMap<String, BufferData> bufs = dataBuffers.get(npo.NODE_ID);
			if (bufs == null) {
				bufs = new HashMap<String, BufferData>();
				dataBuffers.put(npo.NODE_ID, bufs);
			}
			bufs.put(data.key, new BufferData(data.data, System.currentTimeMillis()));
		}
		for (InOutPO _push : npo.storeInfos) {
			DataStorePO dso = (DataStorePO) _push;
			StoreConfig pc = batchPushs.get(dso.STORE_ID);
			if (pc != null) {
				if (pc.dspo.HashCode == dso.HashCode) {
					pc.dspo.AUTO_REAL_FLUSH = dso.AUTO_REAL_FLUSH;
					pc.dspo.FLUSH_BUFFER_SIZE = dso.FLUSH_BUFFER_SIZE;
					pc.dspo.FLUSH_TIME_INTERVAL = dso.FLUSH_TIME_INTERVAL;
				} else {
					pc.dspo = dso;
					pc.close();
					pc.inited = 0;
					pc.init();
				}
			} else {
				pc = new StoreConfig(npo, dso);
				pc.init();
				synchronized (batchPushs) {
					batchPushs.put(dso.STORE_ID, pc);
				}
			}
			if (pc.inited != 1) {
				if (LOG.isDebugEnabled())
					LOG.debug("存储规则[" + pc.dspo.STORE_ID + "]初始化失败，不存储数据：" + data);
				continue;
			}
			if (pc.termExp != null) {
				if (pc.dspo.termIndex != null) {
					calcEnv.clear();
					for (int i = 0; i < pc.dspo.termIndex.length; i++) {
						short index = pc.dspo.termIndex[i];
						Object obj = EStormConstant.castAviatorObject(data.data[index].toString()); // 需要做类型转换
						calcEnv.put(pc.dspo.colMacs[i], obj);
					}
				}
				try {
					Object obj = pc.termExp.execute(calcEnv);
					if (!EStormConstant.getAviatorBoolean(obj)) {
						if (LOG.isDebugEnabled())
							LOG.debug("数据不满足存储规则[" + pc.dspo.STORE_ID + "]过滤条件，不存储数据：" + data);
						return;
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			synchronized (pc.bufs) {
				pc.bufs.add(data.key);// 添加到缓存中
				pc.bufferPushCounts.get(data.key)[1]++;
			}
		}
	}

	private java.util.concurrent.ExecutorService threadPoolExecutorService;

	public StoreManage() {
		int maxThreads = StormNodeConfig.conf.getInt(EStormConstant.STORM_MSG_STORE_THREAD_THREADS, 10);
		retryTimes = StormNodeConfig.conf.getInt(EStormConstant.STORM_MSG_STORE_RETRY_TIMES, 3);
		threadPoolExecutorService = Threads.getBoundedCachedThreadPool(maxThreads, 60L, TimeUnit.SECONDS,
				Threads.newDaemonThreadFactory("Push."));
	}

	@Override
	public void run() {
		Thread.currentThread().setName("StoreMag");
		long lastClearTime = System.currentTimeMillis();
		while (true) {
			long now = System.currentTimeMillis();
			try {
				for (Map.Entry<Long, StoreConfig> _pc : batchPushs.entrySet()) {
					StoreConfig pc = _pc.getValue();
					int blen = pc.bufs.size();
					synchronized (pc) {
						if (pc.errorTime != 0 && now - pc.errorTime < 600000) {// 有错误十分钟内不重试
							continue;
						}
						if (blen > 0 && (now - pc.lastPutTime >= pc.dspo.FLUSH_TIME_INTERVAL || blen > pc.dspo.FLUSH_BUFFER_SIZE)) {
							if (!pc.isRunning) {
								pc.lastPutTime = now;
								pc.isRunning = true;
								threadPoolExecutorService.execute(new StoreThread(this, pc));
							}
						}
					}
				}
			} catch (Exception e) {
				LOG.error("push error", e);
			} finally {
				Utils.sleep(1000);
			}
			if (now - lastClearTime > 180000) {
				for (Long nodeId : dataBuffers.keySet()) {
					HashMap<String, BufferData> bufData = dataBuffers.get(nodeId);
					for (Map.Entry<String, BufferData> key : bufData.entrySet()) {
						if (now - key.getValue().time > 180000) {
							bufData.remove(key.getKey());
						}
					}
				}
			}
		}
	}

	// StormNodeConfig.

	public static class StoreThread extends Thread {
		private static final Log LOG = LogFactory.getLog(StoreThread.class);
		StoreConfig pc;
		StoreManage pm = null;

		StoreThread(StoreManage pm, StoreConfig pc) {
			this.pm = pm;
			this.pc = pc;
		}

		public void run() {
			Thread.currentThread().setName("StoreThread");
			int retry = 0;
			while (retry < pm.retryTimes) {
				try {
					retry++;
					if (pc.pushData()) {
						break;
					}
				} catch (Exception e) {
					LOG.error("推送订阅消息发生错误", e);
					Utils.sleep(1000);
				} finally {
				}
			}
			if (retry >= pm.retryTimes) {// 失败
				pc.errorTime = System.currentTimeMillis();
			}
			pc.isRunning = false;
		}
	}
}

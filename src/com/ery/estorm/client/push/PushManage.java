package com.ery.estorm.client.push;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import backtype.storm.utils.Utils;

import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.Expression;
import com.ery.estorm.client.node.BoltNode.BufferRow;
import com.ery.estorm.client.node.StormNodeConfig;
import com.ery.estorm.config.EStormConstant;
import com.ery.estorm.daemon.topology.NodeInfo.InOutPO;
import com.ery.estorm.daemon.topology.NodeInfo.NodePO;
import com.ery.estorm.daemon.topology.NodeInfo.PushPO;
import com.ery.estorm.exceptions.PushException;
import com.ery.estorm.util.HasThread;
import com.ery.estorm.util.Threads;

public class PushManage extends HasThread {
	private static final Log LOG = LogFactory.getLog(PushManage.class);
	// 根据节点ID存储缓存数据
	static HashMap<Long, HashMap<String, BufferData>> dataBuffers = new HashMap<Long, HashMap<String, BufferData>>();
	// 存储推送目标
	static HashMap<Long, PushConfig> batchPushs = new HashMap<Long, PushConfig>();
	// 缓存数据推送次数 0：全量已Push目标数 1：增量记录数已Push目标数 2:需要Push的目标数
	static HashMap<Long, HashMap<String, short[]>> bufDataPushCounts = new HashMap<Long, HashMap<String, short[]>>();
	int retryTimes = 3;

	public static class BufferData {
		public BufferData(Object[] data, Object[] incData, long currentTimeMillis) {
			this.data = data;
			this.incData = incData;
			this.time = currentTimeMillis;
		}

		public Object[] data = null;
		public Object[] incData = null;
		long time;
	}

	static class PushConfig {
		long lastPushTime = System.currentTimeMillis();
		public NodePO npo;
		public PushPO ppo;
		PushConnection conn;
		LinkedList<String> bufs = new LinkedList<String>();// 每个PUSHPO的缓存列表
		public HashMap<String, BufferData> bufferData;
		public HashMap<String, short[]> bufferPushCounts;
		boolean isRunning = false;
		long errorTime = 0;
		Expression termExp = null;
		LinkedList<String> buf = null;
		public Object pushMetx = new Object();

		PushConfig(NodePO npo, PushPO ppo) {
			this.npo = npo;
			this.ppo = ppo;
		}

		void close() {
			if (conn != null)
				conn.close();
			conn = null;
		}

		boolean pushBegin() {
			long l = System.currentTimeMillis();
			while (true) {
				if (conn == null) {
					conn = PushConnection.createConnection(this);
				}
				if (conn != null) {
					if (!conn.isConnected) {
						try {
							conn.open();
							return true;
						} catch (Exception e) {
							this.inited = 2;
						}
					} else {
						return true;
					}
				}
				if (conn == null) {
					Utils.sleep(1000);
					if (System.currentTimeMillis() - l > 120000) {
						return false;
					}
					continue;
				}
			}
		}

		boolean pushData() throws PushException {
			buf = bufs;
			try {
				synchronized (bufs) {// 减少拷贝
					bufs = new LinkedList<String>();
				}
				conn.putData(buf);
				conn.commit();

			} catch (PushException e) {
				synchronized (bufs) {
					if (bufs.size() == 0) {
						bufs = buf;
					} else if (bufs.size() < buf.size()) {
						buf.addAll(0, bufs);
						bufs = buf;
					} else {
						bufs.addAll(0, buf);
					}
				}
				throw e;
			}
			return true;
		}

		// 提交成功，判断是否还有需要推送的
		boolean pushEnd(boolean res) {
			for (String key : buf) {
				synchronized (bufferPushCounts) {
					synchronized (bufferData) {
						short[] pushSize = bufferPushCounts.get(key);
						pushSize[this.ppo.pushMsgType]++;
						if (pushSize[0] + pushSize[1] >= pushSize[2]) {
							bufDataPushCounts.remove(key);
							bufferData.remove(key);
						}
					}
				}
			}
			LOG.info("[" + this.npo.NODE_ID + ":" + this.ppo.PUSH_ID + "]推送记录数：" + buf.size());
			// 写成功订阅日志
			if (res)
				buf.clear();
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
				if (this.ppo.PUSH_EXPR != null && !this.ppo.PUSH_EXPR.equals("")) {// 编译计算表达式
					termExp = AviatorEvaluator.compile(this.ppo.PUSH_EXPR, true);
					this.ppo.colMacs = termExp.getVariableNames().toArray(new String[0]);
					if (this.ppo.colMacs.length > 0) {
						this.ppo.termIndex = new short[this.ppo.colMacs.length];
						for (int i = 0; i < this.ppo.colMacs.length; i++) {
							Integer index = this.npo.fieldNameRel.get(this.ppo.colMacs[i]);
							this.ppo.termIndex[i] = index.shortValue();
						}
					}
				}
				conn = PushConnection.createConnection(this);
				inited = 1;
			} catch (Exception e) {
				LOG.error("初始化推送配置打开连接异常", e);
			}
		}
	}

	static Map<String, Object> calcEnv = new HashMap<String, Object>();

	public static void add(NodePO npo, BufferRow data, Object[] incData) {
		synchronized (dataBuffers) {
			HashMap<String, short[]> cbufs = bufDataPushCounts.get(npo.NODE_ID);
			if (cbufs == null) {
				cbufs = new HashMap<String, short[]>();
				bufDataPushCounts.put(npo.NODE_ID, cbufs);
			}
			cbufs.put(data.key, new short[] { 0, 0, 0 });
			HashMap<String, BufferData> bufs = dataBuffers.get(npo.NODE_ID);
			if (bufs == null) {
				bufs = new HashMap<String, BufferData>();
				dataBuffers.put(npo.NODE_ID, bufs);
			}
			synchronized (bufs) {
				bufs.put(data.key, new BufferData(data.data, incData, System.currentTimeMillis()));
			}
		}
		for (InOutPO _push : npo.pushInfos) {
			PushPO ppo = (PushPO) _push;
			PushConfig pc = batchPushs.get(ppo.PUSH_ID);
			if (pc != null) {
				if (pc.ppo.HashCode == ppo.HashCode) {
					pc.ppo.SUPPORT_BATCH = ppo.SUPPORT_BATCH;
					pc.ppo.PUSH_BUFFER_SIZE = ppo.PUSH_BUFFER_SIZE;
					pc.ppo.PUSH_TIME_INTERVAL = ppo.PUSH_TIME_INTERVAL;
				} else {
					pc.ppo = ppo;
					pc.close();
					pc.inited = 0;
					pc.init();
				}
			} else {
				pc = new PushConfig(npo, ppo);
				pc.init();
				synchronized (batchPushs) {
					batchPushs.put(ppo.PUSH_ID, pc);
				}
			}
			if (pc.inited != 1) {
				if (LOG.isDebugEnabled())
					LOG.debug("订阅规则[" + pc.ppo.PUSH_ID + "]初始化失败，不推送数据：" + data);
				continue;
			}
			if (pc.termExp != null) {
				if (pc.ppo.termIndex != null) {
					calcEnv.clear();
					for (int i = 0; i < pc.ppo.termIndex.length; i++) {
						short index = pc.ppo.termIndex[i];
						Object obj = EStormConstant.castAviatorObject(data.data[index].toString()); // 需要做类型转换
						calcEnv.put(pc.ppo.colMacs[i], obj);
					}
				}
				try {
					Object obj = pc.termExp.execute(calcEnv);
					if (!EStormConstant.getAviatorBoolean(obj)) {
						if (LOG.isDebugEnabled())
							LOG.debug("数据不满足订阅规则[" + pc.ppo.PUSH_ID + "]过滤条件，不推送数据：" + data);
						continue;
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			synchronized (pc.bufs) {
				synchronized (pc.bufferPushCounts) {
					pc.bufs.add(data.key);// 添加到缓存中
					pc.bufferPushCounts.get(data.key)[2]++;
				}
			}
		}
	}

	private java.util.concurrent.ExecutorService threadPoolExecutorService;

	public PushManage() {
		int maxThreads = StormNodeConfig.conf.getInt(EStormConstant.STORM_MSG_PUSH_THREAD_THREADS, 10);
		retryTimes = StormNodeConfig.conf.getInt(EStormConstant.STORM_MSG_PUSH_RETRY_TIMES, 3);
		threadPoolExecutorService = Threads.getBoundedCachedThreadPool(maxThreads, 60L, TimeUnit.SECONDS,
				Threads.newDaemonThreadFactory("Push."));
	}

	@Override
	public void run() {
		Thread.currentThread().setName("pushMag");
		long lastClearTime = System.currentTimeMillis();
		while (true) {
			long now = System.currentTimeMillis();
			try {
				for (Map.Entry<Long, PushConfig> _pc : batchPushs.entrySet()) {
					PushConfig pc = _pc.getValue();
					if (pc.inited != 1)
						continue;
					int blen = pc.bufs.size();

					synchronized (pc) {
						if (pc.errorTime != 0 && now - pc.errorTime < 600000) {// 有错误十分钟内不重试
							continue;
						}
						if (pc.ppo.SUPPORT_BATCH != 0) {
							if (blen > 0 &&
									(now - pc.lastPushTime >= pc.ppo.PUSH_TIME_INTERVAL || blen > pc.ppo.PUSH_BUFFER_SIZE)) {
								if (!pc.isRunning) {
									pc.lastPushTime = now;
									pc.isRunning = true;
									threadPoolExecutorService.execute(new PushThread(this, pc));
								}
							}
						} else {
							// pc.isRunning = false;
							if (blen > 0 && !pc.isRunning) {
								pc.isRunning = true;
								threadPoolExecutorService.execute(new PushThread(this, pc));
							}
						}
					}
				}
			} catch (Exception e) {
				LOG.error("push error", e);
			} finally {
				if (now - lastClearTime > 18000) {
					try {
						List<Long> ids = new ArrayList<Long>(dataBuffers.keySet());
						for (Long nodeId : ids) {
							HashMap<String, BufferData> bufData = dataBuffers.get(nodeId);
							synchronized (bufData) {
								for (Map.Entry<String, BufferData> key : bufData.entrySet()) {
									if (now - key.getValue().time > 180000) {
										bufData.remove(key.getKey());
									}
								}
							}
						}
					} catch (Exception e) {
					}
					lastClearTime = System.currentTimeMillis();
				}
				Utils.sleep(1000);
			}
		}
	}

	// StormNodeConfig.

	public static class PushThread extends Thread {
		private static final Log LOG = LogFactory.getLog(PushThread.class);
		PushConfig pc;
		PushManage pm = null;

		PushThread(PushManage pm, PushConfig pc) {
			this.pm = pm;
			this.pc = pc;
		}

		public void run() {
			int retry = 0;
			Thread.currentThread().setName("PushThread");
			while (retry < pm.retryTimes) {
				retry++;
				synchronized (pc.pushMetx) {
					pc.pushBegin();
					boolean res = false;
					try {
						res = pc.pushData();
					} catch (Exception e) {
						pc.close();
						LOG.error("推送订阅消息发生错误", e);
						Utils.sleep(1000);
					} finally {
						if (res || retry >= pm.retryTimes)
							pc.pushEnd(res);
					}
				}
			}
			if (retry >= pm.retryTimes) {// 失败
				pc.errorTime = System.currentTimeMillis();
				pc.bufs.removeAll(pc.buf);
			}
			pc.isRunning = false;
		}
	}
}

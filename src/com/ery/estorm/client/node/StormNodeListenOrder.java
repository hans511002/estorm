package com.ery.estorm.client.node;

import java.io.IOException;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.utils.Utils;

import com.ery.estorm.ServerInfo;
import com.ery.estorm.client.node.tracker.BackMasterTracker;
import com.ery.estorm.client.node.tracker.MasterTracker;
import com.ery.estorm.socket.Connection;
import com.ery.estorm.socket.OrderHeader;
import com.ery.estorm.socket.OrderHeader.Order;
import com.ery.estorm.util.DataInputBuffer;
import com.ery.estorm.util.DataOutputBuffer;
import com.ery.estorm.util.Threads;

public class StormNodeListenOrder extends Thread {
	private static final Log LOG = LogFactory.getLog(StormNodeConfig.class);
	public MasterTracker masterTracker = null;// 监听Master变化
	public BackMasterTracker backMasters;
	public Map<String, List<StormNodeProcess>> nodeProcesses = new HashMap<String, List<StormNodeProcess>>();
	private java.util.concurrent.ExecutorService threadPoolExecutorService;
	public Map<ServerInfo, BackMasterLisenThread> backMasteLisen = new HashMap<ServerInfo, BackMasterLisenThread>();
	byte[] buf = new byte[1];
	public DataOutputBuffer outputBuffer = new DataOutputBuffer(64 * 1024);
	public DataInputBuffer inputBuffer = new DataInputBuffer();

	public StormNodeListenOrder(int maxThreads) {
		if (maxThreads < 1)
			maxThreads = 1;
		backMasters = StormNodeConfig.backMasters;
		masterTracker = StormNodeConfig.masterTracker;
		threadPoolExecutorService = Threads.getBoundedCachedThreadPool(maxThreads, 60L, TimeUnit.SECONDS,
				Threads.newDaemonThreadFactory("ORDER."));
	}

	//
	public void register(String group, StormNodeProcess spoutNode) {
		synchronized (nodeProcesses) {
			List<StormNodeProcess> sps = nodeProcesses.get(group);
			if (sps == null) {
				sps = new ArrayList<StormNodeProcess>();
				nodeProcesses.put(group, sps);
			}
			if (!sps.contains(spoutNode)) {
				sps.add(spoutNode);
			}
		}
	}

	// // 同一虚拟机中只能是同一个TOP的节点，不需要删除
	// public boolean remove(String group, StormNodeProcess spoutNode) {
	// synchronized (nodeProcesses) {
	// List<StormNodeProcess> sps = nodeProcesses.get(group);
	// if (sps != null) {
	// boolean res = sps.remove(spoutNode);
	// if (sps.size() == 0) {
	// nodeProcesses.remove(group);
	// }
	// return res;
	// }
	// return false;
	// }
	// }
	//
	// public List<StormNodeProcess> remove(String group) {
	// synchronized (nodeProcesses) {
	// return nodeProcesses.remove(group);
	// }
	// }

	// 启动与备Master的通信
	public class BackMasterLisenThread extends Thread {
		ServerInfo backServerName;
		byte[] buf = new byte[1];
		DataOutputBuffer outputBuffer = new DataOutputBuffer(64 * 1024);
		DataInputBuffer inputBuffer = new DataInputBuffer();

		public BackMasterLisenThread(ServerInfo backServerName) {
			this.backServerName = backServerName;
		}

		public void run() {
			Connection con = null;
			while (backMasters.masterServers.containsKey(backServerName.hostName)) {
				con = StormNodeConfig.getConnection(backServerName);
				try {
					while (con.input.read(buf) > 0) {
						OrderHeader header = OrderHeader.ParseHeader(buf[0], con.input, inputBuffer);
						// OrderHeader header = OrderHeader.ParseHeader(buf[0],
						// con.input);
						if (header.order == Order.ping) {
							// OrderHeader.sendHeader(con.output,
							// OrderHeader.createPing());
							OrderHeader.createPing().sendHeader(con.output, outputBuffer);
						} else {
							if (LOG.isDebugEnabled())
								LOG.debug("收到" + header);
							synchronized (threadPoolExecutorService) {
								threadPoolExecutorService
										.submit(new OrderCallable(StormNodeConfig.listen, con, header));
							}
						}
					}
				} catch (IOException e) {
					LOG.error("read order error:", e);
				}
			}
			if (con != null) {
				con.close();
			}
		}
	}

	// 主Master通信命令
	public void run() {
		Thread.currentThread().setName("NodeOrderListen");
		while (true) {
			if (masterTracker.master == null) {
				masterTracker.getMaster(true);
				if (masterTracker.master == null) {
					LOG.warn("主Master为空，等待Master启动或切换。");
					Utils.sleep(1000);
					continue;
				}
			}
			Connection con = StormNodeConfig.getConnection(masterTracker.master);
			try {
				while (con.input.read(buf) > 0) {
					OrderHeader header = OrderHeader.ParseHeader(buf[0], con.input, inputBuffer);
					if (header.order == Order.ping) {
						synchronized (con.output) {
							OrderHeader ping = OrderHeader.createPing();
							// ping.data.put("ServerName",
							// StormNodeConfig.localhost);
							// ping.data.put("runtops", StringUtils.join(",",
							// StormNodeConfig.StormIds));
							ping.sendHeader(con.output, outputBuffer);
						}
						// OrderHeader.sendHeader(con.output,
						// OrderHeader.createPing());
					} else {
						if (LOG.isDebugEnabled())
							LOG.debug("收到" + header);
						synchronized (threadPoolExecutorService) {
							threadPoolExecutorService.submit(new OrderCallable(this, con, header));
						}
					}
				}
			} catch (IOException e) {
				LOG.error("read order error:", e);
				if (e instanceof SocketException) {
					if (con.sc.isClosed() || !con.sc.isConnected() || "Connection reset".equals(e.getMessage()) ||
							"Socket is closed".equals(e.getMessage())) {
						LOG.error("Connection reset exit server handle", e);
						StormNodeConfig.close(con.server);
						con.close();
					}
				}
			}
		}
	}

	public void pauseOrder(OrderHeader header, Map<String, Object> res) {
		if (header.data == null || header.data.size() == 0) {// 全部暂停
			for (String group : nodeProcesses.keySet()) {
				for (StormNodeProcess sp : nodeProcesses.get(group)) {
					res.put(group + ":" + sp.getTaskId(), sp.pause());
				}
			}
		} else {
			for (String group : header.data.keySet()) {
				List<StormNodeProcess> nsp = nodeProcesses.get(group);
				if (nsp != null) {
					for (StormNodeProcess sp : nsp) {
						res.put(group + ":" + sp.getTaskId(), sp.pause());
					}
				}
			}
		}
	}

	public void stopOrder(OrderHeader header, Map<String, Object> res) {
		Set<String> stopSet;
		if (header.data == null) {// 全部停止
			stopSet = nodeProcesses.keySet();
		} else {
			stopSet = header.data.keySet();
		}
		header.fromHost = StormNodeConfig.localhost;
		header.numTasks = 0;
		Map<String, List<StormNodeProcess>> _nodeProcesses = new HashMap<String, List<StormNodeProcess>>();

		for (String group : stopSet) {// 需要先处理SpoutNode
			if (nodeProcesses.containsKey(group)) {
				for (StormNodeProcess sp : nodeProcesses.get(group)) {
					if (sp instanceof SpoutNode || sp instanceof SpoutQuery) {
						if (!_nodeProcesses.containsKey(group))
							_nodeProcesses.put(group, new ArrayList<StormNodeProcess>());
						_nodeProcesses.get(group).add(sp);
						res.put(group + ":" + sp.getTaskId(), sp.stop());
						header.numTasks++;
					}
				}
			}
		}
		for (String group : stopSet) {// 需要后处理BoltNode
			if (nodeProcesses.containsKey(group)) {
				for (StormNodeProcess sp : nodeProcesses.get(group)) {
					if (sp instanceof BoltNode || sp instanceof BoltQuery) {
						if (!_nodeProcesses.containsKey(group))
							_nodeProcesses.put(group, new ArrayList<StormNodeProcess>());
						_nodeProcesses.get(group).add(sp);
						res.put(group + ":" + sp.getTaskId(), sp.stop());
						header.numTasks++;
					}
				}
			}
		}
		boolean allStoped = true;
		while (true) {
			allStoped = true;
			for (String group : stopSet) {
				if (_nodeProcesses.containsKey(group)) {
					for (StormNodeProcess sp : _nodeProcesses.get(group)) {
						if (!sp.isStoped()) {
							allStoped = false;
							break;
						}
					}
				}
				if (!allStoped)
					break;
			}
			if (!allStoped)
				Utils.sleep(100);
			else
				break;
		}
	}

	// 独立分布式查询服务命令
	public void queryOrder(OrderHeader header, Map<String, Object> res) {
		if (StormNodeConfig.spoutQuery == null) {// 未实现
			res.put("Exception", new Exception("当前暂未实现"));
		}
		SpoutOutputCollector sop = StormNodeConfig.spoutQuery.getCollector();
		if (sop != null) {
			Object args = header.data.get("queryArgs");
			if (args instanceof List) {// 多组查询参数
				List parms = (List) args;
				header.numTasks = (short) parms.size();
				for (Object arg : parms) {
					List<Object> s = new ArrayList<Object>();
					s.add(header);// SEQ
					s.add(arg);// args
					sop.emit(StormNodeConfig.spoutQuery.getStreamId(), s);
				}
			} else {// map 一组参数
				List<Object> s = new ArrayList<Object>();
				s.add(header);// SEQ
				s.add(args);// args
				sop.emit(StormNodeConfig.spoutQuery.getStreamId(), s);
			}
			res.put("submit", true);
		}
	}

	public void searchOrder(OrderHeader header, Map<String, Object> res) {
		// 运行节点内存中查询数据
	}

	public void ExecOrder(Connection con, OrderHeader header) {
		try {
			header.isRequest = false;// 标记成回复
			Map<String, Object> res = new HashMap<String, Object>();
			if (header.order == Order.pause) {
				pauseOrder(header, res);
			} else if (header.order == Order.stop) {
				stopOrder(header, res);
			} else if (header.order == Order.query) {
				queryOrder(header, res);
			} else if (header.order == Order.search) {
				searchOrder(header, res);

			} else if (header.order == Order.close) {
				StormNodeConfig.close(con.server);
			}
			header.data = res;
		} catch (Exception e) {
			header.data.put("error：", e);
		} finally {
			if (header.order == Order.close)
				return;
			boolean res = false;
			int tryNum = 0;
			while (tryNum++ < 5) {
				synchronized (con.output) {
					try {
						// OrderHeader.sendHeader(con.output, header);
						header.sendHeader(con.output, con.outputBuffer);
						res = true;
						break;
					} catch (IOException e) {
						LOG.error("发送回复消息失败:" + header, e);
						Utils.sleep(1000);
					}
				}
			}// 重试发送失败，切换到主Master上
			if (!res) {// 发送失败,重发
				tryNum = 0;
				while (tryNum++ < 50) {// 50秒重发,等待Master切换
					StormNodeConfig.close(con.server);
					con = StormNodeConfig.getConnection(masterTracker.master);
					synchronized (con.output) {
						try {
							header.sendHeader(con.output, con.outputBuffer);
							// OrderHeader.sendHeader(con.output, header);
							res = true;
							break;
						} catch (IOException e) {
							LOG.error("发送回复消息失败:" + header, e);
							Utils.sleep(1000);
						}
					}
				}
			}
		}
	}

	public static class OrderCallable implements Callable<Object> {
		private StormNodeListenOrder executive;
		OrderHeader header;
		Connection con;

		public OrderCallable(StormNodeListenOrder executive, Connection con, OrderHeader header) {
			this.executive = executive;
			this.header = header;
			this.con = con;
		}

		@Override
		public Object call() throws Exception {
			executive.ExecOrder(con, header);
			return null;
		}
	}
}

package com.ery.estorm.socket;

import java.io.IOException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import backtype.storm.utils.Utils;

import com.ery.estorm.ServerInfo;
import com.ery.estorm.config.Configuration;
import com.ery.estorm.config.EStormConstant;
import com.ery.estorm.daemon.DaemonMaster;
import com.ery.estorm.socket.OrderHeader.Order;
import com.ery.estorm.socket.ServerOrders.ServerOrder;
import com.ery.estorm.util.DataInputBuffer;
import com.ery.estorm.util.DataOutputBuffer;

//对外提供服务类
public class SCServer {
	DaemonMaster master;
	private static final Log LOG = LogFactory.getLog(SCServer.class.getName());
	Configuration conf;
	// private ExecutorService clientPool;
	public List<Handler> handles = new ArrayList<Handler>();
	List<Handler> removeHandles = new ArrayList<Handler>();
	Listener listener;
	public String _listenIp;
	public int _listenPort;
	Thread listenThread;

	int activeIntevalTime = 30;
	private int maxSocketClientSize = 1000;
	protected volatile boolean running = true; // true while server runs

	// public Map<ServerName, List<String>> HostRunTops = new
	// HashMap<ServerName, List<String>>();

	// 监听线程,保持连接
	class ListenThread extends Thread {
		SCServer server;

		public ListenThread(SCServer server) {
			this.server = server;
		}

		@Override
		public void run() {
			while (running) {
				Utils.sleep(30000);
				for (Handler handler : handles) {
					try {
						handler.ping();
					} catch (IllegalMonitorStateException e) {
					} catch (Exception e) {
						if (e instanceof SocketException) {
							if (handler.isClose) {
								LOG.info("断开连接" + handler.sc.getInetAddress() + "=>" +
										handler.sc.getRemoteSocketAddress());
							} else if (handler.sc.isClosed() || !handler.sc.isConnected() ||
									e.getMessage().indexOf("Connection reset") >= 0 ||
									"Socket is closed".equals(e.getMessage())) {
								LOG.error("Connection reset exit server handle：" + handler.sc.getInetAddress() + "=>" +
										handler.sc.getRemoteSocketAddress(), e);
							} else {
								LOG.error("监听线程,保持连接异常", e);
							}
							handler.close();
						} else {
							LOG.error("监听线程,保持连接异常", e);
						}
					}
				}
				synchronized (removeHandles) {
					for (Handler e : removeHandles) {
						removeHandler(e);
					}
					removeHandles.clear();
				}
			}
		}
	}

	public class Handler extends Thread {
		public Socket sc;
		public SCServer server;
		// PrintWriter streamWriter;
		// BufferedReader streamReader;
		byte[] buf = new byte[4];
		public long lastActiveTime;
		// String user;
		// String pass;
		public boolean isSupervisor;// 连接类型,是否Storm运行客户端 排除查询客户端
		public ServerInfo clientName;
		public boolean isClose = false;
		public boolean longined = false;
		public DataOutputBuffer outputBuffer = new DataOutputBuffer(64 * 1024);
		public DataInputBuffer inputBuffer = new DataInputBuffer();

		public Handler(SCServer server, Socket sc) {
			this.server = server;
			this.sc = sc;
		}

		public void run() {
			Thread.currentThread().setName("SocketServer");

			LOG.info("begin Thread read data for client " + sc.getRemoteSocketAddress().toString());
			while (running) {
				try {
					int len = this.sc.getInputStream().read(buf, 0, 1);
					if (len > 0) {
						lastActiveTime = System.currentTimeMillis();
						OrderHeader header = OrderHeader.ParseHeader(buf[0], sc.getInputStream(), inputBuffer);
						if (header != null && header.order != Order.ping) {// 非ping
							if (LOG.isDebugEnabled())
								LOG.debug("收到" + header);
							ServerOrder call = ServerOrder.createResponseCall(this, sc, header);
							if (call != null) {
								int res = call.process(header);
								if (header.order == Order.close) {
									break;
								} else if (res != 0) {// 登录失败
									if (header.order == Order.login) {
										break;
									}
								}
							}
						}
					} else if (len == -1) {
						Utils.sleep(5000);
					}
				} catch (Exception e) {
					if (e instanceof SocketException) {
						if (isClose) {
							LOG.info("断开连接" + sc.getInetAddress() + "=>" + sc.getRemoteSocketAddress());
							break;
						} else if (this.sc.isClosed() || !this.sc.isConnected() ||
								e.getMessage().indexOf("Connection reset") >= 0 ||
								"Socket is closed".equals(e.getMessage())) {
							LOG.warn(
									"Connection reset exit server handle：" + sc.getInetAddress() + "=>" +
											sc.getRemoteSocketAddress(), e);
							break;
						} else {
							LOG.error("server handle", e);
						}
					} else {
						LOG.error("server handle", e);
					}
				}
			}
			close();
		}

		public void ping() throws IOException {
			if (!longined)
				return;
			long l = System.currentTimeMillis();
			if (l - lastActiveTime > activeIntevalTime * 1000) {
				synchronized (sc) {
					sc.getOutputStream().flush();
					OrderHeader.createPing().sendHeader(sc.getOutputStream(), outputBuffer);
					lastActiveTime = l;
				}
			}
		}

		public void close() {
			try {
				isClose = true;
				sc.close();
			} catch (IOException e) {
				LOG.error("close client socket exception", e);
			}
			synchronized (removeHandles) {
				removeHandles.add(this);
			}
			// removeHandler(this);
		}
	}

	String bindAddress;

	// 为空则不支持查询，未启动服务
	public Handler findSpoutQueryHandler() {
		return null;
	}

	// 要求客户端重连接ServerName保持不变
	public Handler findClientHandler(ServerInfo clientName) {
		for (Handler hand : handles) {
			if (hand.clientName.equals(clientName))
				return hand;
		}
		return null;
	}

	public SCServer(DaemonMaster master) throws IOException {
		this.master = master;
		this.conf = master.getConfiguration();
		bindAddress = conf.get(EStormConstant.MASTER_IPC_ADDRESS_KEY);

		this._listenPort = conf.getInt(EStormConstant.MASTER_PORT_KEY, 3000);
		this.maxSocketClientSize = conf.getInt(EStormConstant.MAX_CLIENT_SIZE_KEY, 1000);
		listener = new Listener(this);
		listenThread = new ListenThread(this);
	}

	private class Listener extends Thread {
		SCServer server;
		ServerSocket ss;

		Listener(SCServer server) throws IOException {
			this.server = server;
			if (bindAddress == null) {
				ss = new ServerSocket(_listenPort);
			} else {
				ss = new ServerSocket();
				ss.bind(new InetSocketAddress(bindAddress, _listenPort));
			}
		}

		public void run() {
			Socket incomingConnection = null;
			try {
				LOG.info("start socket server on port:" + _listenPort);
				LOG.info("Allow accept " + maxSocketClientSize + " client ");
				while (running) {
					incomingConnection = null;
					incomingConnection = ss.accept();
					// stats.initStats(incomingConnection,procManger);
					if (handles.size() >= maxSocketClientSize) {
						try {
							// 返回拒绝信息
							incomingConnection.close();
						} catch (Exception e) {
						}
						continue;
					}
					incomingConnection.setKeepAlive(true);
					// incomingConnection.setSoTimeout(10);// 读取超时时间 ms,
					// 轮循策略时使用，当前使用多线程阻塞读取
					incomingConnection.setTcpNoDelay(true);
					LOG.info("begin Server Thread for client " + incomingConnection.getRemoteSocketAddress().toString());
					addHandler(new Handler(server, incomingConnection));
				}
			} catch (BindException e) {
				LOG.error("start server listen failed", e);
				Thread.currentThread().stop();
			} catch (IOException e) {
				LOG.error("Unable to instantiate a ServerSocket on port: " + _listenPort, e);
				Thread.currentThread().stop();
			} finally {
				if (incomingConnection != null) {
					try {
						incomingConnection.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
		}

		InetSocketAddress getAddress() {
			return (InetSocketAddress) ss.getLocalSocketAddress();
		}
	}

	public synchronized InetSocketAddress getListenerAddress() {
		return listener.getAddress();
	}

	public synchronized void addHandler(Handler e) {
		handles.add(e);
		e.setDaemon(true);// 结束后自动回收
		e.start();
		// clientPool.execute(e);
	}

	public synchronized void removeHandler(Handler e) {
		handles.remove(e);
		e.stop();
	}

	public void start() {
		startThreads();
	}

	public synchronized void startThreads() {
		running = true;
		listener.start();
		listenThread.start();
	}

	public void stop() {
		running = false;
		for (Handler handler : handles) {
			handler.close();
		}
		for (Handler e : removeHandles) {
			removeHandler(e);
		}
		listenThread.stop();
		listener.stop();
		removeHandles.clear();
		// clientPool.shutdownNow();
		// notifyAll();
	}

}

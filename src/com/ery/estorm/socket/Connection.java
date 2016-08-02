package com.ery.estorm.socket;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import backtype.storm.utils.Utils;

import com.ery.estorm.ServerInfo;
import com.ery.estorm.client.node.StormNodeConfig;
import com.ery.estorm.socket.OrderHeader.Order;
import com.ery.estorm.util.DataInputBuffer;
import com.ery.estorm.util.DataOutputBuffer;
import com.ery.estorm.util.ToolUtil;

public class Connection implements Closeable {
	private static final Log LOG = LogFactory.getLog(Connection.class.getName());
	public Socket sc;
	public ServerInfo server;
	public long sTime;// 连接时间
	public long lastActive;
	int sleepMillis = 1000;
	public OutputStream output;
	public InputStream input;
	public DataOutputBuffer outputBuffer = new DataOutputBuffer(64 * 1024);
	public DataInputBuffer inputBuffer = new DataInputBuffer();

	public Connection(ServerInfo server, int sleepMillis) {
		this.server = server;
		this.sleepMillis = sleepMillis;
		ReConnect();
	}

	public void ReConnect() {
		ReConnect(Integer.MAX_VALUE);
	}

	public void ReConnect(int times) {
		int trys = 0;
		while (trys++ <= times) {
			close();
			sc = new Socket();
			SocketAddress sa = new InetSocketAddress(server.getHostname(), server.getPort());
			try {
				sc.connect(sa);
				sTime = System.currentTimeMillis();
				output = this.sc.getOutputStream();
				input = this.sc.getInputStream();
				// 发送登录信息
				OrderHeader header = new OrderHeader();
				header.order = Order.login;
				header.isRequest = true;
				header.fromHost = StormNodeConfig.localhost;
				header.data.put("username", "supervisor");
				header.data.put("password", "supervisor");
				header.data.put("clientType", "supervisor");
				header.data.put("ServerName", StormNodeConfig.localhost);
				// if (this.server.equals(StormNodeConfig.masterTracker.master)) {
				// header.data.put("runtops", StringUtils.join(",", StormNodeConfig.StormIds));
				// }
				LOG.debug(" ===============login=========" + header);
				synchronized (output) {
					header.sendHeader(output, outputBuffer);
					// OrderHeader.sendHeader(output, header);
				}
				byte[] buf = new byte[1];
				long ls = System.currentTimeMillis();
				while (input.read(buf) > 0) {
					// OrderHeader resHeader = OrderHeader.ParseHeader(buf[0], input);
					OrderHeader resHeader = OrderHeader.ParseHeader(buf[0], input, inputBuffer);
					LOG.debug(" ===========res=============" + resHeader);
					if (resHeader.order == Order.ping) {
						OrderHeader.createPing().sendHeader(output, outputBuffer);
					} else {
						if (resHeader.order == Order.login && resHeader.isRequest == false && resHeader.serialNumber == header.serialNumber) {
							LOG.info("[login]" + StormNodeConfig.localhost + " logined " + resHeader.fromHost + "  "
									+ resHeader.data.get("description"));
							break;
						}
					}
					if (System.currentTimeMillis() - ls > 120000)
						throw new java.net.ConnectException("登录超时未应答登录验证信息");
				}
				break;
			} catch (IOException e) {
				if (e instanceof java.net.ConnectException) {
					if (trys < 60 || trys % 60 < 10) {
						LOG.warn("连接服务器[" + server.getHostname() + ":" + server.getPort() + "]失败，Retrys:" + trys + "，Sleep " + sleepMillis
								+ " Millis to try", e);
						Utils.sleep(sleepMillis);
					} else {
						LOG.warn("连接服务器[" + server.getHostname() + ":" + server.getPort() + "]失败，Retrys:" + trys + "，Sleep " + sleepMillis
								* 10 + " Millis to try", e);
						Utils.sleep(sleepMillis * 10);
					}
				}
			}
			Utils.sleep(100);
		}
	}

	public void close() {
		ToolUtil.close(output);
		ToolUtil.close(input);
		ToolUtil.close(sc);
		sc = null;
		output = null;
		input = null;
	}

	public boolean isActiveMaster() {
		return this.server.equals(StormNodeConfig.masterTracker.master);
	}
}

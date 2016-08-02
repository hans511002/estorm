package com.ery.estorm.socket;

import java.io.IOException;
import java.net.Socket;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.ery.estorm.ServerInfo;
import com.ery.estorm.socket.OrderHeader.Order;
import com.ery.estorm.socket.OrderHeader.OrderSeq;
import com.ery.estorm.socket.SCServer.Handler;

public class ServerOrders {
	private static final Log LOG = LogFactory.getLog(ServerOrder.class.getName());

	public abstract static class ServerOrder {
		public Handler handler;
		public Socket sc;
		final Order order;

		public ServerOrder(Handler handler, Socket sc, Order order) {
			this.order = order;
			this.handler = handler;
			this.sc = sc;

		}

		public ServerInfo getServerName() {
			return this.handler.server.master.getServerName();
		}

		public abstract int process(OrderHeader header) throws IOException;

		public static ServerOrder createResponseCall(Handler handler, Socket sc, OrderHeader header) throws IOException {
			if (header.order == Order.ping) {
				// if (header.data.size() > 1) {
				// Object clientHost = header.data.get("ServerName");
				// Object runTops = header.data.get("runtops");
				// if (clientHost != null && runTops != null) {
				// ServerName spSn =
				// ServerName.parseServerName(clientHost.toString());
				// synchronized (handler.server.HostRunTops) {
				// handler.server.HostRunTops.put(spSn,
				// Arrays.asList(runTops.toString().split(",")));
				// }
				// }
				// }
				return null;
			}
			if (header.order == Order.pause) {
				return new PauseOrder(handler, sc);
			}
			if (header.order == Order.stop) {
				return new StopOrder(handler, sc);
			}
			if (header.order == Order.query) {
				return new QueryOrder(handler, sc);
			}
			if (header.order == Order.search) {
				return new SearchClientOrder(handler, sc);
			}
			if (header.order == Order.login) {
				return new LoginOrder(handler, sc);
			}
			return null;

		}
	}

	public static class PauseOrder extends ServerOrder {
		PauseOrder(Handler handler, Socket sc) {
			super(handler, sc, Order.pause);
		}

		// 处理请求数据，并返回结果
		public int process(OrderHeader header) throws IOException {
			long l = System.currentTimeMillis();
			if (!header.isRequest) {// 客户端的回复// 不支持客户端反向请求
				synchronized (OrderHeader.orderResponse) {
					OrderSeq seq = OrderSeq.createResponseOrderSeq(header);
					List<OrderHeader> res = OrderHeader.orderResponse.get(seq);
					if (res != null)
						res.add(header);
				}
			}
			handler.lastActiveTime = l;
			return 0;
		}
	}

	public static class StopOrder extends ServerOrder {

		StopOrder(Handler handler, Socket sc) {
			super(handler, sc, Order.stop);
		}

		@Override
		public int process(OrderHeader header) throws IOException {
			if (header.order != this.order)
				return -1;
			if (!header.isRequest) {// 客户端的回复
				synchronized (OrderHeader.orderResponse) {
					OrderSeq seq = OrderSeq.createResponseOrderSeq(header);
					List<OrderHeader> res = OrderHeader.orderResponse.get(seq);
					if (res != null) {// 添加了监听才保存回复
						LOG.info("  listen " + seq);
						res.add(header);
					} else {
						LOG.warn("not listen " + seq);
					}
				}
			}
			return 0;
		}
	}

	public static class QueryOrder extends ServerOrder {

		QueryOrder(Handler handler, Socket sc) {
			super(handler, sc, Order.query);
		}

		@Override
		public int process(OrderHeader header) throws IOException {
			if (header.order != this.order)
				return -1;
			if (header.isRequest) {// 本身是服务端，接受来自查询客户端的请求
				OrderHeader serverQueryHeader = new OrderHeader();// 生成新的header请求
				serverQueryHeader.fromHost = this.handler.server.master.getServerName();
				serverQueryHeader.isRequest = true;
				serverQueryHeader.order = Order.query;
				// 把原始请求包一起发送到SPOUT，再流转到最后一个Bolt,当发送请求的服务器挂掉后，可以发送到其它服务端
				// 其它服务端再扫描是否存在此客户端标识的handler，存在再发送数据
				serverQueryHeader.data.put("CLIENT_QUERY_HEADER", header);
				handler.lastActiveTime = System.currentTimeMillis();
				Handler spQueryHand = this.handler.server.findSpoutQueryHandler();// 查找
																					// spoutQuery所在的hander
				if (spQueryHand != null) {
					synchronized (spQueryHand.sc) {
						serverQueryHeader.sendHeader(spQueryHand.sc.getOutputStream(), this.handler.outputBuffer); // 发送查询命令到
																													// spoutQuery
					}
					orderRequest.put(OrderSeq.createOrderSeq(serverQueryHeader), this.handler);// 使用本地序列，保证唯一性
				} else {
					synchronized (this.handler.sc) {
						header.data.clear();
						header.data.put("error", "查询服务未启动");
						header.sendHeader(handler.sc.getOutputStream(), this.handler.outputBuffer); //
					}
				}
			} else {// 客户端的回复 如果是数据 则查找请求客户端的handler
				OrderSeq oseq = OrderSeq.createResponseOrderSeq(header);
				// 从OrderHeader.orderResponse 中查找
				Handler hand = orderRequest.get(oseq);
				OrderHeader coh = (OrderHeader) header.data.get("CLIENT_QUERY_HEADER");
				coh.isRequest = false;
				coh.data = header.data;
				if (hand == null) {// not this host send request
					hand = this.handler.server.findClientHandler(coh.fromHost);
					if (header.data.containsKey("continue")) {
						orderRequest.put(oseq, hand);
					}
				}
				if (hand == null)
					return 0;
				synchronized (hand.sc) {
					coh.sendHeader(hand.sc.getOutputStream(), this.handler.outputBuffer);
				}
				if (header.data == null || header.data.size() == 0 || !header.data.containsKey("continue")) {
					orderRequest.remove(oseq);
				}
			}
			return 0;
		}
	}

	// 客户请求对象
	public static Map<OrderSeq, Handler> orderRequest = new HashMap<OrderSeq, Handler>();

	// 处理对用户客户端handler的查询确认请求
	public static class SearchClientOrder extends ServerOrder {
		SearchClientOrder(Handler handler, Socket sc) {
			super(handler, sc, Order.search);
		}

		@Override
		public int process(OrderHeader header) throws IOException {
			if (header.order != this.order)
				return -1;
			if (header.isRequest) {// 客户端来的请求
				header.isRequest = false;
				header.fromHost = getServerName();
				ServerInfo clientServerName = (ServerInfo) header.data.get("ServerName");
				header.data.clear();
				header.data.put("client", this.handler.server.findClientHandler(clientServerName) != null);
				handler.lastActiveTime = System.currentTimeMillis();
				synchronized (handler.sc) {
					header.sendHeader(handler.sc.getOutputStream(), handler.outputBuffer);
				}
			}
			return 0;
		}
	}

	// 处理登录请求
	public static class LoginOrder extends ServerOrder {

		LoginOrder(Handler handler, Socket sc) {
			super(handler, sc, Order.login);
		}

		@Override
		public int process(OrderHeader header) throws IOException {
			if (header.order != this.order)
				return -1;
			Map<String, Object> data = header.data;
			String name = data.get("username").toString();
			String pass = data.get("password").toString();
			// 判断登录
			LOG.info("[login]" + header);
			header.isRequest = false;
			header.fromHost = OrderHeader.hostName;
			String clientType = data.get("clientType").toString();
			if (clientType == null) {// 未知客户端
				header.data.clear();
				header.data.put("error", "未知客户类型不允许登录");
				synchronized (handler.sc) {
					header.sendHeader(sc.getOutputStream(), handler.outputBuffer);
				}
				this.handler.close();
				return -1;
			} else if (clientType.equals("supervisor")) {
				Object sno = data.get("ServerName");
				if (sno instanceof ServerInfo) {
					this.handler.clientName = (ServerInfo) sno;
					this.handler.isSupervisor = true;
				} else {
					String sn = data.get("ServerName").toString();
					if (sn != null) {
						this.handler.clientName = new ServerInfo(sn);
						this.handler.isSupervisor = true;
					}
				}
				synchronized (handler.sc) {
					header.data.clear();
					header.data.put("description", "Successful login");
					header.sendHeader(sc.getOutputStream(), handler.outputBuffer);
				}
				LOG.info("[login]" + header.fromHost + " login Successful " +
						(new Date(System.currentTimeMillis()).toLocaleString()));
			} else if (clientType.equals("queryClient")) {
				synchronized (handler.sc) {
					header.data.clear();
					header.data.put("description", "Successful login");
					header.sendHeader(sc.getOutputStream(), handler.outputBuffer);
				}
			}
			handler.longined = true;
			// 登录失败
			// this.handler.close();
			return 0;
		}
	}
}

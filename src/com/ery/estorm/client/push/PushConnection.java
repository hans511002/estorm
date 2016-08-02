package com.ery.estorm.client.push;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.ery.estorm.client.push.PushManage.PushConfig;
import com.ery.estorm.exceptions.PushException;

public abstract class PushConnection {
	private static final Log LOG = LogFactory.getLog(PushConnection.class);
	public final PushConfig pc;
	public boolean isConnected = false;

	public PushConnection(PushConfig pc) {
		this.pc = pc;
	}

	// public long lastPushTime = System.currentTimeMillis();
	// public PushPO ppo;
	// public PushConnection conn;
	// public List<String> bufs = new ArrayList<String>();// 每个PUSHPO的缓存列表
	// public boolean isRunning = false;
	// public long errorTime = 0;
	// // public long PUSH_ID;
	// // public long NODE_ID;
	// // public long PUSH_TYPE;// 订阅类型 订阅类型// 0，WS // 1，SOCKET // 2，HTTP // 3，ZK // 4, kafka
	// // public String PUSH_URL;// 订阅地址
	// // public String PUSH_USER;
	// // public String PUSH_PASS;
	// // public String PUSH_EXPR;// 条件表达式
	// // public String PUSH_PARAM;// push参数，根据接口类型不一样，参数规则不一样
	// // public int SUPPORT_BATCH;
	// // public long PUSH_BUFFER_SIZE;
	// // public long PUSH_TIME_INTERVAL;

	/**
	 * 打开连接，开启事务
	 */
	public abstract void open();

	public abstract void close();

	/**
	 * 锁PC.bufs，内有重试机制，成功的需要移除
	 * 
	 * @param key
	 */
	public abstract void putData(List<String> keys) throws PushException;

	public abstract void commit();

	public static PushConnection createConnection(PushConfig pc) {
		switch (pc.ppo.PUSH_TYPE) {
		case 0:// WS
			return new WSPush(pc);
		case 1:// SOCKET
			return new SocketPush(pc);
		case 2:// http
			return new HttpPush(pc);
		case 3:// ZK
			return new ZKPush(pc);
		case 4:// kafka
			return new KafkaPush(pc);
		}
		return null;
	}

}

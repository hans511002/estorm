package com.ery.estorm.client.push;

import java.net.Socket;
import java.util.List;

import com.ery.estorm.client.push.PushManage.PushConfig;
import com.ery.estorm.exceptions.PushException;

public class SocketPush extends PushConnection {
	Socket sc;// SOCKET 订阅

	public SocketPush(PushConfig pc) {
		super(pc);
	}

	@Override
	public void open() {
		// TODO Auto-generated method stub

	}

	@Override
	public void close() {
		// TODO Auto-generated method stub

	}

	@Override
	public void commit() {
		// TODO Auto-generated method stub

	}

	@Override
	public void putData(List<String> key) throws PushException {
		// TODO Auto-generated method stub

	}

}

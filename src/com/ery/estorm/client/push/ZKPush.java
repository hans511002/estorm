package com.ery.estorm.client.push;

import java.util.List;

import com.ery.estorm.client.push.PushManage.PushConfig;
import com.ery.estorm.exceptions.PushException;
import com.ery.estorm.zk.RecoverableZooKeeper;

public class ZKPush extends PushConnection {

	RecoverableZooKeeper zk;// ZK 订阅

	public ZKPush(PushConfig pc) {
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

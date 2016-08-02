package com.ery.estorm.client.node;

import com.ery.estorm.daemon.topology.NodeInfo;

public interface StormNodeProcess {

	public void nodeConfigChanage(NodeInfo oldN, NodeInfo newN);

	public boolean stop();

	public boolean isStoped();

	public boolean pause();

	public void recover();

	public int getTaskId();

	public NodeInfo getNodeInfo();
}

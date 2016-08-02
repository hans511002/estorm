package com.ery.estorm.daemon;

import com.ery.estorm.ServerInfo;
import com.ery.estorm.config.Configuration;
import com.ery.estorm.zk.ZooKeeperWatcher;

public interface Server extends Abortable, Stoppable {
	/**
	 * Gets the configuration object for this server.
	 */
	Configuration getConfiguration();

	/**
	 * Gets the ZooKeeper instance for this server.
	 */
	ZooKeeperWatcher getZooKeeper();

	/**
	 * @return The unique server name for this server.
	 */
	ServerInfo getServerName();
}

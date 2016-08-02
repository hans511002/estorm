package com.ery.estorm.client.store;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.ery.estorm.client.store.StoreManage.StoreConfig;
import com.ery.estorm.exceptions.PushException;

public abstract class StoreConnection {
	private static final Log LOG = LogFactory.getLog(StoreConnection.class);
	public final StoreConfig pc;
	public boolean isConnected = false;

	public StoreConnection(StoreConfig pc) {
		this.pc = pc;
	}

	// public String TARGET_NAME;// 目标名称存储源不一样，代表不同含义，可以是文件名，表名
	// public String STORE_FIELD_MAPPING;// 存储字段映射： srcField:destField，srcField:destField，srcField:destField
	// public String TARGET_PRIMARY_RULE;// 目标主键规则： HBASE：rowkey计算表达式 ORACLE,MYSQL：字段列表
	// public int AUTO_REAL_FLUSH;// '是否自动刷新（自动写入到外部存储）',
	// public long FLUSH_BUFFER_SIZE;// '刷新缓存大小，用于批量刷新，提高性能',
	// public long FLUSH_TIME_INTERVAL;// '刷新时间间隔，单位 ms',

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
	public abstract void putData(String key) throws PushException;

	public abstract void commit();

	public static StoreConnection createConnection(StoreConfig pc) {
		switch (pc.dspo.dataSourcePo.DATA_SOURCE_TYPE) {

		}
		return null;
	}

}

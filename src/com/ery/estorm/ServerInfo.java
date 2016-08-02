package com.ery.estorm;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.ery.estorm.config.EStormConstant;
import com.ery.estorm.exceptions.DeserializationException;
import com.ery.estorm.util.Addressing;
import com.ery.estorm.util.GZIPUtils;
import com.ery.base.support.utils.Convert;

public class ServerInfo implements Comparable<ServerInfo>, java.io.Serializable {
	public static final Log LOG = LogFactory.getLog(ServerInfo.class);

	private static final long serialVersionUID = 948662243093059938L;

	public final String hostName;
	public final int port;
	public long startTime;
	public long deadTime = 0;

	/**
	 * Cached versioned bytes of this ServerName instance.
	 * 
	 * @see #getVersionedBytes()
	 */
	public static final List<ServerInfo> EMPTY_SERVER_LIST = new ArrayList<ServerInfo>(0);

	public ServerInfo(final String hostname, final int port, final long startTime) {
		// Drop the domain is there is one; no need of it in a local cluster.
		// With it, we get long
		// unwieldy names.
		this.hostName = hostname;
		this.port = port;
		this.startTime = startTime;
	}

	public ServerInfo(String hostPort) {
		String tmp[] = hostPort.split(":");
		hostName = tmp[0];
		port = Convert.toInt(tmp[1]);
		if (tmp.length > 2) {
			startTime = Convert.toLong(tmp[2]);
		}
	}

	public ServerInfo(final String hostName, final int port) {
		this.hostName = hostName;
		this.port = port;
	}

	@Override
	public String toString() {
		String info = "hostName:" + hostName + ",port:" + port + " startTime:" +
				EStormConstant.sdf.format(new Date(startTime));
		if (deadTime > 0) {
			info += ",deadTime:" + EStormConstant.sdf.format(new Date(deadTime));
		}
		return info;
	}

	/**
	 * @return Return a SHORT version of {@link ServerInfo#toString()}, one that
	 *         has the host only, minus the domain, and the port only -- no
	 *         start code; the String is for us internally mostly tying threads
	 *         to their server. Not for external use. It is lossy and will not
	 *         work in in compares, etc.
	 */
	public String getHostPort() {
		return hostName + ":" + port;
	}

	public String getHostname() {
		return hostName;
	}

	public int getPort() {
		return port;
	}

	public long getStartTime() {
		return this.startTime;
	}

	public long getDeadTime() {
		return deadTime;
	}

	public void setDeadTime(long t) {
		deadTime = t;
	}

	public String getHostAndPort() {
		return Addressing.createHostAndPortStr(this.hostName, this.port);
	}

	@Override
	public int compareTo(ServerInfo other) {
		int compare = this.getHostname().toLowerCase().compareTo(other.getHostname().toLowerCase());
		if (compare != 0)
			return compare;
		compare = this.getPort() - other.getPort();
		if (compare != 0)
			return compare;
		return (int) (this.getStartTime() - other.getStartTime());
	}

	@Override
	public int hashCode() {
		return this.toString().hashCode();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (o == null)
			return false;
		if (!(o instanceof ServerInfo))
			return false;
		return this.compareTo((ServerInfo) o) == 0;
	}

	/**
	 * @param left
	 * @param right
	 * @return True if <code>other</code> has same hostname and port.
	 */
	public static boolean isSameHostnameAndPort(final ServerInfo left, final ServerInfo right) {
		if (left == null)
			return false;
		if (right == null)
			return false;
		return left.getHostname().equals(right.getHostname()) && left.getPort() == right.getPort();
	}

	public byte[] toByteArray() {
		try {
			ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
			ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
			try {
				objectOutputStream.writeObject(this);
				byte[] bts = GZIPUtils.zip(byteArrayOutputStream.toByteArray());
				return bts;
			} finally {
				objectOutputStream.close();
				byteArrayOutputStream.close();
			}
		} catch (IOException e) {
			return new byte[0];
		}
	}

	public static ServerInfo parseFrom(final byte[] data) throws DeserializationException {
		if (data == null || data.length <= 0)
			return null;
		byte[] bts;
		try {
			bts = GZIPUtils.unzip(data);
			ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bts);
			ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream);
			try {
				return (ServerInfo) objectInputStream.readObject();
			} finally {
				objectInputStream.close();
				byteArrayInputStream.close();
			}
		} catch (Exception e) {
			LOG.error("反序列化错误", e);
			throw new DeserializationException(e);
		}
	}
}

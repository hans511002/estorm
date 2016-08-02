package com.ery.estorm.socket;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import backtype.storm.utils.WritableUtils;

import com.ery.estorm.ServerInfo;
import com.ery.estorm.join.DataSerializable;
import com.ery.estorm.util.DataInputBuffer;
import com.ery.estorm.util.DataOutputBuffer;
import com.ery.estorm.util.GZIPUtils;

public class OrderHeader implements Serializable {
	private static final long serialVersionUID = 2027998401675352881L;
	public static ServerInfo hostName;
	public Order order = Order.ping;// 命令编码
	public final int hostNumber;//
	public final int serialNumber;
	public boolean isRequest;// 1请求 0回复
	public short numTasks;
	public ServerInfo fromHost = null;// 来源host
	public Map<String, Object> data = new HashMap<String, Object>();

	public String toString() {
		return toString(true);
	}

	public String toString(boolean haveData) {
		StringBuffer sb = new StringBuffer();
		sb.append("hostNumber=" + hostNumber);
		sb.append("\nserialNumber=" + serialNumber);
		sb.append("\norder=" + order);
		sb.append("\nisRequest=" + isRequest);
		sb.append("\nnumTasks=" + numTasks);
		sb.append("\nfromHost=" + fromHost);
		if (haveData)
			sb.append(" data=" + data);
		return sb.toString();
	}

	public enum Order implements Serializable {
		ping, pause, stop, query, login, close, search,
	}

	public static byte OrderToInt(Order o) {
		switch (o) {
		case ping:
			return 0;
		case login:
			return 1;
		case pause:
			return 2;
		case stop:
			return 3;
		case query:// 查询数据
			return 4;
		case search:// 查询客户端
			return 5;
		case close:
			return 100;
		default:
			return 0;
		}
	}

	public static Order IntToOrder(byte o) {
		switch (o) {
		case 0:
			return Order.ping;
		case 1:
			return Order.login;
		case 2:
			return Order.pause;
		case 3:
			return Order.stop;
		case 4:// 查询数据
			return Order.query;
		case 5:// 查询客户端
			return Order.search;
		case 100:
			return Order.close;
		default:
			return null;
		}
	}

	// 监听回复
	public static Map<OrderSeq, List<OrderHeader>> orderResponse = new HashMap<OrderSeq, List<OrderHeader>>();

	public static class OrderSeq {
		public final int serialNumber;
		public final Order order;// 命令编码
		public boolean isRequest;// 1请求 0回复
		public final int hostNumber;

		protected OrderSeq(OrderHeader oh) {
			this.order = oh.order;
			this.isRequest = oh.isRequest;
			this.serialNumber = oh.serialNumber;
			this.hostNumber = oh.hostNumber;
		}

		public boolean equals(Object oth) {
			if (oth instanceof OrderSeq) {
				OrderSeq os = (OrderSeq) oth;
				return this.hostNumber == os.hostNumber && this.serialNumber == os.serialNumber &&
						this.order == os.order && this.isRequest == os.isRequest;
			} else {
				return false;
			}
		}

		public int hashCode() {
			return (this.hostNumber + "_" + this.serialNumber + "_" + this.order.ordinal() + "_" + this.isRequest)
					.hashCode();
		}

		public static OrderSeq createOrderSeq(OrderHeader oh) {
			return new OrderSeq(oh);
		}

		// 创建请求需要回复SEQ
		public static OrderSeq createRequestOrderSeq(OrderHeader oh) {
			if (!oh.isRequest)// 本身是请求命令
				return null;
			OrderSeq os = new OrderSeq(oh);
			os.isRequest = false;
			return os;
		}

		// 创建回复对应的请求SEQ
		public static OrderSeq createResponseOrderSeq(OrderHeader oh) {
			if (oh.isRequest == true)// 本身是回复命令
				return null;
			OrderSeq os = new OrderSeq(oh);
			os.isRequest = true;
			return os;
		}
	}

	public OrderHeader() {
		serialNumber = getOrderSerial();
		hostNumber = HostNumber;
	}

	private OrderHeader(int s) {
		serialNumber = s;
		this.hostNumber = HostNumber;
	}

	private OrderHeader(int s, int hostNumber) {
		serialNumber = s;
		this.hostNumber = hostNumber;
	}

	static byte[] readOrder(byte buffer, InputStream input) throws IOException {
		byte firstByte = buffer;
		int len = WritableUtils.decodeVIntSize(firstByte);
		int offset = 0;
		int vlen = 0;
		if (len == 1) {
			vlen = firstByte;
		} else {
			long i = 0;
			byte[] buf = new byte[len - 1];
			input.read(buf, 0, buf.length);
			for (int idx = 0; idx < len - 1; idx++) {
				byte b = buf[offset++];
				i = i << 8;
				i = i | (b & 0xFF);
			}
			vlen = (int) (WritableUtils.isNegativeVInt(firstByte) ? ~i : i);
		}
		byte[] data = new byte[vlen];
		input.read(data, 0, vlen);
		return data;
	}

	public static OrderHeader ParseHeader(byte buffer, InputStream input, DataInputBuffer inputBuffer)
			throws IOException {
		OrderHeader header = deserialize(GZIPUtils.unzip(readOrder(buffer, input)), inputBuffer);
		return header;
	}

	// public static OrderHeader ParseHeader(byte buffer, InputStream input)
	// throws IOException {
	// OrderHeader header = deserializeObject(readOrder(buffer, input));
	// return header;
	// }
	// byte[] toByteArray() {
	// byte[] arrData = serialObject(this);
	// byte[] vl = com.ery.estorm.util.Bytes.vintToBytes(arrData.length);
	// byte[] res = new byte[arrData.length + vl.length];
	// System.arraycopy(arrData, 0, res, 0, vl.length);
	// System.arraycopy(arrData, 0, res, vl.length, arrData.length);
	// return res;
	// }

	// public void sendHeader(OutputStream output) throws IOException {
	// byte[] arrData = serialObject(this);
	// byte[] vl = com.ery.estorm.util.Bytes.vintToBytes(arrData.length);
	// output.write(vl);
	// output.write(arrData);
	// output.flush();
	// }
	// public static void sendHeader(OutputStream output, OrderHeader header)
	// throws IOException {
	// header.sendHeader(output);
	// }
	public void sendHeader(OutputStream output, DataOutputBuffer buffer) throws IOException {
		byte[] arrData = serializable(buffer);
		arrData = GZIPUtils.zip(arrData, 0, buffer.getLength());
		buffer.reset();
		byte[] vl = com.ery.estorm.util.Bytes.vintToBytes(arrData.length);
		output.write(vl);
		output.write(arrData);
		output.flush();
	}

	private static OrderHeader deserialize(byte[] data, DataInputBuffer buffer) throws IOException {
		if (buffer == null)
			buffer = new DataInputBuffer();
		buffer.reset(data, data.length);
		int hostNumber = buffer.readInt(); // public final int hostNumber;
		int serialNumber = buffer.readInt(); // public final int serialNumber;
		OrderHeader oh = new OrderHeader(serialNumber, hostNumber);
		oh.order = IntToOrder(buffer.readByte()); // public Order order 命令编码
		oh.isRequest = buffer.readByte() != 0; // public boolean isRequest;//
												// 1请求 0回复
		short nt = buffer.readByte();
		oh.numTasks = (short) (nt << 8 & buffer.readByte()); // public short
																// numTasks;
		byte len = buffer.readByte();// 来源host
		if (len > 0) {
			byte[] buf = new byte[len];
			buffer.read(buf);
			oh.fromHost = new ServerInfo(new String(buf));
		}
		int dSize = buffer.readInt();
		for (int i = 0; i < dSize; i++) {
			byte[] buf = new byte[buffer.readByte()];
			buffer.read(buf);
			String key = new String(buf);
			Object value = DataSerializable.readFromBytes(buffer);
			oh.data.put(key, value);
		}
		return oh;
	}

	private byte[] serializable(DataOutputBuffer buffer) throws IOException {// 外部缓存
		if (buffer == null)
			buffer = new DataOutputBuffer();
		buffer.reset();
		buffer.writeInt(hostNumber);// public final int hostNumber;
		buffer.writeInt(serialNumber);// public final int serialNumber;
		buffer.writeByte(OrderToInt(order));// public Order order 命令编码
		buffer.writeByte(isRequest ? 1 : 0);// public boolean isRequest;// 1请求
											// 0回复
		buffer.writeByte((numTasks >> 8) & 0xFF);
		buffer.writeByte(numTasks & 0xFF);// public short numTasks;
		if (fromHost != null) {// // public ServerName fromHost = null;// 来源host
			buffer.writeByteString(fromHost.getHostAndPort());
		} else {
			buffer.writeByte(0);
		}
		buffer.writeInt(data.size());
		for (Map.Entry<String, Object> d : data.entrySet()) {
			buffer.writeByteString(d.getKey());
			DataSerializable.writeToBytes(d.getValue(), buffer);
		}
		return DataSerializable.getBufferData(buffer);
	}

	public static OrderHeader createPing() {
		OrderHeader ping = new OrderHeader(0);
		ping.order = Order.ping;
		return ping;
	}

	private static int SerialNumber = 0;
	private static int HostNumber = 0;

	public static int getCurrentSerialNumber() {
		return SerialNumber;
	}

	public static synchronized int getOrderSerial() {
		if (HostNumber == 0)
			HostNumber = hostName.hashCode();
		if (SerialNumber >= Integer.MAX_VALUE - 1 || SerialNumber < 0) {
			SerialNumber = 0;
		}
		return ++SerialNumber;
	}
}

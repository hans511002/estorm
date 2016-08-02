package com.ery.estorm.join;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.ery.estorm.ServerInfo;
import com.ery.estorm.client.node.JoinDataTree;
import com.ery.estorm.client.node.JoinDataTree.JoinDataTreeVal;
import com.ery.estorm.util.DataInputBuffer;
import com.ery.estorm.util.DataOutputBuffer;
import com.ery.estorm.util.GZIPUtils;

public class DataSerializable implements Serializable {

	private static final long serialVersionUID = 2748940480212654177L;
	byte[] row = null;

	public static byte[] Serializable(byte[][] row, DataOutputBuffer buffer) throws IOException {
		if (buffer == null)
			buffer = new DataOutputBuffer();
		buffer.reset();
		com.ery.estorm.util.WritableUtils.writeVInt(buffer, row.length);// 写入字段数
		for (byte[] col : row) {
			com.ery.estorm.util.WritableUtils.write(buffer, col);// 写入字段
		}
		return DataSerializable.getBufferData(buffer);
	}

	public static byte[][] Deserialization(byte[] row, DataInputBuffer buffer) throws IOException {
		if (buffer == null)
			buffer = new DataInputBuffer();
		buffer.reset(row, row.length);
		int colLen = com.ery.estorm.util.WritableUtils.readVInt(buffer);// 写入字段数

		byte[][] cols = new byte[colLen][];
		for (int i = 0; i < cols.length; i++) {
			cols[i] = com.ery.estorm.util.WritableUtils.read(buffer);
		}
		return cols;
	}

	public static Object readFromBytes(DataInputBuffer buffer) throws IOException {
		byte type = buffer.readByte();
		switch (type) {
		case 1: {
			int len = buffer.readInt();
			if (len > 0) {
				byte[] bt = new byte[len];
				buffer.read(bt);
				return new String(bt);
			} else {
				return "";
			}
		}
		case 2:
			return com.ery.estorm.util.WritableUtils.readVInt(buffer);
			// return buffer.readInt();
		case 4:
			// return buffer.readLong();
			return com.ery.estorm.util.WritableUtils.readVLong(buffer);
		case 3:
			return buffer.readDouble();
		case 8:
			return new Date(buffer.readLong());
		case 9: {
			List<Object> list = new LinkedList<Object>();
			int len = buffer.readInt();
			for (int i = 0; i < len; i++) {
				list.add(readFromBytes(buffer));
			}
			return list;
		}
		case 15: {
			Map map = new HashMap();
			int len = buffer.readInt();
			for (int i = 0; i < len; i++) {
				map.put(readFromBytes(buffer), readFromBytes(buffer));
			}
			return map;
		}
		case 71: {
			int len = buffer.readByte();
			byte[] buf = new byte[len];
			buffer.read(buf);
			return new ServerInfo(new String(buf));
		}
		case 81: {
			JoinDataTree jdt = new JoinDataTree();
			int dlen = buffer.readByte();
			jdt.joinDataRel = new HashMap<Long, JoinDataTree.JoinDataTreeVal>();
			for (int i = 0; i < dlen; i++) {
				jdt.joinDataRel.put((Long) readFromBytes(buffer), (JoinDataTree.JoinDataTreeVal) readFromBytes(buffer));
			}
		}
		case 82: {
			JoinDataTreeVal jdtv = new JoinDataTreeVal();
			byte b = buffer.readByte();
			jdtv.inited = b == 127;
			short len = buffer.readShort();
			jdtv.value = new byte[len][];
			for (int i = 0; i < len; i++) {
				int vl = buffer.readShort();
				if (vl > 0) {
					jdtv.value[i] = new byte[vl];
					buffer.read(jdtv.value[i]);
				}
			}
			return jdtv;
		}
		case 127:
			return true;
		case -1:
			return false;
		case -2: {
			int len = buffer.readInt();
			if (len > 0) {
				byte[] buf = new byte[len];
				buffer.read(buf);
				return deserializeObject(buf);
			} else {
				return null;
			}
		}
		case 0:
		default:
			return null;
		}
	}

	@SuppressWarnings("unchecked")
	public static void writeToBytes(Object value, DataOutputBuffer destBuffer) throws IOException {
		if (value == null) {
			destBuffer.writeByte(0);
			return;
		}
		if (value instanceof String) {
			destBuffer.writeByte(1);
			byte[] bt = ((String) value).getBytes();
			destBuffer.writeInt(bt.length);
			destBuffer.write(bt);
		} else if (value instanceof Integer) {
			destBuffer.writeByte(2);
			// byte[] bt = Bytes.toBytes((Integer) value);
			// destBuffer.write(bt);
			com.ery.estorm.util.WritableUtils.writeVLong(destBuffer, (Integer) value);
		} else if (value instanceof Boolean) {
			destBuffer.writeByte(((Boolean) value) ? 127 : -1);// 127 255
		} else if (value instanceof Double) {
			destBuffer.writeByte(3);
			destBuffer.writeDouble((Double) value);
		} else if (value instanceof Long) {
			destBuffer.writeByte(4);
			com.ery.estorm.util.WritableUtils.writeVLong(destBuffer, (Long) value);
			// destBuffer.writeLong(((Long) value));
		} else if (value instanceof Date) {
			destBuffer.writeByte(8);
			destBuffer.writeLong((((Date) value).getTime()));
		} else if (value instanceof List) {
			destBuffer.writeByte(9);
			List list = (List) value;
			destBuffer.writeInt(list.size());
			for (Object obj : list) {
				writeToBytes(obj, destBuffer);
			}
		} else if (value instanceof Map) {
			destBuffer.writeByte(15);
			Map map = (Map) value;
			destBuffer.writeInt(map.size());
			for (Object key : map.keySet()) {
				writeToBytes(key, destBuffer);
				writeToBytes(map.get(key), destBuffer);
			}
		} else if (value instanceof ServerInfo) {
			destBuffer.writeByte(71);
			destBuffer.writeByteString(((ServerInfo) value).getHostAndPort());
		} else if (value instanceof JoinDataTree) {// 节点 记录关联数据
			destBuffer.writeByte(81);
			JoinDataTree jdt = (JoinDataTree) value;
			destBuffer.writeByte(jdt.joinDataRel == null ? 0 : jdt.joinDataRel.size());// 不超过255个关联
			if (jdt.joinDataRel != null && jdt.joinDataRel.size() > 0) {
				for (Long key : jdt.joinDataRel.keySet()) {
					writeToBytes(key, destBuffer);
					writeToBytes(jdt.joinDataRel.get(key), destBuffer);
				}
			}
		} else if (value instanceof JoinDataTreeVal) {// 节点 记录关联数据
			destBuffer.writeByte(82);
			JoinDataTreeVal jdtv = (JoinDataTreeVal) value;
			destBuffer.writeByte((jdtv.inited) ? 127 : -1);// 127 255

			destBuffer.writeShort(jdtv.value.length);
			for (byte[] col : jdtv.value) {
				if (col != null) {
					destBuffer.writeShort(col.length);
					if (col.length > 0)
						destBuffer.write(col);
				} else {
					destBuffer.writeShort(0);
				}
			}
		} else {
			destBuffer.writeByte(-2);
			byte[] bt = serialObject(value);
			if (bt != null) {
				destBuffer.writeInt(bt.length);
				destBuffer.write(bt);
			} else {
				destBuffer.writeInt(0);
			}
		}
	}

	public static <T> T deserializeObject(byte[] bts) throws IOException {
		bts = GZIPUtils.unzip(bts);
		ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bts);
		ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream);
		try {
			return (T) objectInputStream.readObject();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			throw new IOException(e);
		} finally {
			objectInputStream.close();
			byteArrayInputStream.close();
		}
	}

	public static byte[] serialObject(Object obj) {
		try {
			ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
			ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
			objectOutputStream.writeObject(obj);
			byte[] bts = GZIPUtils.zip(byteArrayOutputStream.toByteArray());
			objectOutputStream.close();
			byteArrayOutputStream.close();
			return bts;
		} catch (Exception e) {
			return null;
		}
	}

	public static byte[] getBufferData(DataOutputBuffer buffer) {
		if (buffer.getLength() == buffer.getData().length) {
			return buffer.getData();
		} else {
			return Arrays.copyOf(buffer.getData(), buffer.getLength());
		}
	}
}

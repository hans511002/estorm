package com.ery.estorm.client.node;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.ery.estorm.daemon.topology.NodeInfo.FieldPO;
import com.ery.estorm.util.Bytes;
import com.ery.estorm.util.DataOutputBuffer;
import com.ery.estorm.util.ToolUtil;

public class KafkaMsgScheme implements Scheme {
	private static final long serialVersionUID = 1748313428597836562L;
	FieldPO[] nodeOutFields;

	public KafkaMsgScheme(FieldPO[] nodeOutFields) {
		this.nodeOutFields = nodeOutFields;
	}

	@Override
	public List<Object> deserialize(byte[] bytes) {
		try {
			// if(mpo.nodeOutFields.length)
			// String strVal = new String(bytes, "UTF-8");
			// List<Object> l = EStormConstant.objectMapper.readValue(strVal, List.class);
			// mpo = EStormConstant.castObject(bytes);
			// Object val[] = new Object[mpo.nodeOutFields.length];
			// for (int i = 0; i < val.length; i++) {
			// val[i] = mpo.nodeOutFields[i].FIELD_NAME;
			// }
			// return new Values(l.toArray());
			List<Object> row = new ArrayList<Object>();
			int offset = 0;
			int filedLen = 0;
			while (offset < bytes.length) {
				FieldPO nfpo = nodeOutFields[filedLen++];
				switch (nfpo.filedType) {
				case 1: {
					int len = Bytes.toInt(bytes, offset);
					offset += 4;
					if (bytes.length >= offset + len) {
						row.add(new String(bytes, offset, len));
						offset += len;
					} else
						row.add("");
					break;
				}
				case 2:
				case 8: {
					row.add(Bytes.toLong(bytes, offset));
					offset += Bytes.SIZEOF_LONG;
					break;
				}
				case 4: {
					row.add(Bytes.toDouble(bytes, offset));
					offset += Bytes.SIZEOF_DOUBLE;
					break;
				}
				default: {
					int len = Bytes.toInt(bytes, offset);
					offset += 4;
					if (bytes.length >= offset + len) {
						row.add(new String(bytes, offset, len));
						offset += len;
					} else
						row.add("");
					break;
				}
				}
				if (filedLen >= nodeOutFields.length)
					break;
			}
			return new Values(row.toArray());
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public Fields getOutputFields() {
		List<String> val = new ArrayList<String>();
		// Fields fds=new Fields(List<String> fields)
		for (FieldPO f : nodeOutFields) {
			val.add(f.FIELD_NAME);
		}
		// String val[] = new String[mpo.nodeOutFields.length];
		// for (int i = 0; i < val.length; i++) {
		// val[i] = mpo.nodeOutFields[i].FIELD_NAME;
		// }
		return new Fields(val);
	}

	ByteBuffer buf = null;

	public byte[] serialize(Object[] data) {
		try {
			DataOutputBuffer buf = new DataOutputBuffer(1024);
			buf.reset();
			for (int i = 0; i < data.length; i++) {
				FieldPO nfpo = nodeOutFields[i];
				switch (nfpo.filedType) {
				case 1: {
					byte[] bt = data[i].toString().getBytes();
					buf.writeInt(bt.length);
					buf.write(bt);
					break;
				}
				case 2:
				case 8: {
					if (data[i] instanceof Long) {
						buf.writeLong((Long) data[i]);
					} else {
						buf.writeLong(ToolUtil.toLong(data[i].toString()));
					}
					break;
				}
				case 4: {
					if (data[i] instanceof Double) {
						buf.writeDouble((Double) data[i]);
					} else {
						buf.writeDouble(ToolUtil.toDouble(data[i].toString()));
					}
					break;
				}
				default: {
					byte[] bt = data[i].toString().getBytes();
					buf.writeInt(bt.length);
					buf.write(bt);
					break;
				}
				}
			}
			return Arrays.copyOf(buf.getData(), buf.getLength());
			// return buf.getData();
		} catch (IOException e) {
			return null;
		}

	}
}
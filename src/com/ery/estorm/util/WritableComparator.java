/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ery.estorm.util;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;

/**
 * A Comparator for {@link WritableComparable}s.
 * 
 * <p>
 * This base implemenation uses the natural ordering. To define alternate orderings, override
 * {@link #compare(WritableComparable,WritableComparable)}.
 * 
 * <p>
 * One may optimize compare-intensive operations by overriding {@link #compare(byte[],int,int,byte[],int,int)}. Static utility methods are
 * provided to assist in optimized implementations of this method.
 */
public class WritableComparator implements Comparator {

	private static HashMap<Class, WritableComparator> comparators = new HashMap<Class, WritableComparator>(); // registry

	/** Get a comparator for a {@link WritableComparable} implementation. */
	public static synchronized WritableComparator get(Class<? extends Comparable> c) {
		WritableComparator comparator = comparators.get(c);
		if (comparator == null)
			comparator = new WritableComparator(c, true);
		return comparator;
	}

	/**
	 * Register an optimized comparator for a {@link WritableComparable} implementation.
	 */
	public static synchronized void define(Class c, WritableComparator comparator) {
		comparators.put(c, comparator);
	}

	private final Class<? extends Comparable> keyClass;
	private final Comparable key1;
	private final Comparable key2;
	private final DataInputBuffer buffer;

	/** Construct for a {@link WritableComparable} implementation. */
	protected WritableComparator(Class<? extends Comparable> keyClass) {
		this(keyClass, false);
	}

	protected WritableComparator(Class<? extends Comparable> keyClass, boolean createInstances) {
		this.keyClass = keyClass;
		if (createInstances) {
			key1 = newKey();
			key2 = newKey();
			buffer = new DataInputBuffer();
		} else {
			key1 = key2 = null;
			buffer = null;
		}
	}

	/** Returns the WritableComparable implementation class. */
	public Class<? extends Comparable> getKeyClass() {
		return keyClass;
	}

	/** Construct a new {@link WritableComparable} instance. */
	public Comparable newKey() {
		return ReflectionUtils.newInstance(keyClass, null);
	}

	/**
	 * Compare two WritableComparables.
	 * 
	 * <p>
	 * The default implementation uses the natural ordering, calling {@link Comparable#compareTo(Object)}.
	 */
	@SuppressWarnings("unchecked")
	public int compare(Comparable a, Comparable b) {
		return a.compareTo(b);
	}

	public int compare(Object a, Object b) {
		return compare((Comparable) a, (Comparable) b);
	}

	/** Lexicographic order of binary data. */
	public static int compareBytes(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
		int end1 = s1 + l1;
		int end2 = s2 + l2;
		for (int i = s1, j = s2; i < end1 && j < end2; i++, j++) {
			int a = (b1[i] & 0xff);
			int b = (b2[j] & 0xff);
			if (a != b) {
				return a - b;
			}
		}
		return l1 - l2;
	}

	/** Compute hash for binary data. */
	public static int hashBytes(byte[] bytes, int offset, int length) {
		int hash = 1;
		for (int i = offset; i < offset + length; i++)
			hash = (31 * hash) + (int) bytes[i];
		return hash;
	}

	/** Compute hash for binary data. */
	public static int hashBytes(byte[] bytes, int length) {
		return hashBytes(bytes, 0, length);
	}

	/** Parse an unsigned short from a byte array. */
	public static int readUnsignedShort(byte[] bytes, int start) {
		return (((bytes[start] & 0xff) << 8) + ((bytes[start + 1] & 0xff)));
	}

	/** Parse an integer from a byte array. */
	public static int readInt(byte[] bytes, int start) {
		return (((bytes[start] & 0xff) << 24) + ((bytes[start + 1] & 0xff) << 16) + ((bytes[start + 2] & 0xff) << 8) + ((bytes[start + 3] & 0xff)));

	}

	/** Parse a float from a byte array. */
	public static float readFloat(byte[] bytes, int start) {
		return Float.intBitsToFloat(readInt(bytes, start));
	}

	/** Parse a long from a byte array. */
	public static long readLong(byte[] bytes, int start) {
		return ((long) (readInt(bytes, start)) << 32) + (readInt(bytes, start + 4) & 0xFFFFFFFFL);
	}

	/** Parse a double from a byte array. */
	public static double readDouble(byte[] bytes, int start) {
		return Double.longBitsToDouble(readLong(bytes, start));
	}

	/**
	 * Reads a zero-compressed encoded long from a byte array and returns it.
	 * 
	 * @param bytes
	 *            byte array with decode long
	 * @param start
	 *            starting index
	 * @throws java.io.IOException
	 * @return deserialized long
	 */
	public static long readVLong(byte[] bytes, int start) throws IOException {
		int len = bytes[start];
		if (len >= -112) {
			return len;
		}
		boolean isNegative = (len < -120);
		len = isNegative ? -(len + 120) : -(len + 112);
		if (start + 1 + len > bytes.length)
			throw new IOException("Not enough number of bytes for a zero-compressed integer");
		long i = 0;
		for (int idx = 0; idx < len; idx++) {
			i = i << 8;
			i = i | (bytes[start + 1 + idx] & 0xFF);
		}
		return (isNegative ? (i ^ -1L) : i);
	}

	/**
	 * Reads a zero-compressed encoded integer from a byte array and returns it.
	 * 
	 * @param bytes
	 *            byte array with the encoded integer
	 * @param start
	 *            start index
	 * @throws java.io.IOException
	 * @return deserialized integer
	 */
	public static int readVInt(byte[] bytes, int start) throws IOException {
		return (int) readVLong(bytes, start);
	}
}

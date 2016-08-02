package com.ery.estorm.util.DJudge;

import java.io.IOException;

import com.ery.estorm.util.DJudge.HashTable.HashNode;

public interface JudgeHash {
	public boolean addNode(HashNode node) throws IOException;

	public boolean contains(HashNode node) throws IOException;

	public void close();

	public void clear();
}

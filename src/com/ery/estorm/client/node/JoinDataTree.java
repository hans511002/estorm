package com.ery.estorm.client.node;

import java.util.Map;

import com.ery.estorm.client.node.BoltNode.JoinTree;
import com.ery.estorm.daemon.topology.NodeInfo.NodeJoinPO;

public class JoinDataTree {
	public static class JoinDataTreeVal {
		public boolean inited = false;
		public byte[][] value;
		public NodeJoinPO jpo = null;
	}

	public Map<Long, JoinDataTreeVal> joinDataRel;

	public void ComJoinDataTree(JoinDataTree other) {// 通过合并关联上jpo
		for (Long joinId : joinDataRel.keySet()) {
			JoinDataTreeVal jdtv = joinDataRel.get(joinId);
			JoinDataTreeVal ojdtv = other.joinDataRel.get(joinId);
			jdtv.jpo = ojdtv.jpo != null ? ojdtv.jpo : jdtv.jpo;
			if (jdtv.inited == false && ojdtv.inited == true) {
				jdtv.inited = true;
				jdtv.value = ojdtv.value;
			}
		}
	}

	public void initJpo(Map<Long, JoinTree> joinRels) {
		for (Long joinId : this.joinDataRel.keySet()) {
			this.joinDataRel.get(joinId).jpo = joinRels.get(joinId).joinPo;
		}
	}
}

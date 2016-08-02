package storm.kafka;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

/**
 * Copyrights @ 2013,Tianyuan DIC Information Co.,Ltd. All rights reserved.
 * 
 * @author wangcs
 * @description
 * @date 13-12-6 -
 * @modify
 * @modifyDate -
 */
public class PrinterBolt extends BaseBasicBolt {

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		System.out.print("【" + tuple.size() + ":");
		for (Object o : tuple.getValues()) {
			System.out.print(o + "  ");
		}
		System.out.print("】\n");
	}

}

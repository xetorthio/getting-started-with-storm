package bolts;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class WordNormalizer implements IRichBolt {

	private OutputCollector collector;

	public void cleanup() {}

	public void execute(Tuple input) {
		String word = input.getString(0);
		word = word.trim();
		word = word.toLowerCase();
		String[] words = word.split(" ");
		for(String str : words){
			str = str.trim();
			if(!"".equals(str)){
				collector.emit(new Values(str));
				collector.ack(input);
			}
		}
	}

	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

}

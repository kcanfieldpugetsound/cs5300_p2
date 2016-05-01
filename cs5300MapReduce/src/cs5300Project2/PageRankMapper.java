package cs5300Project2;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PageRankMapper extends Mapper<Text, Text, LongWritable, Text> {
	
	private LongWritable outKey = new LongWritable();
	private Text outValue = new Text();

	public void map(Text ikey, Text ivalue, Context context) throws IOException, InterruptedException {
		
		if (ivalue.toString().equals(""))
			return;
		
		Node n = new Node(ivalue.toString());
		
		//write page rank for outgoing nodes
		double outgoingPageRank = 
			n.getCurrPageRank() / n.getAdjacencyList().size();
		for (Pair<Integer,Integer> p : n.getAdjacencyList()) {
			//p consists of <blockId, nodeId>, we don't care about blockId
			Integer outgoingNodeId = p.right();
			
			outKey.set(outgoingNodeId);
			outValue.set("P" + outgoingPageRank);
			context.write(outKey, outValue);
		}
		
		//write ourselves!
		outKey.set(n.nodeId());
		outValue.set("N" + n.toString());		
	}
}

package cs5300Project2;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PageRankReducer extends Reducer<LongWritable, Text, Text, Text> {
	
	private static final double ALPHA = 0.85;
	public static String CONF_NUM_NODES = "pagerank.numnodes";
	private int numNodes; 
	public static final long SCALING_FACTOR = 10000;
	
	public static enum Counter{
		CONVERGENCE
	}
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException{
		numNodes = context.getConfiguration().getInt(CONF_NUM_NODES, 0);
	}
	
	//private Text outValue = new Text();

	public void reduce(LongWritable _key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		Text outKey = new Text();
		Text outValue = new Text();
		
		Integer nodeId = new Integer(_key.toString());
		
		//process incoming data
		
		Node n;
		List<Double> incomingPageRanks = new ArrayList<Double>();
		for (Text t : values) {
			String text = t.toString();
			char type = text.charAt(0);
			String contents = text.substring(1);
			
			switch (type) {
			case 'N':
				n = new Node(contents);
				break;
			case 'P':
				incomingPageRanks.add(new Double(contents));
			}
		}
		
		//perform page rank algorithm
		
		//shift currPageRank -> prevPageRank
		n.setPrevPageRank(n.getCurrPageRank);
		n.setCurrPageRank(0);
		
		//add page ranks to node
		double oldPR;
		double newPR;
		for (Double pr : incomingPageRanks) {
			oldPR = n.getCurrPageRank();
			newPR = oldPR + pr;
			n.setCurrPageRank(newPR);
		}
		
		//apply damping factor
		newPR = ((1 - ALPHA) / numNodes)) + (ALPHA * newPR);
		
		//calculate convergence
		long delta = (long) 
			(SCALING_FACTOR * 
				(Math.abs(n.getPrevPageRank() - n.getCurrPageRank()) / n.getCurrPageRank()));  
		
		context.getCounter(Counter.CONVERGENCE).increment(delta);
		
		outValue.set(graph);
		
		//write results
		outKey.set(n.nodeId());
		outValue.set(n.toString());
		
		context.write(outKey, outValue);
		
	}

}

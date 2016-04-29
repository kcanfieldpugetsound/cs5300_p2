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
		Text outValue = new Text();
		
		double newPageRank = 0.0;
		
		double oldPageRank = 0.0;
		
		String graph = "";
		
		for (Text val : values) {
			String input = val.toString();
			
			if (input.substring(0, 1).equals("G")){
				
				String[] graphData = input.substring(1).split("#");
				graph += graphData[0] + "#" + graphData[1] + "#" + graphData[3];
				oldPageRank = Double.parseDouble(graphData[3]);
				
				
			}
			else if (input.substring(0, 1).equals("P")){
				newPageRank += Double.parseDouble(input.substring(1));
			}
		}
			
			newPageRank = (1.0 - ALPHA) / numNodes + ALPHA * newPageRank;
			
			graph += "#" + newPageRank;
			
			long delta = (long)(SCALING_FACTOR * Math.abs(oldPageRank - newPageRank) / newPageRank);  
			
			context.getCounter(Counter.CONVERGENCE).increment(delta);
			
			outValue.set(graph);
			
			Text outkey = new Text();
			outkey.set("" + _key.get());
			
			context.write(outkey, outValue);
			
			

		
	}

}

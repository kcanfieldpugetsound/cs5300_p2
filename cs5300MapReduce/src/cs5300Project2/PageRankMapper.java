package cs5300Project2;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PageRankMapper extends Mapper<Text, Text, LongWritable, Text> {
	
	private LongWritable outKey = new LongWritable();
	private Text outValue = new Text();

	
	/**
	 * This method takes the input file/data, reads through line by line, and processes each line.
	 * Each line is in the format "nodeId"<one tab>nodeId#$child1,child2,child3,...,childN#prevPageRank#currPageRank
	 * the adjacency list can be empty. 
	 * 
	 * We first emit the key and the entire line(putting in a label to say it is graph data), so that the graph data is maintained.
	 * Then we check the adjacency list. If it is empty, then we are done with this input. Otherwise, we go through each child node,  
	 * and output <childNode, parentPageRank / parentNumChildren>. 
	 */
	public void map(Text ikey, Text ivalue, Context context) throws IOException, InterruptedException {
		
		if (ivalue.toString().equals(""))
			return;
		
		//System.out.println("running a pass");
		String[] nodeData = ivalue.toString().split("#");
		
		String graphData = "G" + ivalue.toString();
		
		//System.out.println("input value is " + ivalue.toString());
		
		outKey.set(Long.parseLong(ikey.toString()));
		outValue.set(graphData);
		
		context.write(outKey, outValue);
		
		double pr = Double.parseDouble(nodeData[3]);
		
		if (nodeData[1].length() > 0){
			String [] childNodes = nodeData[1].split(",");
			
			for (int i = 0; i < childNodes.length; i++){
				outKey.set(Long.parseLong(childNodes[i]));
				outValue.set("P" + (pr / childNodes.length));
				context.write(outKey, outValue);
			}
				
		}
		 
		
	}
}

package cs5300Project2;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PageRankMapper extends Mapper<Text, Text, Text, Text> {
	
	private Text outKey = new Text(), outValue = new Text();

	public void map(Text ikey, Text ivalue, Context context) throws IOException, InterruptedException {
		
		String[] nodeData = ivalue.toString().split("#");
		
		String graphData = "G" + ivalue.toString();
		
		outKey.set(nodeData[0]);
		outValue.set(graphData);
		
		context.write(outKey, outValue);
		
		Integer node = Integer.parseInt(nodeData[0]);
		
		double pr = Double.parseDouble(nodeData[3]);
		
		ArrayList<String> adjNodes = new ArrayList<String>();
		
		if (nodeData[1].length() > 0){
			String [] childNodes = nodeData[1].split(",");
			
			for (int i = 0; i < childNodes.length; i++){
				outKey.set(childNodes[i]);
				outValue.set("P" + (pr / childNodes.length));
				context.write(outKey, outValue);
			}
				
		}
		
		
		
	}

}

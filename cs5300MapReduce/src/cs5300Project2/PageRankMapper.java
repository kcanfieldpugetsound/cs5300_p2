package cs5300Project2;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PageRankMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
	
	private LongWritable outKey = new LongWritable();
	private Text outValue = new Text();

	public void map(LongWritable ikey, Text ivalue, Context context) throws IOException, InterruptedException {
		System.out.println("running a pass");
		String[] nodeData = ivalue.toString().split("#");
		
		String graphData = "G" + ivalue.toString();
		
		outKey.set(ikey.get());
		outValue.set(graphData);
		
		context.write(outKey, outValue);
		
		Integer node = Integer.parseInt(nodeData[0]);
		
		double pr = Double.parseDouble(nodeData[3]);
		
		ArrayList<String> adjNodes = new ArrayList<String>();
		
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

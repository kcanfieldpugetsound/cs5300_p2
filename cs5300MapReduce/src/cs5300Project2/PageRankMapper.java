package cs5300Project2;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PageRankMapper extends Mapper<Text, Text, Text, Text> {
	
	private Text outKey = new Text();
	private Text outValue = new Text();

	public void map(Text ikey, Text ivalue, Context context) throws IOException, InterruptedException {
		
		if (ivalue.toString().equals(""))
			return;
		
		//System.out.println("running a pass");
		String[] nodeData = ivalue.toString().split("#");
		
		String graphData = "G" + ivalue.toString();
		
		outKey.set(ikey.toString());
		outValue.set(graphData);
		
		context.write(outKey, outValue);
		
		double pr = Double.parseDouble(nodeData[3]);
		
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

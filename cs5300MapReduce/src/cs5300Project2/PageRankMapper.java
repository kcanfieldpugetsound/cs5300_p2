package cs5300Project2;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PageRankMapper extends Mapper<Text, Text, Text, Text> {

	public void map(Text ikey, Text ivalue, Context context) throws IOException, InterruptedException {

	}

}

package cs5300Project2;

import java.io.File;
import java.io.PrintWriter;
import java.util.Map.Entry;
import java.util.Scanner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Main {
	
	private static long numEdges = 0;

	public static void main(String[] args) throws Exception{
		
		if (args[0].equals("run") && args.length == 4)
		pageRank(args[1], args[2], args[3]);
		else if (args[0].equals("pre") && args.length == 4){
			Path output = new Path(args[2]);
			File formattedInputFile =  new File(output.toString() + "/input.txt");
			createFormattedFile(args[1], formattedInputFile, Integer.parseInt(args[3]));
		}
		else{
			System.err.println("Usage: Use args <pre inputFile outputDirectory totalNumberofNodes> to preprocess edges.txt or your input file");
			System.err.println("Usage: Use args <run outputDirectory numIterations totalNumberOfNodes> to run mapreduce on a preprocessed file");
			System.err.println("Note: If you were to do a run of y iterations, then a run of x (x < y) iterations, "
					+ "the output folders a for x < a <= y will remain in the output directory unless manually deleted.");
			
		}
		
	}
	
	

	
	private static void pageRank(String outputDir, String numRuns, String numberofnodes) throws Exception{
		Configuration config = new Configuration();
		System.out.println("configuring to run mapreduce");
		Path output = new Path(outputDir);
		//output.getFileSystem(config).delete(output, true);
		//output.getFileSystem(config).mkdirs(output);
		
		
		Path input = new Path(output, "input.txt");
		
		int maxRuns = Integer.parseInt(numRuns);
		
		//File formattedInputFile = new File(output.toString() + "/input.txt");
		
		int totalNodes = Integer.parseInt(numberofnodes);
		
		
		//if (shouldPreprocess.equals("1"))
		//createFormattedFile(inputFile, formattedInputFile, totalNodes);
		
		//System.out.println(input.toString());
		double converged = 0.001;
		
		File convergenceFile = new File("convergence.txt");
		
		if (!convergenceFile.exists())
			convergenceFile.createNewFile();
		
		
		
		PrintWriter logger = new PrintWriter(convergenceFile);
		logger.println("Number of edges is " + numEdges);
		int currentIteration = 1;
		System.out.println("finshed configuration. Starting mapreduce.");
		while(currentIteration <= maxRuns){
			Path jobOutput = new Path(output, String.valueOf(currentIteration));
			jobOutput.getFileSystem(config).delete(jobOutput, true);
			
			double convValue = runPageRank(input, jobOutput, totalNodes);
			
			if (convValue < converged){
				System.out.println("We have converged below " + converged + " on iteration " + currentIteration);
				//break;
			}
			logger.println("Convergence on run " + currentIteration + ": " + convValue);
			
			input = jobOutput;
			currentIteration++;
		}
		
		logger.close();
		
		
		
	}

	private static double runPageRank(Path input, Path jobOutput, int numNodes) throws Exception {
		Configuration config = new Configuration();
		config.setInt(PageRankReducer.CONF_NUM_NODES, numNodes);
		Job job = Job.getInstance(config, "NaivePageRank");
		job.setJarByClass(Main.class);
		job.setMapperClass(PageRankMapper.class);
		job.setReducerClass(PageRankReducer.class);
		
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, input);
		FileOutputFormat.setOutputPath(job, jobOutput);
		
		org.apache.log4j.BasicConfigurator.configure();
		
		if (!job.waitForCompletion(true)){
			throw new Exception("The job has failed to complete");
		}
		
		long scaledConvergence = job.getCounters().findCounter(PageRankReducer.Counter.CONVERGENCE).getValue();
		
		return ((double) scaledConvergence / (double) PageRankReducer.SCALING_FACTOR / (double) numNodes);
		
		//return conv;
		
		
	}

	private static void createFormattedFile(String inputFile, File formattedInputFile, int totalNodes) throws Exception{
		File file = new File(inputFile);
		
		Scanner sc = new Scanner(file);
		
		Graph graph = new Graph(totalNodes);
		
		
		String[] stringArray = null;
		
		double netId = .644;
		double lowerBound = netId * 0.9;
		double upperBound = lowerBound + 0.01;
		Integer source = null, destination = null;
		
		while (sc.hasNextLine()){
			stringArray = sc.nextLine().trim().split("\\s+");
			
			if (stringArray.length != 3)
				continue;
			
			if (!(lowerBound <= Double.valueOf(stringArray[2]).doubleValue()
					&& Double.valueOf(stringArray[2]).doubleValue() <= upperBound)){
				numEdges++;
				source = Integer.valueOf(stringArray[0]);
				destination = Integer.valueOf(stringArray[1]);
				graph.addEdge(source, destination);
				
			}
		}
		
		sc.close();
		System.out.println("loaded graph into memory");
		
		//Integer maxKey = graph.getGraph().lastKey();
		
		for (int i = 0; i < totalNodes; i++){
			graph.addSinkNode(new Integer(i));
		}
		
		System.out.println("added sinks to memory (if any).");
		
		int numNodes = graph.numNodes();
		
		if (numNodes <= 0)
			throw new RuntimeException("file is empty");
		
		double dummyPR = (double)Integer.MIN_VALUE;
		
		double startPR = (1.0) / (double) numNodes;
		
		//File writeToFile = new File(formattedInput.toString());
		
		//System.out.println(writeToFile.getParentFile().exists());
		
		if (!formattedInputFile.exists()){
			//swriteToFile.mkdirs();
			formattedInputFile.createNewFile();
		}
			
		PrintWriter writer = new PrintWriter(formattedInputFile);
		String result = null;
		
		
		for (Entry<Integer, AdjacencyList> entry : graph.getGraph().entrySet()){		
			result = entry.getKey().toString() + "\t" + entry.getKey().toString()
					+ "#" + entry.getValue().toString() + "#" + dummyPR + "#" + startPR;
			writer.println(result);
		}
		
		writer.close();
		System.out.println("finished formatting file");
		//return graph.getGraph().entrySet().size();

	}
	
	
	
	
}

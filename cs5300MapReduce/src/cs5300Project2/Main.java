package cs5300Project2;

import java.io.File;
import java.io.PrintWriter;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class Main {

	public static void main(String[] args) throws Exception{
		pageRank(args[0], args[1]);
	}

	private static void pageRank(String inputFile, String outputDir) throws Exception{
		Configuration config = new Configuration();
		Path output = new Path(outputDir);
		output.getFileSystem(config).delete(output, true);
		output.getFileSystem(config).mkdirs(output);
		
		Path input = new Path(output, "formattedInput.txt");
		
		
		
		File formattedInputFile = new File(output.toString() + "/formattedInput.txt");
		
		createFormattedFile(inputFile, formattedInputFile);
		
		//System.out.println(input.toString());
		
		
		
	}

	private static void createFormattedFile(String inputFile, File formattedInputFile) throws Exception{
		File file = new File(inputFile);
		
		Scanner sc = new Scanner(file);
		
		Graph graph = new Graph();
		
		
		String[] stringArray = null;
		
		double netId = .644;
		double lowerBound = netId * 0.9;
		double upperBound = lowerBound + 0.01;
		Integer source = null, destination = null;
		
		while (sc.hasNextLine()){
			stringArray = sc.nextLine().split("\\s+");
			
			if (!(lowerBound <= Double.valueOf(stringArray[3]).doubleValue()
					&& Double.valueOf(stringArray[3]).doubleValue() <= upperBound)){
				source = Integer.valueOf(stringArray[1]);
				destination = Integer.valueOf(stringArray[2]);
				graph.addEdge(source, destination);
				
			}
		}
		
		Integer maxKey = graph.getGraph().lastKey();
		
		for (int i = 0; i <= maxKey.intValue(); i++){
			graph.addSinkNode(new Integer(i));
		}
		
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
			result = entry.getKey().toString() + "#" + entry.getValue().toString() + "#" + dummyPR + "#" + startPR;
			writer.println(result);
		}
		
		writer.close();

	}
	
	
	
	
}

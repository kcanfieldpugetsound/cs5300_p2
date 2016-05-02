package cs5300Project2;

import java.util.ArrayList;

public class AdjacencyList {
	
	
	/**
	 * This class stores the child nodes of the parent. This is the Value for the hashmap stored in the Graph class.
	 */
	private ArrayList<Integer> adjacencies = new ArrayList<Integer>();
	
	public AdjacencyList(){
		
	}
	
	/**
	 * 
	 * @param destination This is a new destination node to add to the list
	 */
	public void add(Integer destination){
		adjacencies.add(destination);
	}
	
	
	/**
	 * 
	 * @return the current adjacency list
	 */
	public ArrayList<Integer> getList(){
		return adjacencies;
	}
	
	
	/**
	 * This method is used to print out to the formatted file
	 */
	public String toString(){
		String result  = "";
		
		for (Integer dest : adjacencies)
			result += dest.toString() + ",";
		
		
		if (result.length() > 0)
			result = result.substring(0, result.length() - 1);
		
		return result;
	}

}

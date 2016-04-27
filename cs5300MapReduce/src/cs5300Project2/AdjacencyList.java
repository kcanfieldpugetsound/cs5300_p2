package cs5300Project2;

import java.util.ArrayList;

public class AdjacencyList {
	
	private ArrayList<Integer> adjacencies = new ArrayList<Integer>();
	
	public AdjacencyList(){
		
	}
	
	public void add(Integer destination){
		adjacencies.add(destination);
	}
	
	public ArrayList<Integer> getList(){
		return adjacencies;
	}
	
	public String toString(){
		String result  = "";
		
		for (Integer dest : adjacencies)
			result += dest.toString() + ",";
		
		
		if (result.length() > 0)
			result = result.substring(0, result.length() - 1);
		
		return result;
	}

}

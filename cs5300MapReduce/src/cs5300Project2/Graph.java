package cs5300Project2;

import java.util.HashMap;

public class Graph {
	
	private HashMap<Integer, AdjacencyList> graph;
	
	public Graph(int totalNodes){
		graph = new HashMap<Integer, AdjacencyList>(totalNodes); 
	}
	
	public void addEdge(Integer source, Integer destination){
		if (graph.containsKey(source)){
			graph.get(source).add(destination);
		}
		else{
			AdjacencyList aList = new AdjacencyList();
			aList.add(destination);
			graph.put(source, aList);
		}
	}
	
	public int numNodes(){
		return graph.keySet().size();
	}
	
	public HashMap<Integer, AdjacencyList> getGraph(){
		return graph;
	}
	
	public void addSinkNode(Integer node){
		
		if (graph.containsKey(node))
			return;
		else{
			AdjacencyList aList = new AdjacencyList();
			graph.put(node, aList);
		}
	}

}

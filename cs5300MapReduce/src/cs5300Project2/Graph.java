package cs5300Project2;

import java.util.TreeMap;

public class Graph {
	
	private TreeMap<Integer, AdjacencyList> graph = new TreeMap<Integer, AdjacencyList>();
	
	public Graph(){
		
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
	
	public TreeMap<Integer, AdjacencyList> getGraph(){
		return graph;
	}
	
	private boolean nodeInGraph(Integer node){
		return graph.containsKey(node);
	}
	
	public void addSinkNode(Integer node){
		
		if (nodeInGraph(node))
			return;
		
		AdjacencyList aList = new AdjacencyList();
		graph.put(node, aList);
	}

}

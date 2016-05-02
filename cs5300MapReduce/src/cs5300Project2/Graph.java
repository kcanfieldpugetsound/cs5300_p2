package cs5300Project2;

import java.util.HashMap;

public class Graph {
	/**
	 * This data structure stores an unsorted mapping from node ID to adjacency list. This is the representation of the graph. 
	 * For small datasets, a TreeMap is neater, but for massive datasets, like edges.txt, HashMap is better for time complexity. Also,
	 * if a treemap attempts to balance the tree on new node insertion, time taken to traverse edges.txt is horifically bad. 
	 * We do not need to maintain sorted order with our input, so a hashmap suffices. Please note that it still takes a couple minutes for 
	 * preprocessing due to the large size of the file and slow file I/O. 
	 */
	private HashMap<Integer, AdjacencyList> graph;
	
	
	/**
	 * 
	 * @param totalNodes each node gets its own bucket. Set capacity of map
	 */
	public Graph(int totalNodes){
		graph = new HashMap<Integer, AdjacencyList>(totalNodes); 
	}
	
	
	/**
	 * 
	 * @param source
	 * @param destination
	 * Check if the source is in the map. If so, then append the destination to the adjacency list. 
	 * Otherwise, make a new adjacency list with the destination as the first element in it. Add a new mapping <source, adjacency list>
	 * to the hashmap
	 */
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
	
	/**
	 * 
	 * @return number of nodes inserted
	 */
	public int numNodes(){
		return graph.keySet().size();
	}
	
	
	/**
	 * 
	 * @return the graph (map)
	 */
	public HashMap<Integer, AdjacencyList> getGraph(){
		return graph;
	}
	
	
	/**
	 * 
	 * @param node: add a node with no children, but only if it does not exist in the graph already
	 */
	public void addSinkNode(Integer node){
		
		if (graph.containsKey(node))
			return;
		else{
			AdjacencyList aList = new AdjacencyList();
			graph.put(node, aList);
		}
	}

}

public class Edge{
		
	private int source;
	private int destination;

	public Edge(int src, int dest)
	{
		this.source = src;
		this.destination = dest;
	}
	
	// GETTERS
	public int getSource()
	{ return this.source; }
	public int getDestination()
	{ return this.destination; }
}

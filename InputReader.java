import java.io.File;
import java.util.*;


class InputReader
{

// compute filter parameters for netid jf446 - Jerome Francis
double fromNetID = 0.644; // 644 is 446 reversed
double rejectMin = 0.9 * fromNetID;
double rejectLimit = rejectMin + 0.01;


// assume 0.0 <= rejectMin < rejectLimit <= 1.0
boolean selectInputLine(double x) {
	return ( ((x >= rejectMin) && (x < rejectLimit)) ? false : true );
}

// get the input file and filter the edges based on netID digits
public static Set<Edge> makeEdgeSet()
{
	Set<Edge> edgeSet = new HashSet<Edge>();

	try{
		File edgesFile = new File("edges.txt");
		Scanner sc = new Scanner(edgesFile);

		while(sc.hasNextLine())
		{
			String line = sc.nextLine();
			String[] array = line.split("\\s+");
			Double filterNum = Double.valueOf(array[array.length-1]);
			if(selectInputLine(filterNum))//for the lines we are keeping
			{//add them to the edge set
				int src = array[1];
				int dest = array[2];
				Edge newEdge = new Edge(src,dest);
				edgeSet.add(newEdge);
			}
		}

	}
	catch(Exception e)
	{
		System.err.println("Error reading file");
	}
return edgeSet;
}

}

package core.data;

/**
 * This class defines the partitioning of the graph.
 * It is used when calculating the query cost.
 * 
 * @author alekh
 *
 */
public class Partitioning {
	
	public static class SourceVertexPartitioning extends Partitioning {
		// partition by source vertex
	}
	
	public static class DestinationVertexPartitioning extends Partitioning {
		// partition by destination vertex
	}
	
	public static class TwoDPartitioning extends Partitioning {
		// partition by both source and destination vertices
	}
	
	
	public long getKey(Edge e){
		return 0;	// TODO
	}
}

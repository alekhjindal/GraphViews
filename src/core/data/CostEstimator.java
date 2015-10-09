package core.data;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Vector;

import core.query.Query;

public class CostEstimator {
	private Hashtable<String, Double> attributeStatHashTable = new Hashtable<String, Double>();
	private Hashtable<String, Double> valueStatHashTable = new Hashtable<String, Double>();
	private Hashtable<String, Double> overallStatHashTable = new Hashtable<String, Double>();
	
	public CostEstimator (String statsFile) {		
    	try {
    		//statsFile = "/data/aaaaaaaaaaaa/DGPM/stats/" + statsFile + ".stats";
    		FileInputStream fstream = new FileInputStream(statsFile);
    		DataInputStream in = new DataInputStream(fstream);
    		BufferedReader br = new BufferedReader(new InputStreamReader(in));
    		
    		String strLine;
    		while ((strLine = br.readLine()) != null) {
    			String array[] = strLine.split("\\|");
    			
    			if (array[0].equals("overall")) {
    				//System.out.println("overall");
    				overallStatHashTable.put(array[1], Double.parseDouble(array[2]));
    			}
    			else if (array[0].equals("value")) {
    				valueStatHashTable.put(array[1], Double.parseDouble(array[2]) / overallStatHashTable.get("numberOfTuples"));
    			}
    			else if (array[0].equals("attribute")) {
    				attributeStatHashTable.put(array[1], Double.parseDouble(array[2]) / overallStatHashTable.get("numberOfTuples"));
    			}
    		}
    		
    		in.close();
    	} catch (Exception e) {
    		System.err.println("Error: " + e.getMessage());
    	}
		
    	/*
		if (statsFile.equals("amazon")) {
			numberOfTuples = 1788725;
			distinctValueOfHeads = 379093;
			distinctValueOfTails = 410057;
			distinctValueOfHeadTailPairs = numberOfTuples;
			return;
		} else if (statsFile.equals("youtube")) {
			numberOfTuples = 2962892;
			distinctValueOfHeads = 151249;
			distinctValueOfTails = 1160724;
			distinctValueOfHeadTailPairs = numberOfTuples;
			return;
		}  else if (statsFile.equals("google-web-graph")) {
			numberOfTuples = 5105039;
			distinctValueOfHeads = 739454;
			distinctValueOfTails = 714545;
			distinctValueOfHeadTailPairs = numberOfTuples;
			return;
		}  
		*/
	}
	
	public double sizeEstimate(Query q) {
		// this is the number of tuples we keep track of 
		double sizeEstimate = 0;
		Vector<Edge> edges = q.getGraphQuery().getEdges();
		HashSet<String> set = new HashSet<String>(); 
		
		for (int i = 0; i < edges.size(); i++) {
			Edge e = edges.get(i);
			boolean joinOnHead = false;
			boolean joinOnTail = false;
			
			if (i == 0) {
				sizeEstimate = edgeSizeEstimate(e);
				set.add(e.getHead());
				set.add(e.getTail());
				continue;
			}
			
			int intersectionSize = 0;
			
			if (set.contains(e.getHead())) {
				intersectionSize++;
				joinOnHead = true;
			}
			
			if (set.contains(e.getTail())) {
				intersectionSize++;
				joinOnTail = true;
			}
			
			double edgeSizeEstimate = edgeSizeEstimate(e);
			double sizeOfProduct = sizeEstimate * edgeSizeEstimate;
		
			// We assume uniform distribution of values and  
			if (intersectionSize == 0) {
				sizeEstimate = sizeOfProduct;
			// Refer to page 582 of the textbook	
			} else if (intersectionSize == 1) {
				if (joinOnHead)
					sizeEstimate = sizeOfProduct / overallStatHashTable.get("distinctValueOfHeads"); 
				if (joinOnTail)
					sizeEstimate = sizeOfProduct / overallStatHashTable.get("distinctValueOfTails"); 
			} else if (intersectionSize == 2) {
				sizeEstimate = sizeOfProduct / overallStatHashTable.get("distinctValueOfHeadTailPairs"); 
			}
			
			// Add the head and tail of the edge to the vertex set
			set.add(e.getHead());
			set.add(e.getTail());
		}
		
		// multiplication of number of tuples and number of columns
		return sizeEstimate * set.size();
	}
	
	private double edgeSizeEstimate(Edge e) {
		double edgeSizeEstimate = overallStatHashTable.get("numberOfTuples");
	
		/*
		// TODO: Support multiple filters on the same edge
		if (e.hasFilter()) {
			String filter = e.getFilter();
			// The first quote of X.attribute
			int indexOfFirstQuote = filter.indexOf("'");
			// The second quote of X.attribute
			int indexOfSecondQuote = filter.indexOf("'", indexOfFirstQuote + 1);
			// The first quote of X.value
			int indexOfThirdQuote = filter.indexOf("'", indexOfSecondQuote + 1);
			// The second quote of X.value
			int indexOfFourthQuote = filter.indexOf("'", indexOfThirdQuote + 1);
			// We ONLY care about the selectivity of value
			String tmp = filter.substring(indexOfThirdQuote + 1, indexOfFourthQuote);
			
			if (valueStatHashTable.get(tmp) == null) {
				System.out.println("The value is not specified!");
				return edgeSizeEstimate;
			}
			
			return edgeSizeEstimate * valueStatHashTable.get(tmp);
		}
		else
		*/ 
			return edgeSizeEstimate;
	}
}

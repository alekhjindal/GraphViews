package core.algo.patternmatching.views;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Hashtable;
import java.util.List;
import java.util.Vector;

import core.algo.patternmatching.ProcessInput;
import core.data.CostEstimator;
import core.data.Edge;
import core.data.Partitioning;
import core.data.Partitioning.SourceVertexPartitioning;
import core.query.Query;
import core.query.plan.QueryPlanNode;
import junit.framework.TestCase;

public class TestProcessViews extends TestCase{

	List<String> queryFiles;
	
	Hashtable<String, Query> ht;
	Vector<CostEstimator> viewCostEstimators;
	Partitioning p;
	
	int numberOfMachines = 10;

	long startTime;

	public void setUp(){
		String queryDir = "/Users/alekh/Work/MIT/GraphFrame/GraphViews/queries2";
		String[] excludeQuerySet = new String[]{"README", "amazon", "yahoo", "youtube", "filter"};
		
		String statsDir = "/Users/alekh/Work/MIT/GraphFrame/GraphViews/stats";
		String[] includeStatSet = new String[]{"amazon"};
		
		File folder = new File(queryDir);
		File[] listOfFiles = folder.listFiles();
		queryFiles = new ArrayList<String>();
		for (int i = 0; i < listOfFiles.length; i++) 
			if (listOfFiles[i].isFile()) {
				boolean excluded = false;
				for(String incl: excludeQuerySet){
					if(listOfFiles[i].getName().contains(incl))
						excluded = true;					
				}
				if(excluded)
					continue;
				
				//System.out.println("File " + listOfFiles[i].getName());
				queryFiles.add(queryDir+"/"+listOfFiles[i].getName());
			}
		
		viewCostEstimators = new Vector<CostEstimator>();
		for(String includeStat: includeStatSet){
			String statsFile = statsDir + "/"+ includeStat + ".stats";        
	    	CostEstimator ce = new CostEstimator(statsFile);	    	
	    	viewCostEstimators.addElement(ce);
		}
		
    	p = new SourceVertexPartitioning();		// TODO: change to other partitioning if applicable
	}
	
	private String prepareQuery(String queryFile){
		ht = new Hashtable<String, Query>();
    	String inputQuery = ProcessInput.GenerateInputString(queryFile);
    	startTime = System.nanoTime();
    	return inputQuery;
	}
	
	public void testLinear(){
		for(String queryFile: queryFiles){
			String inputQuery = prepareQuery(queryFile); 
			Vector<View> emptyViewSet = new Vector<View>();
			ProcessViews.findPlan(inputQuery, ht, numberOfMachines, viewCostEstimators.get(0), emptyViewSet, "DYNAMICPROGRAMMINGLINEAR", p);
			printPlan(inputQuery);
		}
	}
	
	public void testBushy(){
		for(String queryFile: queryFiles){
			//ProcessViews.candidateViews = new Vector<Solution>();
			
			String inputQuery = prepareQuery(queryFile); 
			Vector<View> emptyViewSet = new Vector<View>();
			ProcessViews.findPlan(inputQuery, ht, numberOfMachines, viewCostEstimators.get(0), emptyViewSet, "DYNAMICPROGRAMMINGBUSHY", p);
			printPlan(inputQuery);
			System.out.println("\n");
		}
		
		printTopViews(3, 5);
	}
	
	private void printTopViews(int size, int topK){
		List<Solution> viewList = new ArrayList<Solution>();
		for(Solution s: ProcessViews.candidateViews){
			if(s.getPlan().getQuery().getGraphQuery().getEdges().size() <= size)
				viewList.add(s);
		}
		
		Collections.sort(viewList);
//		Collections.sort(viewList, new Comparator<Solution>(){
//			public int compare(Solution o1, Solution o2) {
//				double score1 = o1.getCost() / o1.getPlan().getQuery().getGraphQuery().getEdges().size();
//				double score2 = o2.getCost() / o2.getPlan().getQuery().getGraphQuery().getEdges().size();
//				if(score1 > score2)
//					return 1;
//				else if (score1 < score2)
//					return -1;
//				else
//					return 0; 
//			}
//		});
		
		for(int i=0; i<topK && i<viewList.size(); i++){
			Solution s = viewList.get(viewList.size()-i-1); 
			System.out.println("Query: ");
			Vector<Edge> edges = s.getPlan().getQuery().getGraphQuery().getEdges();
			for(Edge e: edges)
				e.print();
	        System.out.println("Cost: "+s.getCost());
	        System.out.println("-----------------------");
		}
	}
	
	private void printPlan(String inputQuery){
		long endTime = System.nanoTime();
		Solution solution = ProcessViews.findMinCostSolution(inputQuery, ht);
	    double cost = solution.getCost();
	    QueryPlanNode plan = solution.getPlan();    	
        plan.assignNodeID();
        System.out.println("Query: "+inputQuery);
        System.out.println("Cost: "+cost);
        System.out.println("Plan: "+plan);
        System.out.println("Time taken: "+ ((double)(endTime-startTime)/1E9)+" secs");
	}
	
	public void tearDown(){
		
	}
}

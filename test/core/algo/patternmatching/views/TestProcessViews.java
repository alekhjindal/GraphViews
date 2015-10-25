package core.algo.patternmatching.views;

import java.io.File;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Vector;

import core.algo.patternmatching.ProcessInput;
import core.data.CostEstimator;
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
		String queryDir = "/Users/alekh/Work/MIT/GraphFrame/GraphViews/queries";
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
			ProcessViews.findPlan(inputQuery, ht, numberOfMachines, viewCostEstimators, "DYNAMICPROGRAMMINGLINEAR", p);
			printPlan(inputQuery);
		}
	}
	
	public void testBushy(){
		for(String queryFile: queryFiles){
			String inputQuery = prepareQuery(queryFile); 
			ProcessViews.findPlan(inputQuery, ht, numberOfMachines, viewCostEstimators, "DYNAMICPROGRAMMINGBUSHY", p);
			printPlan(inputQuery);
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

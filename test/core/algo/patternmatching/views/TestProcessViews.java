package core.algo.patternmatching.views;

import java.util.Hashtable;
import java.util.Vector;

import core.algo.patternmatching.ProcessInput;
import core.data.CostEstimator;
import core.data.Partitioning;
import core.data.Partitioning.SourceVertexPartitioning;
import core.query.Query;
import core.query.plan.QueryPlanNode;
import junit.framework.TestCase;

public class TestProcessViews extends TestCase{

	String inputQuery;
	Hashtable<String, Query> ht;
	Vector<CostEstimator> viewCostEstimators;
	Partitioning p;
	
	int numberOfMachines = 10;
	

	public void setUp(){
		String queryFile = "/Users/alekh/Work/MIT/GraphFrame/GraphViews/queries/ab-ac.txt";
		String viewset = "amazon";
		
		String statsFile = "/Users/alekh/Work/MIT/GraphFrame/GraphViews/stats/" + viewset + ".stats";        
    	CostEstimator ce = new CostEstimator(statsFile);
    	viewCostEstimators = new Vector<CostEstimator>();
    	viewCostEstimators.addElement(ce);
    	
    	inputQuery = ProcessInput.GenerateInputString(queryFile);
    	
    	ht = new Hashtable<String, Query>();
    	p = new SourceVertexPartitioning();		// TODO
	}
	
	public void testLinear(){
		ProcessViews.findPlan(inputQuery, ht, numberOfMachines, viewCostEstimators, "DYNAMICPROGRAMMINGLINEAR", p);
	}
	
	public void testBushy(){
		ProcessViews.findPlan(inputQuery, ht, numberOfMachines, viewCostEstimators, "DYNAMICPROGRAMMINGBUSHY", p);
	}
	
	public void tearDown(){
		Solution solution = ProcessViews.findMinCostSolution(inputQuery, ht);
	    double cost = solution.getCost();
	    QueryPlanNode plan = solution.getPlan();    	
        plan.assignNodeID();
        
        System.out.println("Cost: "+cost);
        System.out.println("Plan: "+plan);
	}
}

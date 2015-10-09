package core.algo.patternmatching.views;

import java.util.HashSet;
import java.util.Hashtable;
import java.util.Set;
import java.util.Vector;

import core.algo.patternmatching.LinearDecomposition;
import core.algo.patternmatching.ProcessInput;
import core.data.CostEstimator;
import core.data.Partitioning;
import core.query.GraphQuery;
import core.query.Query;
import core.query.ViewQuery;
import core.query.plan.QueryPlanNode;

public class ProcessViews extends ProcessInput{

	// 1. we need to create a different CostEstimator for each view
	// 2. add a view id on the query plan triple
	
	public static void findPlan(String q, Hashtable<String, Query> ht, int numberOfMachines, Vector<CostEstimator> viewCostEstimators, String approach, Partitioning p) {
    	ViewQuery query = getQuery(ht, q); 
    	Vector<Solution> planSolutions = new Vector<Solution>();
    	
    	if (query.planTripleCount() > 0)
    		return;
    	
    	if (query.hasEdgesCopartitioned(p)) {
    		QueryPlanNode node = new QueryPlanNode(query, q);
    		
    		for(CostEstimator ce: viewCostEstimators){
    			Solution solution = new Solution(p.getKey(query.firstEdge()), node, ce, 0);	// not sure why the cost is set to 0!
    			query.addPlanSolution(solution);
    		}
    		
    		return;
    	} 
    	
    	Vector<LinearDecomposition> vld = linearDecomposition(query.getGraphQuery());
    	
    	//System.out.println("linearDecomposition size " + vld.size() + "\n");
    	
    	for (LinearDecomposition ld : vld) {
    		findPlan(ld.getOneHeadSubgraph(), ht, numberOfMachines, viewCostEstimators, approach, p);
    		findPlan(ld.getTheRest(), ht, numberOfMachines, viewCostEstimators, approach, p);
    		Query q1 = ht.get(ld.getOneHeadSubgraph());
    		Query q2 = ht.get(ld.getTheRest());
    		planSolutions.addAll(computeLinearViewCosts(query, (ViewQuery)q2, (ViewQuery)q1, numberOfMachines));
    	}
    	
    	/********************************START OF BUSHY PLANS************************************************/
    	// TODO: fix the bushy plan once we fix the linear one ...
    	
    	//if (approach.equals("DYNAMICPROGRAMMINGBUSHY")) {
    	//	findBushyPlan(q, numberOfMachines, ht, ce, approach, planTriples);
    	//}
    	/********************************END OF BUSHY PLANS**************************************************/
    	// 
    	query.setPlanSolutions(EliminateNonMinViewCosts(planSolutions));
    	
    	//System.out.println(q + "\n" + planTriples.size() + "-----------\n");
    }
	
	@SuppressWarnings("unused")
	public static Vector<Solution> computeLinearViewCosts(ViewQuery query, ViewQuery q1, ViewQuery q2, int numberOfMachines) {
    	Vector<Solution> triples = new Vector<Solution>();
    	
    	Vector<Solution> solutions1 = q1.getPlanSolutions();
    	Vector<Solution> solutions2 = q2.getPlanSolutions();
    	
    	Hashtable<String, Double> ht = new Hashtable<String, Double>();
    	
    	for (Solution s1: solutions1) {
    		for (Solution s2: solutions2) {
    			Set<String> commonVertexes = GraphQuery.findCommonVertexes(q1.getGraphQuery(), q2.getGraphQuery());
    			for (String v: commonVertexes) {
    				double costOft1Andt2 = s1.getCost() + s2.getCost();
    				//double costOfJoinInDB = ce.sizeEstimate(query) + ce.sizeEstimate(q1) + ce.sizeEstimate(q2);
    				
    				// Directed Join: q1 to q2
//    				if (s2.getVertex().equals(v)) {
//        				compareCost(query, 
//        						    costOft1Andt2 + ce.sizeEstimate(q1) + ce.sizeEstimate(query),   // WARNING: total size of the query is added and other joins
//    								ht, 
//    								v,
//    								v,
//    								s1.getPlan(), 
//    								s2.getPlan(), 
//    								triples, 
//    								QueryPlanNodeType.DIRECTED_JOIN_Q1_TO_Q2);				
//    				}
//    				// Directed Join: q2 to q1
//    				else if (s1.getVertex().equals(v)) {
//        				compareCost(query, costOft1Andt2 + ce.sizeEstimate(q2) + ce.sizeEstimate(query),
//    								ht, 
//    								v,
//    								v,
//    								s1.getPlan(), 
//    								s2.getPlan(), 
//    								triples, 
//    								QueryPlanNodeType.DIRECTED_JOIN_Q2_TO_Q1);				
//    				}
//    				// Only use Parallel Hash Join if Directed Join is not applicable    		
//    				else {
//        				compareCost(query, costOft1Andt2 + ce.sizeEstimate(q1) + ce.sizeEstimate(q2) + ce.sizeEstimate(query),
//        							ht, 
//        							v,
//        							v,
//        							s1.getPlan(), 
//        							s2.getPlan(), 
//        							triples, 
//        							QueryPlanNodeType.HASH_JOIN);
//    				}
//    				
//    				// Broadcast Join: q1 to q2
//    				compareCost(query, costOft1Andt2 + ce.sizeEstimate(q1) * numberOfMachines + ce.sizeEstimate(query),
//								ht, 
//								v, 
//								s2.getVertex(),
//								s1.getPlan(), 
//								s2.getPlan(), 
//								triples, 
//								QueryPlanNodeType.BROADCAST_JOIN_Q1_TO_Q2);
//    				
//    				// Broadcast Join: q2 to q1
//    				compareCost(query, costOft1Andt2 + ce.sizeEstimate(q2) * numberOfMachines + ce.sizeEstimate(query),
//								ht, 
//								v, 
//								s1.getVertex(),
//								s1.getPlan(), 
//								s2.getPlan(), 
//								triples, 
//								QueryPlanNodeType.BROADCAST_JOIN_Q2_TO_Q1);
    			}
    		}
    	}
    	
    	return triples;
    }
	
	
	public static Vector<Solution> EliminateNonMinViewCosts(Vector<Solution> solutions) {
    	HashSet<Long> pKeys = new HashSet<Long>();
    	Vector<Solution> result = new Vector<Solution>();
    	
    	for (Solution s: solutions)
    		pKeys.add(s.getPartitioningKey());
    	
    	for (Long v: pKeys) {
    		Solution minCostSolution = null;
    		
    		for (Solution s : solutions) {
    			if (v.equals(s.getPartitioningKey())) {
    				if (minCostSolution == null || s.getCost() < minCostSolution.getCost()) {
    					minCostSolution = s;
    				}
    			}
    		}
    		
    		result.add(minCostSolution);
    	}
    	
//    	for (Solution s : solutions)
//    		System.out.println("vertex:" + s.getPartitioningKey() + "\t\tcost:" + s.getCost() + "\t\tplan:" + s.getPlan().toString() + "\n-----------\n");
    	
    	return result;
//    	System.out.println("triples.size() after elimination: " + solutions.size() + "\n");
    }
	
	
	public static ViewQuery getQuery(Hashtable<String, Query> ht, String query) {
    	ViewQuery result = (ViewQuery)ht.get(query);
    	//System.out.print(query);
    	if (result == null) {
    		result = new ViewQuery(query);
    		ht.put(query, result);
    	}    	
    	return result;    	
    }
}

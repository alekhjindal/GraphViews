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
import core.query.plan.QueryPlanNodeType;

public class ProcessViews extends ProcessInput{

	// 1. we need to create a different CostEstimator for each view
	// 2. add a view id on the query plan triple
	
	public static void findPlan(String q, Hashtable<String, Query> ht, int numberOfMachines, Vector<CostEstimator> viewCostEstimators, String approach, Partitioning p) {
    	ViewQuery query = getQuery(ht, q); 
    	Vector<Solution> planSolutions = new Vector<Solution>();
    	
    	if (query.planSolutionCount() > 0)
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
	
	public static Vector<Solution> computeLinearViewCosts(ViewQuery query, ViewQuery q1, ViewQuery q2, int numberOfMachines) {
    	Vector<Solution> solns = new Vector<Solution>();
    	
    	Vector<Solution> solutions1 = q1.getPlanSolutions();
    	Vector<Solution> solutions2 = q2.getPlanSolutions();
    	
    	Hashtable<Long, Double> ht = new Hashtable<Long, Double>();
    	
    	for (Solution s1: solutions1) {
    		for (Solution s2: solutions2) {
    			CostEstimator qEstimator = CostEstimator.combine(s1.getEstimator(), s2.getEstimator());
    			Set<String> commonVertexes = GraphQuery.findCommonVertexes(q1.getGraphQuery(), q2.getGraphQuery());
    			for (String v: commonVertexes) {
    				double costOft1Andt2 = s1.getCost() + s2.getCost();
    				//double costOfJoinInDB = ce.sizeEstimate(query) + ce.sizeEstimate(q1) + ce.sizeEstimate(q2);
    				
    				// Directed Join: q1 to q2
    				if (s2.getPartitioningKey()==v.hashCode()) {
        				compareCost(query, 
        						    costOft1Andt2 + s1.getEstimator().sizeEstimate(q1) + qEstimator.sizeEstimate(query),   // WARNING: total size of the query is added and other joins
    								ht, 
    								v,
    								v.hashCode(),
    								s1.getPlan(), 
    								s2.getPlan(),
    								qEstimator,
    								solns, 
    								QueryPlanNodeType.DIRECTED_JOIN_Q1_TO_Q2);				
    				}
    				// Directed Join: q2 to q1
    				else if (s1.getPartitioningKey()==v.hashCode()) {
        				compareCost(query, costOft1Andt2 + s2.getEstimator().sizeEstimate(q2) + qEstimator.sizeEstimate(query),
    								ht, 
    								v,
    								v.hashCode(),
    								s1.getPlan(), 
    								s2.getPlan(),
    								qEstimator,
    								solns, 
    								QueryPlanNodeType.DIRECTED_JOIN_Q2_TO_Q1);				
    				}
    				// Only use Parallel Hash Join if Directed Join is not applicable    		
    				else {
        				compareCost(query, costOft1Andt2 + s1.getEstimator().sizeEstimate(q1) + s2.getEstimator().sizeEstimate(q2) + qEstimator.sizeEstimate(query),
        							ht, 
        							v,
        							v.hashCode(),
        							s1.getPlan(), 
        							s2.getPlan(), 
        							qEstimator,        							
        							solns, 
        							QueryPlanNodeType.HASH_JOIN);
    				}
    				
    				// Broadcast Join: q1 to q2
    				compareCost(query, costOft1Andt2 + s1.getEstimator().sizeEstimate(q1) * numberOfMachines + qEstimator.sizeEstimate(query),
								ht, 
								v, 
								s2.getPartitioningKey(),
								s1.getPlan(), 
								s2.getPlan(), 
								qEstimator,
								solns, 
								QueryPlanNodeType.BROADCAST_JOIN_Q1_TO_Q2);
    				
    				// Broadcast Join: q2 to q1
    				compareCost(query, costOft1Andt2 + s2.getEstimator().sizeEstimate(q2) * numberOfMachines + qEstimator.sizeEstimate(query),
								ht, 
								v, 
								s1.getPartitioningKey(),
								s1.getPlan(), 
								s2.getPlan(), 
								qEstimator,
								solns, 
								QueryPlanNodeType.BROADCAST_JOIN_Q2_TO_Q1);
    			}
    		}
    	}
    	
    	return solns;
    }
	
	public static void compareCost(Query query, double cost, Hashtable<Long, Double> ht, String joinVertex, long partitioningKey, QueryPlanNode n1, QueryPlanNode n2, CostEstimator ce, Vector<Solution> triples, QueryPlanNodeType type) {
		Double minCostForV = ht.get(partitioningKey);
		if (minCostForV == null || cost < minCostForV) {
			QueryPlanNode node = new QueryPlanNode(query, type, joinVertex, n1, n2);
			Solution solution = new Solution(
										partitioningKey, 
										node, 
										ce, 
										cost
									);
			triples.add(solution); 
			ht.put(partitioningKey, cost);
		}    	
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
	
	public static Solution findMinCostSolution(String inputQuery, Hashtable<String, Query> ht) {
	    ViewQuery query = (ViewQuery)ht.get(inputQuery);
	    double minCost = -1;
        Solution minCostSolution = null;

  	    for (Solution s : query.getPlanSolutions()) {
  	    	if (minCost < 0 || minCost > s.getCost()) {
  	    		minCost = s.getCost();
  	    		minCostSolution = s;
  	    	}
  	    }
  	    
  	    return minCostSolution;    	
    }
}

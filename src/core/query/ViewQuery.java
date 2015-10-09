package core.query;

import java.util.Vector;

import core.algo.patternmatching.views.Solution;
import core.data.Edge;
import core.data.Partitioning;

/**
 * The query to run over views
 * 
 * @author alekh
 *
 */
public class ViewQuery extends Query {

	private Vector<Solution> solutions = new Vector<Solution>();
	
	public ViewQuery(String query) {
		super(query);
	}

	public Vector<Solution> getPlanSolutions() {
		return solutions;
	}

	public void setPlanSolutions(Vector<Solution> s) {
		solutions = s;
	}
	
	public boolean hasPlanSolutions() {
		return (solutions.size() > 0);
	}
	
	public boolean hasEdgesCopartitioned(Partitioning p) {
		return true;	//TODO
	}
	
	public Edge firstEdge(){
		return null;	// TODO
	}

	public void addPlanSolution(Solution solution) {
		solutions.add(solution);
	}
	
	public int planSolutionCount() {
		return solutions.size();
	}
	
}

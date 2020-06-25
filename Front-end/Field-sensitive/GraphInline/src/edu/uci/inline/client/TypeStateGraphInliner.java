package edu.uci.inline.client;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.uci.inline.callgraph.CallGraph;
import edu.uci.inline.callgraph.CallGraphNode;
import edu.uci.inline.datastructures.CallSite;
import edu.uci.inline.datastructures.GraphVertex;
import edu.uci.inline.datastructures.IntraGraphIdentifier;
import edu.uci.inline.datastructures.TypeStateEdge;
import edu.uci.inline.intragraph.IntraGraph;
import edu.uci.inline.intragraphs.IntraGraphs;

public class TypeStateGraphInliner{
	private static int clone_counter = 1;

	private final CallGraph callGraph;

	private final Map<IntraGraphIdentifier, IntraGraph> graphsMap;

	// the graphs need to exported
	private final Set<IntraGraphIdentifier> outGraphs;

	public TypeStateGraphInliner(CallGraph callGraph, IntraGraphs intraGraphs) {
		this.callGraph = callGraph;
		this.graphsMap = intraGraphs.getGraphs();
		this.outGraphs = new HashSet<IntraGraphIdentifier>();
	}

	public void inline() {
		this.outGraphs.addAll(this.graphsMap.keySet());

		for (CallGraphNode callNode : callGraph.getCallGraphInfoList()) {
			IntraGraphIdentifier caller = callNode.getCaller();
			// for debugging
			// System.out.println(caller);
			// System.out.println("\n\n");

			List<IntraGraphIdentifier> callees = callNode.getCallees();
			// for debugging
			// System.out.println(callees.toString());
			// System.out.println("\n\n");

			if (graphsMap.containsKey(caller)) {
				// this.outGraphs.add(caller);

				IntraGraph callerGraph = graphsMap.get(caller);
				List<CallSite> callSites = callerGraph.getCallSites();
				List<CallSite> shallowClone = new ArrayList<CallSite>(callSites);
				// for debugging
				// System.out.println(callSites);
				// System.out.println("\n\n");

				// number of callsite should be same as the number of callees
				// generated from callgraph
				// assert(callSites.size() == callees.size());

				for (int i = 0; i < callees.size(); i++) {
					IntraGraphIdentifier callee = callees.get(i);

					// for debugging
					// System.out.println("callsites:");
					// System.out.println(shallowClone.toString());

					CallSite callSite = getCallSite(shallowClone, callees, i);
					if (callSite != null) {
						inlineOneInvocation(callee, callSite, callerGraph);
					}
				}
			} else {
				// System.out.println(callNode);
				// throw new RuntimeException("cannot find the corresponding
				// intra graph!");
			}
		}

		System.out.println();
		System.out.println("Total number of clones:\t" + clone_counter);
	}

	private CallSite getCallSite(List<CallSite> callSites, List<IntraGraphIdentifier> callees, int i) {
		IntraGraphIdentifier callee = callees.get(i);
		for (Iterator<CallSite> it = callSites.iterator(); it.hasNext();) {
			CallSite callSite = it.next();
			if (callSite.getCallee().getCallName().equals(callee.getCallName())) {
				it.remove();
				return callSite;
			}
		}
		return null;
	}

	/**
	 * @param callee
	 * @param callSite
	 * @param callerGraph
	 */
	private void inlineOneInvocation(IntraGraphIdentifier callee, CallSite callSite, IntraGraph callerGraph) {
		assert(callee.getCallName().equals(callSite.getCallee().getCallName()));

		if (this.graphsMap.containsKey(callee)) {
			IntraGraph calleeGraph = this.graphsMap.get(callee);
			// for debugging
			// System.out.println("CalleeGraph:*************************");
			// System.out.println(calleeGraph.toString());
			// System.out.println("\n\n");

			Map<GraphVertex, List<TypeStateEdge>> edges_callee = calleeGraph.getTypeStateEdgeList();
			List<GraphVertex> formal_parameters = calleeGraph.getFormal_parameters();
			List<GraphVertex> formal_returns = calleeGraph.getFormal_returns();

			// get the vertices of caller, actual arguments and returns
			Map<GraphVertex, List<TypeStateEdge>> edges_caller = callerGraph.getTypeStateEdgeList();
			List<GraphVertex> actual_args = callSite.getActualArgs();
			List<GraphVertex> actual_returns = callSite.getActualReturns();

			// for debugging
			// System.out.println("CallSite:----------------------");
			// System.out.println(callSite.toString());
			// System.out.println("\n\n");
			assert(formal_parameters.size() <= actual_args.size());

			// deep clone edges
			if (callee.equals(callerGraph.getIdentifier())) {
//				System.out.println(callee.toString() + "\t" + callerGraph.getIdentifier().toString());
			} else {
				this.outGraphs.remove(callee);
				deepCloneEdges(edges_caller, edges_callee, actual_args, formal_parameters, actual_returns,
						formal_returns, callSite);
			}
		}
	}

	/**
	 * @param edges_caller
	 * @param edges_callee
	 * @param actual_args
	 * @param formal_parameters
	 * @param actual_returns
	 * @param formal_returns
	 */
	private void deepCloneEdges(Map<GraphVertex, List<TypeStateEdge>> edges_caller,
			Map<GraphVertex, List<TypeStateEdge>> edges_callee, List<GraphVertex> actual_args,
			List<GraphVertex> formal_parameters, List<GraphVertex> actual_returns, List<GraphVertex> formal_returns,
			CallSite callSite) {
		// int counter = clone_counter++;

		// a map maintaining mapping between version number and counter
		Map<Integer, Integer> counterMap = new HashMap<Integer, Integer>();

		// //maintain a vertex factory so as to avoid identical instance
		// duplication
		for (GraphVertex vertex_callee : edges_callee.keySet()) {
			// clone vertex
			// GraphVertex vertex_callee_clone = new
			// GraphVertex(vertex_callee.getId(), counter);
			GraphVertex vertex_callee_clone = cloneGraphVertex(vertex_callee, counterMap);

			// clone list
			List<TypeStateEdge> vertex_edge_list_callee = edges_callee.get(vertex_callee);
			List<TypeStateEdge> vertex_edge_list_clone = new ArrayList<TypeStateEdge>();
			for (TypeStateEdge edge : vertex_edge_list_callee) {
				// GraphVertex vertex_clone = new GraphVertex(vertex.getId(),
				// counter);
				GraphVertex vertex_clone = cloneGraphVertex(edge.getEnd(), counterMap);
				TypeStateEdge edge_clone = new TypeStateEdge(vertex_callee_clone, vertex_clone, edge.getLabel(),
						edge.getConstraint());
				vertex_edge_list_clone.add(edge_clone);
			}

			edges_caller.put(vertex_callee_clone, vertex_edge_list_clone);
		}

		List<TypeStateEdge> calledges = callSite.getCallEdges();
		for (TypeStateEdge edge : calledges) {
			GraphVertex start = edge.getStart();
			GraphVertex start_clone = cloneGraphVertex(start, counterMap);
			GraphVertex end = edge.getEnd();
			GraphVertex end_clone = cloneGraphVertex(end, counterMap);
			if (edge.getConstraint().endsWith("0)")) {
				if (edges_caller.containsKey(start)) {
					List<TypeStateEdge> list = edges_caller.get(start);
					list.add(new TypeStateEdge(start, end_clone, edge.getLabel(), edge.getConstraint()));
				} else {
					List<TypeStateEdge> list = new ArrayList<TypeStateEdge>();
					list.add(new TypeStateEdge(start, end_clone, edge.getLabel(), edge.getConstraint()));
					edges_caller.put(start, list);
				}
			}else{
				if (edges_caller.containsKey(start_clone)) {
					List<TypeStateEdge> list = edges_caller.get(start_clone);
					list.add(new TypeStateEdge(start_clone, end, edge.getLabel(), edge.getConstraint()));
				} else {
					List<TypeStateEdge> list = new ArrayList<TypeStateEdge>();
					list.add(new TypeStateEdge(start_clone, end, edge.getLabel(), edge.getConstraint()));
					edges_caller.put(start_clone, list);
				}
			}
		}

		// for debugging
//		System.out.println("counter map:");
		// System.out.println(counterMap.toString());

	}
//int nullv = 0;
	private GraphVertex cloneGraphVertex(GraphVertex vertex_callee, Map<Integer, Integer> counterMap) {
		// TODO Auto-generated method stub
		if(vertex_callee == null){
//			System.out.println(nullv++);
			return null;
		}
		int id = vertex_callee.getId();
		int version_num = vertex_callee.getVersion_number();
		if (counterMap.containsKey(version_num)) {
			return new GraphVertex(id, counterMap.get(version_num));
		} else {
			int counter = clone_counter++;
			counterMap.put(version_num, counter);
			return new GraphVertex(id, counter);
		}
	}

	public static int getClone_counter() {
		return clone_counter;
	}

	// public static void setClone_counter(int clone_counter) {
	// GraphInliner.clone_counter = clone_counter;
	// }

	public CallGraph getCallGraph() {
		return callGraph;
	}

	// public IntraGraphs getIntraGraphs() {
	// return intraGraphs;
	// }

	public Map<IntraGraphIdentifier, IntraGraph> getGraphsMap() {
		return graphsMap;
	}

	public Set<IntraGraphIdentifier> getOutGraphs() {
		return outGraphs;
	}

}

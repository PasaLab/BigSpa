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
import edu.uci.inline.intragraph.IntraGraph;
import edu.uci.inline.intragraphs.IntraGraphs;

public class GraphInliner {
	private static int clone_counter = 1;
	
	private final CallGraph callGraph;
	
	
	private final Map<IntraGraphIdentifier, IntraGraph> graphsMap;
	
	//the graphs need to exported
	private final Set<IntraGraphIdentifier> outGraphs;
	
	
	public GraphInliner(CallGraph callGraph, IntraGraphs intraGraphs){
		this.callGraph = callGraph;
		this.graphsMap = intraGraphs.getGraphs();
		this.outGraphs = new HashSet<IntraGraphIdentifier>();
	}

	public void inline(){
		this.outGraphs.addAll(this.graphsMap.keySet());
		
		for(CallGraphNode callNode: callGraph.getCallGraphInfoList()){
			IntraGraphIdentifier caller = callNode.getCaller();
			//for debugging
//			System.out.println(caller);
//			System.out.println("\n\n");
			
			List<IntraGraphIdentifier> callees = callNode.getCallees();
			//for debugging
//			System.out.println(callees.toString());
//			System.out.println("\n\n");
			
			if(graphsMap.containsKey(caller)){
//				this.outGraphs.add(caller);
				
				IntraGraph callerGraph = graphsMap.get(caller);
				List<CallSite> callSites = callerGraph.getCallSites();
				List<CallSite> shallowClone = new ArrayList<CallSite>(callSites);
				//for debugging
//				System.out.println(callSites);
//				System.out.println("\n\n");
				
				//number of callsite should be same as the number of callees generated from callgraph
//				assert(callSites.size() == callees.size());
				
				for(int i = 0; i < callees.size(); i++){
					IntraGraphIdentifier callee = callees.get(i);
					
					//for debugging
//					System.out.println("callsites:");
//					System.out.println(shallowClone.toString());
					
					CallSite callSite = getCallSite(shallowClone, callees, i);
					if(callSite != null){
						inlineOneInvocation(callee, callSite, callerGraph);
					}
				}
			}
			else{
//				System.out.println(callNode);
//				throw new RuntimeException("cannot find the corresponding intra graph!");
			}
		}
		
		System.out.println("\n\n");
		System.out.println("Total number of clones:\t" + clone_counter);
	}

	
	private CallSite getCallSite(List<CallSite> callSites, List<IntraGraphIdentifier> callees, int i) {
		IntraGraphIdentifier callee = callees.get(i);
		for(Iterator<CallSite> it = callSites.iterator(); it.hasNext();){
			CallSite callSite = it.next();
			if(callSite.getCallee().getCallName().equals(callee.getCallName())){
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
		
		if(this.graphsMap.containsKey(callee)){
			IntraGraph calleeGraph = this.graphsMap.get(callee);
			//for debugging
//			System.out.println("CalleeGraph:*************************");
//			System.out.println(calleeGraph.toString());
//			System.out.println("\n\n");
			
			Map<GraphVertex, ArrayList<GraphVertex>> edges_callee = calleeGraph.getAdjacencyList();
			List<GraphVertex> formal_parameters = calleeGraph.getFormal_parameters();
			List<GraphVertex> formal_returns = calleeGraph.getFormal_returns();
			
			//get the vertices of caller, actual arguments and returns 
			Map<GraphVertex, ArrayList<GraphVertex>> edges_caller = callerGraph.getAdjacencyList();
			List<GraphVertex> actual_args = callSite.getActualArgs();
			List<GraphVertex> actual_returns = callSite.getActualReturns();
			
			//for debugging
//			System.out.println("CallSite:----------------------");
//			System.out.println(callSite.toString());
//			System.out.println("\n\n");
			assert(formal_parameters.size() <= actual_args.size());
			
			//deep clone edges
			if(callee.equals(callerGraph.getIdentifier())){
				System.out.println(callee.toString() + "\t" + callerGraph.getIdentifier().toString());
			}
			else{
				this.outGraphs.remove(callee);
				deepCloneEdges(edges_caller, edges_callee, actual_args, formal_parameters, actual_returns, formal_returns);
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
	private void deepCloneEdges(Map<GraphVertex, ArrayList<GraphVertex>> edges_caller, Map<GraphVertex, ArrayList<GraphVertex>> edges_callee,
			List<GraphVertex> actual_args, List<GraphVertex> formal_parameters, List<GraphVertex> actual_returns,
			List<GraphVertex> formal_returns) {
//		int counter = clone_counter++;
		
		//a map maintaining mapping between version number and counter
		Map<Integer, Integer> counterMap = new HashMap<Integer, Integer>();
		
//		//maintain a vertex factory so as to avoid identical instance duplication
		for(GraphVertex vertex_callee: edges_callee.keySet()){
			//clone vertex
//			GraphVertex vertex_callee_clone = new GraphVertex(vertex_callee.getId(), counter);
			GraphVertex vertex_callee_clone = cloneGraphVertex(vertex_callee, counterMap);
			
			//clone list
			ArrayList<GraphVertex> vertex_list_callee = edges_callee.get(vertex_callee);
			ArrayList<GraphVertex> vertex_list_clone = new ArrayList<GraphVertex>();
			for(GraphVertex vertex: vertex_list_callee){
//				GraphVertex vertex_clone = new GraphVertex(vertex.getId(), counter);
				GraphVertex vertex_clone = cloneGraphVertex(vertex, counterMap);
				vertex_list_clone.add(vertex_clone);
			}
			
			edges_caller.put(vertex_callee_clone, vertex_list_clone);
		}
		
		
		//clone edges associated with parameters and returns
		//add edges from actual arguments to formal parameters
		for(int i = 0; i < actual_args.size(); i++){
			GraphVertex actual_arg = actual_args.get(i);
			if(actual_arg == null || actual_arg.getId() == -1){
				continue;
			}
			
			if(formal_parameters.size() <= i){
				continue;
			}
			GraphVertex formal_para = formal_parameters.get(i);
			
			if(formal_para == null || formal_para.getId() == -1){
				continue;
			}
			
//			GraphVertex formal_para_clone = new GraphVertex(formal_para.getId(), counter);
			GraphVertex formal_para_clone = cloneGraphVertex(formal_para, counterMap);
			
			if(edges_caller.containsKey(actual_arg)){
				ArrayList<GraphVertex> actual_list = edges_caller.get(actual_arg);
				actual_list.add(formal_para_clone);
			}
			else{
				ArrayList<GraphVertex> actual_list = new ArrayList<GraphVertex>();
				actual_list.add(formal_para_clone);
				edges_caller.put(actual_arg, actual_list);
			}
		}

		//add edges from formal return to actual return
		for(int i = 0; i < formal_returns.size(); i++){
			GraphVertex formal_return = formal_returns.get(i);
			
//			GraphVertex formal_return_clone = new GraphVertex(formal_return.getId(), counter);
			GraphVertex formal_return_clone = cloneGraphVertex(formal_return, counterMap);
			
			GraphVertex actual_return = actual_returns.get(i);
			
			assert(actual_return.getVersion_number() == 0);
			
			if(edges_caller.containsKey(formal_return_clone)){
				ArrayList<GraphVertex> formal_list = edges_caller.get(formal_return_clone);
				formal_list.add(actual_return);
			}
			else{
				ArrayList<GraphVertex> formal_list = new ArrayList<GraphVertex>();
				formal_list.add(actual_return);
				edges_caller.put(formal_return_clone, formal_list);
			}
		}
		
		//for debugging
//		System.out.println("counter map:");
//		System.out.println(counterMap.toString());
		
	}

	private GraphVertex cloneGraphVertex(GraphVertex vertex_callee, Map<Integer, Integer> counterMap) {
		// TODO Auto-generated method stub
		int id = vertex_callee.getId();
		int version_num = vertex_callee.getVersion_number();
		if(counterMap.containsKey(version_num)){
			return new GraphVertex(id, counterMap.get(version_num));
		}
		else{
			int counter = clone_counter++;
			counterMap.put(version_num, counter);
			return new GraphVertex(id, counter);
		}
	}

	public static int getClone_counter() {
		return clone_counter;
	}

//	public static void setClone_counter(int clone_counter) {
//		GraphInliner.clone_counter = clone_counter;
//	}

	public CallGraph getCallGraph() {
		return callGraph;
	}

//	public IntraGraphs getIntraGraphs() {
//		return intraGraphs;
//	}

	public Map<IntraGraphIdentifier, IntraGraph> getGraphsMap() {
		return graphsMap;
	}

	public Set<IntraGraphIdentifier> getOutGraphs() {
		return outGraphs;
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
}

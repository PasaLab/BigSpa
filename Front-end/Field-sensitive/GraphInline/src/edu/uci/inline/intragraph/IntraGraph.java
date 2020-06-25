package edu.uci.inline.intragraph;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.uci.inline.datastructures.CallSite;
import edu.uci.inline.datastructures.GraphVertex;
import edu.uci.inline.datastructures.IntraGraphIdentifier;
import edu.uci.inline.datastructures.TypeStateEdge;

public abstract class IntraGraph {
	
//	private final File intraGraphFile;
	
	private IntraGraphIdentifier identifier;
	
	private List<GraphVertex> formal_parameters;
	
	private List<GraphVertex> formal_returns;
	
	private List<CallSite> callSites;
	
	private Map<GraphVertex, ArrayList<GraphVertex>> adjacencyList;
	
	private Map<GraphVertex, ArrayList<Boolean>> edgeList;
	
	private Map<GraphVertex, List<TypeStateEdge>> typeStateEdgeList;
	
	public IntraGraph(){
		this.identifier = new IntraGraphIdentifier();
		this.formal_parameters = new ArrayList<GraphVertex>();
		this.formal_returns = new ArrayList<GraphVertex>();
		this.callSites = new ArrayList<CallSite>();
		this.adjacencyList = new HashMap<GraphVertex, ArrayList<GraphVertex>>();
		this.edgeList = new HashMap<GraphVertex, ArrayList<Boolean>>();
		this.typeStateEdgeList = new HashMap<GraphVertex, List<TypeStateEdge>>();
	}
	
	public int getEdgeNum(){
		int edgenum = 0;
		for (GraphVertex src : typeStateEdgeList.keySet()) {
			List<TypeStateEdge> list = typeStateEdgeList.get(src);

			// compute the total number of edges
			edgenum += list.size();
		}
		return edgenum;
	}
	
	public IntraGraph(File file){
//		this.intraGraphFile = file;
		
		this.identifier = new IntraGraphIdentifier();
		this.formal_parameters = new ArrayList<GraphVertex>();
		this.formal_returns = new ArrayList<GraphVertex>();
		this.callSites = new ArrayList<CallSite>();
		this.adjacencyList = new HashMap<GraphVertex, ArrayList<GraphVertex>>();
		this.edgeList = new HashMap<GraphVertex, ArrayList<Boolean>>();
		this.typeStateEdgeList = new HashMap<GraphVertex, List<TypeStateEdge>>();
		loadIntraGraph(file);
	}
	
	
	/**
	 * override this method if you would like to read intra graphs 
	 * from files under a folder, each file corresponds to one intra graph
	 * 
	 * @param inputFile
	 */
	protected abstract void loadIntraGraph(File inputFile);

	
	

	public IntraGraphIdentifier getIdentifier() {
		return identifier;
	}


	public void setIdentifier(IntraGraphIdentifier identifier) {
		this.identifier = identifier;
	}


	public List<GraphVertex> getFormal_parameters() {
		return formal_parameters;
	}


	public void setFormal_parameters(List<GraphVertex> formal_parameters) {
		this.formal_parameters = formal_parameters;
	}



	public List<GraphVertex> getFormal_returns() {
		return formal_returns;
	}


	public void setFormal_returns(List<GraphVertex> formal_returns) {
		this.formal_returns = formal_returns;
	}


	public List<CallSite> getCallSites() {
		return callSites;
	}


	public void setCallSites(List<CallSite> callSites) {
		this.callSites = callSites;
	}



	public Map<GraphVertex, ArrayList<GraphVertex>> getAdjacencyList() {
		return adjacencyList;
	}


	public void setAdjacencyList(Map<GraphVertex, ArrayList<GraphVertex>> adjacencyList) {
		this.adjacencyList = adjacencyList;
	}
	
	public Map<GraphVertex, List<TypeStateEdge>> getTypeStateEdgeList() {
		return typeStateEdgeList;
	}


	public void setTypeStateEdgeList(Map<GraphVertex, List<TypeStateEdge>> typeStateEdgeList) {
		this.typeStateEdgeList = typeStateEdgeList;
	}
	
	public Map<GraphVertex, ArrayList<Boolean>> getEdgeList() {
		return edgeList;
	}
	
	@Override
	public String toString() {
		StringBuilder result = new StringBuilder();
//		String NEW_LINE = System.getProperty("line.separator");
		String NEW_LINE = "\n";

		result.append("File Name: " + identifier.getFileName() + NEW_LINE);
		result.append("Function Name: " + identifier.getCallName() + NEW_LINE);
//		result.append("Range: " + identifier.getStartLineNum() + "," + identifier.getEndLineNum() + NEW_LINE);
		
		result.append("Formal Params: ").append(formal_parameters.toString());
		result.append(NEW_LINE);
		
		result.append("Formal Returns: ").append(formal_returns.toString());
		result.append(NEW_LINE);
		
		for(CallSite call : callSites) {
			result.append("CallSite: ").append(call.toString());
			result.append(NEW_LINE);
		}
		
		for(Map.Entry <GraphVertex, ArrayList<GraphVertex>> entry:  adjacencyList.entrySet()) {
			GraphVertex key = entry.getKey();
			ArrayList<GraphVertex> value = entry.getValue();
			result.append("Adj List: " + key + "--> " + value + NEW_LINE);
		}
		
		return result.toString();
	}

	public int getNumOfCallSites() {
		return callSites.size();
	}
	
	public int getNumOfEdges() {
		int count = 0;
		for(ArrayList<Boolean> val : edgeList.values()) {
			count += val.size();
		}
		return count;
	}

	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
}

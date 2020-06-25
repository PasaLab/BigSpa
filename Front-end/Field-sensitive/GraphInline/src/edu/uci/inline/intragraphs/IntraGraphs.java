package edu.uci.inline.intragraphs;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.uci.inline.datastructures.GraphVertex;
import edu.uci.inline.datastructures.IntraGraphIdentifier;
import edu.uci.inline.datastructures.TypeStateEdge;
import edu.uci.inline.intragraph.IntraGraph;

public abstract class IntraGraphs {
	
	//directory of files or a single file storing intra graphs 
	protected final File intra_graph_file;
	
	protected final Map<IntraGraphIdentifier, IntraGraph> graphs = new HashMap<IntraGraphIdentifier, IntraGraph>();
	
	//only used by NullPointerDataflowIntraGraphs
	protected Set<Integer> zeroIDs;
	protected Map<Integer, HashSet<Integer>> auxiliaryEdges;
	
	protected Map<Integer, List<TypeStateEdge>> typeStateEdges;
	
	protected final Map<GraphVertex, ArrayList<Boolean>> edgesLists = new HashMap<GraphVertex, ArrayList<Boolean>>();
	
//	protected final String package_name_interest; 
	
	public IntraGraphs(File dir) {
		this.intra_graph_file = dir;
//		this.package_name_interest = package_name;
//		readIntraGraphs(mode);
	}
	
	public int getEdgeNum(){
		int num = 0;
		for(IntraGraphIdentifier id : graphs.keySet()){
			IntraGraph graph = graphs.get(id);
			num += graph.getEdgeNum();
		}
		return num;
	}

	
	public void readIntraGraphs(String mode) {
		if(mode.equals("File")){
			readIntraGraphsFromSingleFile();
		}
		else if(mode.equals("Folder")){
			readIntraGraphsFromFilesInFolder();
		}
		else{
			throw new RuntimeException("mode error! must be either \"File\" or \"Folder\"");
		}
//		switch (mode){
//			case "File":{
//				break;
//			}
//			case "Folder":{
//				break;
//			}
//			default:{
//			}
//		}
	}

	public void readCallFile(String callfile) {
		
	}

	private void readIntraGraphsFromFilesInFolder() {
		// TODO Auto-generated method stub
		File[] files = this.intra_graph_file.listFiles();//add filter later
		for(File intraFile: files){
			IntraGraph graph = createIntraGraphFromFile(intraFile);
			IntraGraphIdentifier identifier = graph.getIdentifier();
			this.graphs.put(identifier, graph);
		}
	}

	public Map<GraphVertex, ArrayList<Boolean>> getEdgesLists() {
		return this.edgesLists;
	}
	
	/**
	 * override this method if you would like to read intra graphs 
	 * from one single file
	 */
	protected abstract void readIntraGraphsFromSingleFile();
		

	/**
	 * @param intraFile
	 * @return
	 */
	protected abstract IntraGraph createIntraGraphFromFile(File intraFile);


	public File getInputFile() {
		return intra_graph_file;
	}


	public Map<IntraGraphIdentifier, IntraGraph> getGraphs() {
		return graphs;
	}


	public Set<Integer> getZeroIDs() {
		return zeroIDs;
	}


	public Map<Integer, HashSet<Integer>> getAuxiliaryEdges() {
		return auxiliaryEdges;
	}
	
	
	
	
}

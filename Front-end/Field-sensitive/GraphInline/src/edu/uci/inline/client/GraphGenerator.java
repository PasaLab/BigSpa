package edu.uci.inline.client;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import edu.uci.inline.callgraph.CallGraph;
import edu.uci.inline.callgraph.CallGraphNode;
import edu.uci.inline.datastructures.GraphVertex;
import edu.uci.inline.datastructures.IntraGraphIdentifier;
import edu.uci.inline.export.GraphExporter;
import edu.uci.inline.export.NullPointerDataflowExporter;
import edu.uci.inline.export.PointsToExporter;
import edu.uci.inline.export.TypeStateCheckingExporter;
import edu.uci.inline.intragraph.IntraGraph;
import edu.uci.inline.intragraphs.IntraGraphs;
import edu.uci.inline.intragraphs.NullPointerDataflowIntraGraphs;
import edu.uci.inline.intragraphs.PointsToIntraGraphs;
import edu.uci.inline.intragraphs.TypeStateCheckingIntraGraphs;

public class GraphGenerator {
	
	public static void main(String[] args) {
		//load call graph nodes in topological order
		CallGraph callGraphParser = new CallGraph(new File(args[0]));
//		List<CallGraphNode> callGraphInfoList = callGraphParser.getCallGraphInfoList();
//		System.out.println(callGraphInfoList);
		
		//load intra graphs
//		IntraGraphs graphs = new NullPointerDataflowIntraGraphs(new File(args[1]), args[2]);
//		IntraGraphs graphs = new PointsToIntraGraphs(new File(args[1]), args[2]);
		IntraGraphs graphs = new TypeStateCheckingIntraGraphs(new File(args[1]), args[2]);
		graphs.readIntraGraphs(args[3]);
		graphs.readCallFile(args[5]);
//		System.out.println(graphs.getGraphs().toString());

		System.out.println();
		System.out.println("Total number of intra graphs:\t" + graphs.getGraphs().size());
		System.out.println();
		System.out.println("Total number of call sites:\t" + getTotalNumCallSites(graphs.getGraphs()));
		System.out.println();
		System.out.println("Total number of intra edges:\t" + graphs.getEdgeNum());
		System.out.println();
		//inline
		//GraphInliner inliner = new GraphInliner(callGraphParser, graphs);
		TypeStateGraphInliner inliner = new TypeStateGraphInliner(callGraphParser, graphs);
		inliner.inline();
//		System.out.println(inliner.getGraphsMap());
		
		//export dataflow graphs
//		GraphExporter exporter = new NullPointerDataflowExporter(inliner, new File(args[4]), graphs.getZeroIDs(), graphs.getAuxiliaryEdges());
		TypeStateCheckingExporter exporter = new TypeStateCheckingExporter(inliner, new File(args[4]));
		
		//export points to  graphs
//		GraphExporter exporter = new PointsToExporter(inliner, new File(args[3]), graphs.getEdgesLists());
		exporter.export();

		System.out.println("\n\n");
		System.out.println("Total number of edges:\t" + exporter.getTotal_edge_num());
		System.out.println("\n\n");
		System.out.println("Total number of graphs:\t" + exporter.getSizeMap().size());
		System.out.println("\n\n");
//		System.out.println(sortMap(exporter.getSizeMap()));
	}


	
	private static int getTotalNumCallSites(Map<IntraGraphIdentifier, IntraGraph> graphs) {
		// TODO Auto-generated method stub
		int num = 0;
		for(IntraGraphIdentifier id: graphs.keySet()){
			num += graphs.get(id).getCallSites().size();
		}
		return num;
	}

	private static List<Map.Entry<IntraGraphIdentifier, Integer>> sortMap(Map<IntraGraphIdentifier, Integer> outMap) {
		// TODO Auto-generated method stub
		List<Map.Entry<IntraGraphIdentifier, Integer>> outList = new ArrayList<Map.Entry<IntraGraphIdentifier, Integer>>(outMap.entrySet());
		Collections.sort(outList, new Comparator<Entry<IntraGraphIdentifier, Integer>>(){

			@Override
			public int compare(Entry<IntraGraphIdentifier, Integer> o1, Entry<IntraGraphIdentifier, Integer> o2) {
				// TODO Auto-generated method stub
				return o2.getValue().compareTo(o1.getValue());
			}
			
		});
		
		return outList;
	}

	
	
}

package edu.uci.inline.intragraphs;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import edu.uci.inline.datastructures.CallSite;
import edu.uci.inline.datastructures.GraphVertex;
import edu.uci.inline.intragraph.IntraGraph;
import edu.uci.inline.intragraph.PointsToIntraGraph;

public class PointsToIntraGraphs extends IntraGraphs {
	private final String FILE_START = "*File Name*:";
	private final String FUNCTION_START = "*Function Name*:";
	private final String FORMAL_PARAMS = "*Formal Params*:";
	private final String FORMAL_RETURNS = "*ReturnStmt*:";
	private final String CALL_SITE = "*CallSite*:";
	private final String VERTICES_INFO = "*Vertices*:";
	private final String EDGE_INFO = "*Edges*:";
	private static final String EMPTY_VERTEX = "-1";
	
	public PointsToIntraGraphs(File dir){
		super(dir);
	}

	@Override
	protected IntraGraph createIntraGraphFromFile(File intraFile) {
		// TODO Auto-generated method stub
		return new PointsToIntraGraph(intraFile);
	}

	@Override
	protected void readIntraGraphsFromSingleFile() {
		
		ArrayList<IntraGraph> intraGraphs = new ArrayList<IntraGraph>();
		BufferedReader ins;
		
		try {
			ins = new BufferedReader(new InputStreamReader(new FileInputStream(this.intra_graph_file)));
			String ln;
			int vertexSt = 0, vertexEnd = 0;
			
			PointsToIntraGraph intra= null;
			HashMap<String, GraphVertex> vertexMap = null;
			
			int count = 0;
			
			while ((ln = ins.readLine()) != null) { 
				// begining of a new intra graph
				if(ln.startsWith("********Intra Graph********")) {
					intra = new PointsToIntraGraph();
					intraGraphs.add(intra);
					vertexMap = new HashMap<String, GraphVertex>();
//					System.out.println(count++);
				}
				
				// file name
				if(ln.startsWith("*File Name*:")) {
//					System.out.println(ln.substring(FILE_START.length()));
					intra.getIdentifier().setFileName(ln.substring(FILE_START.length()));
				}
				
				// function name
				if(ln.startsWith("*Function Name*:")) {
					intra.getIdentifier().setCallName(ln.substring(FUNCTION_START.length()));
				}
				
				// formal params
				if(ln.startsWith("*Formal Params*:")) {
					String formal = ln.substring(FORMAL_PARAMS.length());
					String[] params = formal.split("\t");
					// get size of formal params
					int size = Integer.parseInt(params[0]);
					if(size > 0) {
						for(int i = 0; i < size; i++) {
							GraphVertex v = getVertex(params[i + 1], vertexMap);
							if(v != null)
								intra.getFormal_parameters().add(v);
						}
					}
				}
				
				// formal returns
				if(ln.startsWith("*ReturnStmt*:")) {
					String str = ln.substring(FORMAL_RETURNS.length());
					String[] returns = str.split("\t");
					// get size of formal returns
					int size = Integer.parseInt(returns[0]);
					if(size > 0) {
						for(int i = 0; i < size; i++) {
							GraphVertex v = getVertex(returns[i + 1], vertexMap);
							if(v != null)
								intra.getFormal_returns().add(v);
						}
					}
				}
				
				// call sites
				if(ln.startsWith("*CallSite*:")) {
					String call = ln.substring(CALL_SITE.length());
					String[] list = call.split("\t");
					CallSite callSite = new CallSite();
					intra.getCallSites().add(callSite);
					// function name
					callSite.getCallee().setCallName(list[0]);
					callSite.getCallee().setFileName(list[1]);
					
					// return value. -1 if no return
					GraphVertex vActualReturns = getVertex(list[1], vertexMap);
					if(vActualReturns != null)
						callSite.getActualReturns().add(vActualReturns);
					
					// get size of args
					int size = Integer.parseInt(list[2]);
					if(size > 0) {
						GraphVertex vActualArgs;
						for(int i = 0; i < size; i++) {
							vActualArgs = getVertex(list[i + 3], vertexMap);
							if(vActualArgs != null)
								callSite.getActualArgs().add(vActualArgs);
						}
					}
					
				}
				
				// vertices
				if(ln.startsWith("*Vertices*:")) {
					String str = ln.substring(VERTICES_INFO.length());
					String[] verticesInfo = str.split("\t");
					vertexSt = Integer.parseInt(verticesInfo[0]);
					vertexEnd = Integer.parseInt(verticesInfo[1]);
					
					// no edges
					if(vertexSt == vertexEnd)
						continue;
					for(int index = vertexSt; index <= vertexEnd; index++) {
						GraphVertex v = getVertex(Integer.toString(index), vertexMap);
						if(v != null) {
							intra.getAdjacencyList().put(v, new ArrayList<GraphVertex>());
							this.edgesLists.put(v, new ArrayList<Boolean>());
						}
					}
					
					ln = ins.readLine();
					assert(ln.startsWith("*Edges*:"));
					String edgeInfo = ln.substring(EDGE_INFO.length());
					// no edges
					if(edgeInfo.isEmpty())
						continue;
					String[] edges = edgeInfo.split("\t");
					
					// remove duplicate edges
					HashSet<String> edgeSet = removeDupliateEdges(edges);
					
					for(String edge : edgeSet) {
						String[] e = edge.split(" ");
						// use boolean edge value here to save memory
						boolean edgeVal;
						if(e[2].equals("A"))
							edgeVal = false;
						else if(e[2].equals("D")) {
							edgeVal = true;
						} else
							throw new RuntimeException("Edge Weight Error!");
						
						GraphVertex srcVertex = getVertex(e[0], vertexMap);
						if(srcVertex != null) {
							ArrayList<GraphVertex> adjList = intra.getAdjacencyList().get(srcVertex);
							ArrayList<Boolean> edgeList = this.edgesLists.get(srcVertex);
							assert(adjList != null && edgeList != null);
							GraphVertex dstVertex = getVertex(e[1], vertexMap);
							if(dstVertex != null) {
								// add vertex
								adjList.add(dstVertex);
								//add each edge to edgelist
								edgeList.add(edgeVal);
							}
						}
						
					}
				}
			}
			ins.close();
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
		} catch (NumberFormatException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		for(IntraGraph a : intraGraphs) {
			this.graphs.put(a.getIdentifier(), a);
		}
	}
	
	private HashSet<String> removeDupliateEdges(String[] edges) {
		HashSet<String> edgeSet = new HashSet<String>();
		for(String edge: edges){
			edgeSet.add(edge);
		}
		return edgeSet;
	}
	
	private GraphVertex getVertex(String vertexStr, HashMap<String, GraphVertex> vertexMap) {
		GraphVertex v = null;
		// if this is empty vertex
		if(vertexStr.equals(EMPTY_VERTEX))
			return null;
		
		if(!vertexMap.containsKey(vertexStr)) {
			v = new GraphVertex(Integer.parseInt(vertexStr));
			vertexMap.put(vertexStr, v);
			
		} else {
			v = vertexMap.get(vertexStr);
		}
		
		return v;
	}
	
}

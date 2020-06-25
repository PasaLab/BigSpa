package edu.uci.inline.intragraphs;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.uci.inline.datastructures.CallSite;
import edu.uci.inline.datastructures.GraphVertex;
import edu.uci.inline.intragraph.IntraGraph;
import edu.uci.inline.intragraph.NullPointerDataflowIntraGraph;
import edu.uci.inline.intragraphs.NullPointerDataflowIntraGraphs.Entry;

public class NullPointerDataflowIntraGraphs extends IntraGraphs {
	
	private static final String ZERO_ENTRY = "-1:-1:-1:0";

//	public static final String HOME_DIR = "/home/kai/workspace/llvmlinux/targets/x86_64/src/linux/";
	public static final String HOME_DIR = "";
	
	//string to indicate the start of an intra graph
	public static final String INTRA_GRAPH_START = "------------------------IntraGraph------------------------";

	//unique index for vertex
	public static int vertex_index = 0;
	
	
//	public static final String EntryLineNum = "-1";
//	public static final String EntryColumnNum = "-1";
//	public static final String ExitLineNum = "-2";
//	public static final String ExitColumnNum = "-2";
	
	public static final String NothingVarIndex = "-1";
	public static final String WrongVarIndex = "-2";
	public static final String TempReturnVarIndex = "-3";
	
	public static final String EmptySet = "Z";
	
	public static final String InnerSeparator = ":";
	public static final String NodeSeparator = ";";
	public static final String EdgeSeparator = "\t";
	
	
//	private final boolean vertex_info_flag;
	
	private final String package_name_interest;
	

	public NullPointerDataflowIntraGraphs(File input, String packageName) {
		super(input);
		this.zeroIDs = new HashSet<Integer>();
		this.auxiliaryEdges = new HashMap<Integer, HashSet<Integer>>();
		
		this.package_name_interest = packageName;
	}

	@Override
	protected IntraGraph createIntraGraphFromFile(File intraFile) {
		// TODO Auto-generated method stub
		return new NullPointerDataflowIntraGraph(intraFile);
	}
	
	@Override
	protected void readIntraGraphsFromSingleFile() {
		int count = 0;
		//builder to buffer the correspondence information between vertex id and vertex attribute info
		StringBuilder builder = new StringBuilder(501 * 1024 * 1024);
		
		BufferedReader reader = null;
		try {
			String line;
			reader = new BufferedReader(new FileReader(this.intra_graph_file));
			while((line = reader.readLine()) != null){
				//read intra graph one by one
				assert(line.startsWith(INTRA_GRAPH_START) || line.equals(""));
				if(line.startsWith(INTRA_GRAPH_START)){
//					System.out.print(++count + "\t");
					
					//load one intra graph
					IntraGraph intraGraph = readOneIntraGraph(reader, builder, true);
					
					if(intraGraph.getIdentifier().getFileName().startsWith(this.package_name_interest)){
						this.graphs.put(intraGraph.getIdentifier(), intraGraph);
					}
					
					//export vertex info to file
					if(builder.length() >= 500 * 1024 * 1024){
						exportVertexInfoToFile(builder);
						builder = new StringBuilder(501 * 1024 * 1024);
					}
					
				}
			}
			System.out.println();
			reader.close();
		}  
		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		finally{
			if(reader != null){
				try {
					reader.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			//export the vertex info to file
			exportVertexInfoToFile(builder);
		}
		
	}

	/**
	 * @param info
	 * @return 
	 */
	private void exportVertexInfoToFile(StringBuilder info){
		PrintWriter out = null;
		try{
			out = new PrintWriter(new BufferedWriter(new FileWriter(new File(intra_graph_file.getParentFile(), "vertex_info.txt"), true)));
			out.println(info.toString());
			out.close();
		}
		catch(IOException e){
			e.printStackTrace();
		}
		finally{
			out.close();
		}
		
	}
	
	private IntraGraph readOneIntraGraph(BufferedReader reader, StringBuilder vertex_Info, boolean append_flag) throws IOException{
		if(append_flag){
			vertex_Info.append(INTRA_GRAPH_START).append("\n");
		}
		
		//map storing <node_string, node_id> pair
		Map<String, Integer> idMap = new HashMap<String, Integer>();
		
		//map storing <node_id, node_instance> pair, to avoid instance duplication of GraphVertex
		Map<Integer, GraphVertex> vertexMap = new HashMap<Integer, GraphVertex>();
		
		IntraGraph intraGraph = new NullPointerDataflowIntraGraph();
		
		//checked variable indices in this intra-graph
		Set<Integer> checked_variables_index = new HashSet<Integer>();
		
		Entry entry = new Entry();
		
		String line;
		while((line = reader.readLine()) != null){
			if(line.startsWith("File:")){
				intraGraph.getIdentifier().setFileName(getContent(line, "[", "]").replaceFirst(HOME_DIR, ""));
				if(append_flag){
					vertex_Info.append(line.replaceFirst(HOME_DIR, "")).append("\n");
				}
			}
			else if(line.startsWith("Function:")){
				intraGraph.getIdentifier().setCallName(getContent(line, "[", "]"));
				if(append_flag){
					vertex_Info.append(line).append("\n");
				}
			}
			else if(line.startsWith("Range:")){
//				intraGraph.getIdentifier().setSourceRange(getContent(line, "[", "]"));
				if(append_flag){
					vertex_Info.append(line).append("\n");
				}
			}
			else if(line.startsWith("Variables:")){
				if(append_flag){
					vertex_Info.append(line).append("\n");
				}
			}
			else if(line.startsWith("Checked Variables:")){
				//read checked variables
				String[] vars = getContentAndSplit(line, "[", "]", "\t");
				for(String var: vars){
					int index = Integer.parseInt(var.split(":")[0]);
					if(index != -2){//wrong index
						checked_variables_index.add(index);
					}
				}
			}
			else if(line.startsWith("Formal Parameters:")){
				String[] paras = getContentAndSplit(line, "[", "]", "\t");
				addGraphVerticesToList(paras, intraGraph.getFormal_parameters(), idMap, vertexMap, checked_variables_index, entry);
			}
			else if(line.startsWith("Formal Returns:")){
				String[] returns = getContentAndSplit(line, "[", "]", "\t");
				addGraphVerticesToList(returns, intraGraph.getFormal_returns(), idMap, vertexMap, checked_variables_index, entry);
			}
			else if(line.startsWith("Call Sites:")){
				String[] components = getContentAndSplit(line, "[", "]", "\t");
				assert(components.length >= 3);
				
				CallSite callSite = new CallSite();
				
				//set callee identifier
				if(components.length == 4){
					callSite.getCallee().setFileName(components[0].replaceFirst(HOME_DIR, ""));
				}
				else{
					callSite.getCallee().setFileName("");
				}
				callSite.getCallee().setCallName(components[components.length - 3]);
				
				//add actual arguments
				String[] args = getContentAndSplit(components[components.length - 2], "<", ">", ";");
				addGraphVerticesToList(args, callSite.getActualArgs(), idMap, vertexMap, checked_variables_index, entry);
				
				//add actual returns
				String[] returns = getContentAndSplit(components[components.length - 1], "<", ">", ";");
				addGraphVerticesToList(returns, callSite.getActualReturns(), idMap, vertexMap, checked_variables_index, entry);
				
				intraGraph.getCallSites().add(callSite);
			}
			else if(line.startsWith("Identity Edges:")){
				
			}
			else if(line.startsWith("Edges:")){
				Map<GraphVertex, ArrayList<GraphVertex>> adjacencyList = intraGraph.getAdjacencyList();
				
				String[] edges = getContentAndSplit(line, "[", "]", "\t");
				//filter duplicated edges just in case
				Set<String> edges_set = filterDuplication(edges);
				for(String edge: edges_set){
					String[] nodes = edge.split(";");
					assert(nodes.length == 2);
					
					//source vertex
					String fromNode = nodes[0];
					GraphVertex srcNode = getGraphVertex(fromNode, idMap, vertexMap, checked_variables_index, entry);
					
					//destination vertex
					String toNode = nodes[1];
					GraphVertex dstNode = getGraphVertex(toNode, idMap, vertexMap, checked_variables_index, entry);
					
					//add edge to adjacency list
					if(adjacencyList.containsKey(srcNode)){
						adjacencyList.get(srcNode).add(dstNode);
					}
					else{
						ArrayList<GraphVertex> list = new ArrayList<GraphVertex>();
						list.add(dstNode);
						adjacencyList.put(srcNode, list);
					}
				}
			}
			else if(line.startsWith("Deref Nodes:")){
				//TODO in query
				if(append_flag){
					vertex_Info.append(line).append("\n");
				}
			}
			else if(line.equals("")){
				if(append_flag){
					vertex_Info.append(line);
				}
				break;
			}
			else{
				throw new RuntimeException("Wrong Line!!!");
			}
		}
		
		//store vertex correspondence info
		if(append_flag){
			vertex_Info.append("Vertices: [");
			for(String node: idMap.keySet()){
				int id = idMap.get(node);
				vertex_Info.append(id).append(";").append(node).append("\t");
			}
			vertex_Info.append("]\n\n");
		}
		
		//collect auxiliary edges
		if(intraGraph.getIdentifier().getFileName().startsWith(this.package_name_interest) && !entry.getSet().isEmpty()){
			this.auxiliaryEdges.put(entry.getKey(), entry.getSet());
		}
		
		
		return intraGraph;
	}

	/**filter out the potential duplicated edges
	 * @param edges
	 * @return
	 */
	public static Set<String> filterDuplication(String[] edges) {
		Set<String> edges_set = new HashSet<String>();
		for(String edge: edges){
			edges_set.add(edge);
		}
		return edges_set;
	}

	/**get the instance of GraphVertex for specific node
	 * @param node
	 * @param idMap
	 * @param vertexMap
	 * @param checked_variables_index 
	 * @return
	 */
	private GraphVertex getGraphVertex(String node, Map<String, Integer> idMap, Map<Integer, GraphVertex> vertexMap, Set<Integer> checked_variables_index, Entry entry) {
		int id;
		if(idMap.containsKey(node)){
			id = idMap.get(node);
		}
		else{
			id = vertex_index++;
			idMap.put(node, id);
		}
		
		//add the unique ids of zero entry node for further usage
		if(node.equals(ZERO_ENTRY)){
			this.zeroIDs.add(id);
			entry.setKey(id);
		}
		
		//collect auxiliary info
		String[] comps = node.split(":");
//		assert(comps.length == 4);
		String index_string = comps[2];
		int index = Integer.parseInt(index_string);
		if(checked_variables_index.contains(index)){
			entry.getSet().add(id);
		}
		
		GraphVertex vertex = GraphVertex.createInstance(id, 0, vertexMap);
		return vertex;
	}

	/**add vertices to the corresponding list
	 * @param nodes
	 * @param list
	 * @param idMap
	 * @param vertexMap
	 * @param checked_variables_index 
	 * @param entry 
	 */
	private void addGraphVerticesToList(String[] nodes, List<GraphVertex> list, Map<String, Integer> idMap, Map<Integer, GraphVertex> vertexMap, Set<Integer> checked_variables_index, Entry entry) {
		for(String node: nodes){
			if(isNothingNode(node)){
				list.add(null);
			}
			else{
				list.add(getGraphVertex(node, idMap, vertexMap, checked_variables_index, entry));
			}
		}
	}

	/**whether node is NothingVarIndex
	 * @param node
	 * @return
	 */
	private boolean isNothingNode(String node) {
		// TODO Auto-generated method stub
		String[] comps = node.split(InnerSeparator);
		assert(comps.length == 4);
		String index = comps[comps.length - 1];
		if(index.equals(NothingVarIndex)){
			return true;
		}
		else{
			return false;
		}
	}

	/**get the content of a line
	 * @param line
	 * @param start
	 * @param end
	 * @return
	 */
	public static String getContent(String line, String start, String end) {
		return line.substring(line.indexOf(start) + 1, line.lastIndexOf(end)).trim();
	}
	
	
	public static String[] getContentAndSplit(String line, String start, String end, String split){
		String[] splits = getContent(line, start, end).split(split);
		List<String> list = new ArrayList<String>();
		for(String s: splits){
			if(!s.equals("")){
				list.add(s);
			}
		}
		String[] results = new String[list.size()];
		for(int i = 0; i < list.size(); i++){		
			results[i] = list.get(i);
		}
		return results;
	}
	

	public static void main(String[] args) {
		IntraGraphs graphs = new NullPointerDataflowIntraGraphs(new File("refined_out.txt"), "m");
		graphs.readIntraGraphs("File");
		System.out.println(graphs.getGraphs().toString());
	}
	
	
	static class Entry{
		private int key;
		private final HashSet<Integer> set;
		
		public Entry(){
			this.key = -1;
			this.set = new HashSet<Integer>();
		}
		
		public int getKey() {
			return key;
		}
		public void setKey(int key) {
			this.key = key;
		}
		public HashSet<Integer> getSet() {
			return set;
		}
		
		
	}

}

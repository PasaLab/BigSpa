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
import edu.uci.inline.datastructures.IntraGraphIdentifier;
import edu.uci.inline.datastructures.TypeStateEdge;
import edu.uci.inline.intragraph.IntraGraph;
import edu.uci.inline.intragraph.TypeStateCheckingIntraGraph;
import edu.uci.inline.intragraphs.NullPointerDataflowIntraGraphs.Entry;

public class TypeStateCheckingIntraGraphs extends IntraGraphs {

	public static final String HOME_DIR = "";
	// string to indicate the start of an intra graph
	public static final String INTRA_GRAPH_START = "------------------------IntraGraph------------------------";

	// unique index for vertex
	public static int vertex_index = 0;

	public static final String NothingVarIndex = "-1";

	public static final String EmptySet = "Z";

	public static final String InnerSeparator = ":";
	public static final String NodeSeparator = ";";
	public static final String EdgeSeparator = "\t";

	private final String package_name_interest;

	private static Map<String, GraphVertex> callNodeMap = new HashMap<String, GraphVertex>();

	private static Map<String, List<TypeStateEdge>> calledges = new HashMap<String, List<TypeStateEdge>>();

	public TypeStateCheckingIntraGraphs(File input, String packageName) {
		super(input);
		this.zeroIDs = new HashSet<Integer>();
		this.typeStateEdges = new HashMap<Integer, List<TypeStateEdge>>();

		this.package_name_interest = packageName;
	}

	@Override
	protected IntraGraph createIntraGraphFromFile(File intraFile) {
		// TODO Auto-generated method stub
		return new TypeStateCheckingIntraGraph(intraFile);
	}

	@Override
	protected void readIntraGraphsFromSingleFile() {
		// builder to buffer the correspondence information between vertex id
		// and vertex attribute info
		StringBuilder builder = new StringBuilder(501 * 1024 * 1024);

		BufferedReader reader = null;
		try {
			String line;
			reader = new BufferedReader(new FileReader(this.intra_graph_file));
			int i = 0;
			int edgenum = 0;
			while ((line = reader.readLine()) != null) {
				// read intra graph one by one

//				if (i > 2000)
//					break;
				assert(line.startsWith(INTRA_GRAPH_START) || line.equals(""));
				if (line.startsWith(INTRA_GRAPH_START)) {
					// System.out.print(++count + "\t");

					// load one intra graph
					IntraGraph intraGraph = readOneIntraGraph(reader, builder, true);
					// System.out.println(i);
					i++;
					if (intraGraph.getIdentifier().getCallName() != null) {
						// not package_name_interest checking
						if (this.graphs.containsKey(intraGraph.getIdentifier())) {
							System.out.println(intraGraph.getIdentifier().getFileName()+":"+intraGraph.getIdentifier().getCallName());
						}
						this.graphs.put(intraGraph.getIdentifier(), intraGraph);
						edgenum += intraGraph.getEdgeNum();
//						System.out.println(this.graphs.size());
					} else {
						System.out.println(i);
					}
					// export vertex info to file
					if (builder.length() >= 500 * 1024 * 1024) {
						exportVertexInfoToFile(builder);
						builder = new StringBuilder(501 * 1024 * 1024);
					}

				}
			}

			System.out.println("edgenum: " + edgenum + "; index:" + i);
//			System.out.println(funcnum);
			reader.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (reader != null) {
				try {
					reader.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

			// export the vertex info to file
			exportVertexInfoToFile(builder);
		}
	}

	public void readCallFile(String callfile) {
		// add call edges to callsite
		try {
			File callFile = new File(callfile);
			if (!callFile.exists())
				System.exit(0);
			BufferedReader callreader = new BufferedReader(new FileReader(callFile));
			String line;
			while ((line = callreader.readLine()) != null) {
				String[] edges = line.split("\t");
				assert(edges.length == 5);
				String callNum = edges[0];
				GraphVertex srcNode = callNodeMap.get(edges[1]);
				GraphVertex dstNode = callNodeMap.get(edges[2]);
//				if(dstNode == null){
//					System.out.println();
//				}
				TypeStateEdge typeStateEdge = new TypeStateEdge(srcNode, dstNode, edges[3], edges[4]);
				if (calledges.containsKey(callNum))
					calledges.get(callNum).add(typeStateEdge);
				else {
					List<TypeStateEdge> list = new ArrayList<TypeStateEdge>();
					list.add(typeStateEdge);
					calledges.put(callNum, list);
				}
			}
			callreader.close();

			for (IntraGraphIdentifier id : graphs.keySet()) {
				for (CallSite callsite : graphs.get(id).getCallSites()) {
					addCallEdges(callsite);
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public void addCallNodeMap(String key, GraphVertex value) {
		if (!callNodeMap.containsKey(key)) {
			callNodeMap.put(key, value);
		} else {
			assert(callNodeMap.get(key).equals(value));
		}
	}

	public void addCallNodeMap(String[] keylist, List<GraphVertex> valuelist) {
		assert(keylist.length == valuelist.size());
		for (int i = 0; i < keylist.length; i++) {
			String key = keylist[i];
			if (!callNodeMap.containsKey(key)) {
				callNodeMap.put(key, valuelist.get(i));
			} else {
				assert(callNodeMap.get(key).equals(valuelist.get(i)));
			}
		}
	}

	/**
	 * @param info
	 * @return
	 */
	private void exportVertexInfoToFile(StringBuilder info) {
		PrintWriter out = null;
		try {
			out = new PrintWriter(new BufferedWriter(
					new FileWriter(new File(intra_graph_file.getParentFile(), "vertex_info.txt"), true)));
			out.println(info.toString());
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			out.close();
		}

	}
//int funcnum = 0;
	private IntraGraph readOneIntraGraph(BufferedReader reader, StringBuilder vertex_Info, boolean append_flag)
			throws IOException {
		if (append_flag) {
			vertex_Info.append(INTRA_GRAPH_START).append("\n");
		}

		// map storing <node_string, node_id> pair
		Map<String, Integer> idMap = new HashMap<String, Integer>();

		// map storing <node_id, node_instance> pair, to avoid instance
		// duplication of GraphVertex
		Map<Integer, GraphVertex> vertexMap = new HashMap<Integer, GraphVertex>();

		IntraGraph intraGraph = new TypeStateCheckingIntraGraph();

		// checked variable indices in this intra-graph
		Set<Integer> checked_variables_index = new HashSet<Integer>();

		Entry entry = new Entry();

		String line;
		while ((line = reader.readLine()) != null) {
			if (line.startsWith("File:")) {
				intraGraph.getIdentifier().setFileName(getContent(line, "[", "]").replaceFirst(HOME_DIR, ""));
//				intraGraph.getIdentifier().setFileName(line.split(": ")[1]);
				if (append_flag) {
					vertex_Info.append(line.replaceFirst(HOME_DIR, "")).append("\n");
				}
			} else if (line.startsWith("Function:")) {
				intraGraph.getIdentifier().setCallName(getContent(line, "[", "]"));
				if (append_flag) {
					vertex_Info.append(line).append("\n");
				}
//				if(line.contains("writeLocalJobFile(org.apache.hadoop.fs.Path,org.apache.hadoop.mapred.JobConf)"))
//
//					System.out.println(line);
//				funcnum++;
			} else if (line.startsWith("Range:")) {
				// intraGraph.getIdentifier().setSourceRange(getContent(line,
				// "[", "]"));
				if (append_flag) {
					vertex_Info.append(line).append("\n");
				}
			} else if (line.startsWith("Variables:")) {
				if (append_flag) {
					vertex_Info.append(line).append("\n");
				}
			} else if (line.startsWith("Checked Variables:")) {
				// read checked variables
				String[] vars = getContentAndSplit(line, "[", "]", "\t");
				for (String var : vars) {
					int index = Integer.parseInt(var.split(":")[0]);
					if (index != -2) {// wrong index
						checked_variables_index.add(index);
					}
				}
			} else if (line.startsWith("Formal Parameters:")) {
				String[] paras = getContentAndSplit(line, "[", "]", "\t");
				addGraphVerticesToList(paras, intraGraph.getFormal_parameters(), idMap, vertexMap,
						checked_variables_index, entry);
				// addCallNodeMap(paras, intraGraph.getFormal_parameters());
			} else if (line.startsWith("Formal Returns:")) {
				String[] returns = getContentAndSplit(line, "[", "]", "\t");
				addGraphVerticesToList(returns, intraGraph.getFormal_returns(), idMap, vertexMap,
						checked_variables_index, entry);
				// addCallNodeMap(returns, intraGraph.getFormal_returns());
			} else if (line.startsWith("Call Sites:")) {
				String[] split = line.split(": ");
				assert(split.length >= 3);
				String callNum = split[1];
				String[] components = getContentAndSplit(line, "[", "]", "\t");
				assert(components.length >= 3);

				CallSite callSite = new CallSite();
				callSite.setCallNum(callNum);

				// set callee identifier
				// if (components.length == 5) {
				callSite.getCallee().setFileName(components[0].replaceFirst(HOME_DIR, ""));
				// } else {
				// callSite.getCallee().setFileName("");
				// }
				// callSite.getCallee().setReturnName(components[components.length
				// - 4]);
				callSite.getCallee().setCallName(components[components.length - 3]);

				// add actual arguments
				String[] args = getContentAndSplit(components[components.length - 2], "<", ">", ";");
				addGraphVerticesToList(args, callSite.getActualArgs(), idMap, vertexMap, checked_variables_index,
						entry);
						// addCallNodeMap(args, callSite.getActualArgs());

				// add actual returns
				String[] returns = getContentAndSplit(components[components.length - 1], "<", ">", ";");
				addGraphVerticesToList(returns, callSite.getActualReturns(), idMap, vertexMap, checked_variables_index,
						entry);
				// addCallNodeMap(returns, callSite.getActualReturns());

				intraGraph.getCallSites().add(callSite);
			} else if (line.startsWith("Identity Edges:")) {

			} else if (line.startsWith("Edges:")) {
				Map<GraphVertex, List<TypeStateEdge>> typeStateEdgeList = intraGraph.getTypeStateEdgeList();

				String[] edges = getContentAndSplit(line, "[", "]", "\t");
				// filter duplicated edges just in case
				Set<String> edges_set = filterDuplication(edges);
				for (String edge : edges_set) {
					String[] nodes = edge.split(";");
					assert(nodes.length == 4);

					// source vertex
					String fromNode = nodes[0];
					GraphVertex srcNode = getGraphVertex(fromNode, idMap, vertexMap, checked_variables_index, entry);

					// destination vertex
					String toNode = nodes[1];
					GraphVertex dstNode = getGraphVertex(toNode, idMap, vertexMap, checked_variables_index, entry);

					// one intra edge: source, destination, label, constraint
					TypeStateEdge typeStateEdge = new TypeStateEdge(srcNode, dstNode, nodes[2], nodes[3]);
					// TypeStateEdge typeStateEdge = new TypeStateEdge(srcNode,
					// dstNode, nodes[2], "");

					if (typeStateEdgeList.containsKey(srcNode)) {
						typeStateEdgeList.get(srcNode).add(typeStateEdge);
					} else {
						List<TypeStateEdge> list = new ArrayList<TypeStateEdge>();
						list.add(typeStateEdge);
						typeStateEdgeList.put(srcNode, list);
					}
				}
			} else if (line.startsWith("Deref Nodes:")) {
				// TODO in query
				if (append_flag) {
					vertex_Info.append(line).append("\n");
				}
			} else if (line.equals("")) {
				if (append_flag) {
					vertex_Info.append(line);
				}
				break;
			} else {
				throw new RuntimeException("Wrong Line!!!");
			}
		}

		// store vertex correspondence info
		if (append_flag) {
			vertex_Info.append("Vertices: [");
			for (String node : idMap.keySet()) {
				int id = idMap.get(node);
				vertex_Info.append(id).append(";").append(node).append("\t");
			}
			vertex_Info.append("]\n\n");
		}

		return intraGraph;
	}

	// add call edges to callsite
	public void addCallEdges(CallSite callsite) throws IOException {
		assert(calledges.containsKey(callsite.getCallNum()));
		for (TypeStateEdge edge : calledges.get(callsite.getCallNum())) {
			callsite.addCallEdge(edge);
		}
	}

	/**
	 * filter out the potential duplicated edges
	 * 
	 * @param edges
	 * @return
	 */
	public static Set<String> filterDuplication(String[] edges) {
		Set<String> edges_set = new HashSet<String>();
		for (String edge : edges) {
			edges_set.add(edge);
		}
		return edges_set;
	}

	/**
	 * get the instance of GraphVertex for specific node
	 * 
	 * @param node
	 * @param idMap
	 * @param vertexMap
	 * @param checked_variables_index
	 * @return
	 */
	private GraphVertex getGraphVertex(String node, Map<String, Integer> idMap, Map<Integer, GraphVertex> vertexMap,
			Set<Integer> checked_variables_index, Entry entry) {
		int id;
		if (idMap.containsKey(node)) {
			id = idMap.get(node);
		} else {
			id = vertex_index++;
			idMap.put(node, id);
		}

		GraphVertex vertex = GraphVertex.createInstance(id, 0, vertexMap);
		return vertex;
	}

	/**
	 * add vertices to the corresponding list
	 * 
	 * @param nodes
	 * @param list
	 * @param idMap
	 * @param vertexMap
	 * @param checked_variables_index
	 * @param entry
	 */
	private void addGraphVerticesToList(String[] nodes, List<GraphVertex> list, Map<String, Integer> idMap,
			Map<Integer, GraphVertex> vertexMap, Set<Integer> checked_variables_index, Entry entry) {
		for (String node : nodes) {
			// there is no nothing node in I/O checking
			// if (isNothingNode(node)) {
			// list.add(null);
			// } else {
			GraphVertex vertex = getGraphVertex(node, idMap, vertexMap, checked_variables_index, entry);
			list.add(vertex);
			addCallNodeMap(node, vertex);
			// }
		}
	}

	/**
	 * whether node is NothingVarIndex
	 * 
	 * @param node
	 * @return
	 */
	private boolean isNothingNode(String node) {
		// TODO Auto-generated method stub
		String[] comps = node.split(InnerSeparator);
		assert(comps.length == 4);
		String index = comps[comps.length - 1];
		if (index.equals(NothingVarIndex)) {
			return true;
		} else {
			return false;
		}
	}

	/**
	 * get the content of a line
	 * 
	 * @param line
	 * @param start
	 * @param end
	 * @return
	 */
	public static String getContent(String line, String start, String end) {
		return line.substring(line.indexOf(start) + 1, line.lastIndexOf(end)).trim();
	}

	public static String[] getContentAndSplit(String line, String start, String end, String split) {
		String[] splits = getContent(line, start, end).split(split);
		List<String> list = new ArrayList<String>();
		for (String s : splits) {
			if (!s.equals("")) {
				list.add(s);
			}
		}
		String[] results = new String[list.size()];
		for (int i = 0; i < list.size(); i++) {
			results[i] = list.get(i);
		}
		return results;
	}
}

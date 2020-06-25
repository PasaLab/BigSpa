package edu.uci.inline.export;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import edu.uci.inline.client.GraphInliner;
import edu.uci.inline.client.TypeStateGraphInliner;
import edu.uci.inline.datastructures.GraphVertex;
import edu.uci.inline.datastructures.IntraGraphIdentifier;
import edu.uci.inline.datastructures.TypeStateEdge;
import edu.uci.inline.intragraph.IntraGraph;

public class TypeStateCheckingExporter {

	// buffer limit
	protected static final int BUFFER_LIMIT = 501 * 1024 * 1024;

	protected static final String Delimiter = "\t";

	// global index for vertices in the whole forest
	private int index = 0;

	// index map
	protected Map<GraphVertex, Integer> indexMap;

	private final TypeStateGraphInliner inliner;

	private final File outputFile;

	// total number of edges in the whole forest after inlining
	private int total_edge_num = 0;

	// statistics information: the size for each graph after inlining
	private final Map<IntraGraphIdentifier, Integer> sizeMap;

	public TypeStateCheckingExporter(TypeStateGraphInliner inliner, File outFile) {
		this.indexMap = new HashMap<GraphVertex, Integer>();
		this.inliner = inliner;
		this.outputFile = outFile;

		this.sizeMap = new HashMap<IntraGraphIdentifier, Integer>();
	}

	public void export() {
		exportGraphs();
		// addAuxiliaryEdges();
		exportIndexInfo();
	}

	protected void generateOneEdge(IntraGraph intra, GraphVertex src, int src_index, GraphVertex dst, int dst_index,
			String label, String constraint, StringBuilder builder) {
//		builder.append(src_index).append(Delimiter).append(dst_index).append(Delimiter).append(label).append(Delimiter)
//				.append("[").append(constraint).append("]").append("\n");
		builder.append(src_index).append(Delimiter).append(dst_index).append(Delimiter).append(label).append("\n");
	}

	/**
	 * export the mapping information between vertex (id:version) and unique
	 * index
	 * 
	 */
	private void exportIndexInfo() {
		// TODO Auto-generated method stub
		PrintWriter out = null;
		try {
			out = new PrintWriter(new BufferedWriter(
					new FileWriter(new File(this.outputFile.getParentFile(), "index_map_info.txt"), true)));
			for (GraphVertex vertex : this.indexMap.keySet()) {
				if(vertex == null)
					continue;
				out.println(vertex.toString() + Delimiter + this.indexMap.get(vertex));
			}
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (out != null) {
				out.close();
			}
		}
	}

	/**
	 * export out graphs after inlining
	 * 
	 */
	public void exportGraphs() {
		StringBuilder builder = new StringBuilder(BUFFER_LIMIT);

		for (IntraGraphIdentifier id : inliner.getOutGraphs()) {
			IntraGraph graph = inliner.getGraphsMap().get(id);
			exportOneGraph(graph, builder);

			if (builder.length() >= BUFFER_LIMIT - 1024 * 1024) {
				writeOut(builder);
				builder = new StringBuilder(BUFFER_LIMIT);
			}
		}

		writeOut(builder);
	}

	/**
	 * export one graph
	 * 
	 * @param graph
	 * @param builder
	 */
	private void exportOneGraph(IntraGraph graph, StringBuilder builder) {
		int size_graph = 0;

		for (GraphVertex src : graph.getTypeStateEdgeList().keySet()) {
			int src_index = getIndex(src);
			List<TypeStateEdge> list = graph.getTypeStateEdgeList().get(src);

			// compute the total number of edges
			size_graph += list.size();

			for (TypeStateEdge edge : list) {
				assert(src.equals(edge.getStart()));
				GraphVertex dst = edge.getEnd();
				int dst_index = getIndex(dst);
				generateOneEdge(graph, src, src_index, dst, dst_index, edge.getLabel(), edge.getConstraint(), builder);
			}
		}

		this.total_edge_num += size_graph;
		this.sizeMap.put(graph.getIdentifier(), size_graph);
	}

	/**
	 * write graphs into output file
	 * 
	 * @param info
	 */
	protected void writeOut(StringBuilder info) {
		PrintWriter out = null;
		try {
			// if(!this.outputFile.getParentFile().exists()){
			// this.outputFile.getParentFile().mkdirs();
			// }
			out = new PrintWriter(new BufferedWriter(new FileWriter(this.outputFile, true)));
			out.println(info.toString());
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (out != null) {
				out.close();
			}
		}
	}

	/**
	 * get a unique index for a vertex in the final graph after inlining
	 * 
	 * @param vertex
	 * @return
	 */
	private int getIndex(GraphVertex vertex) {
		if (this.indexMap.containsKey(vertex)) {
			return this.indexMap.get(vertex);
		} else {
			int index = this.index++;
			this.indexMap.put(vertex, index);
			return index;
		}
	}

	public Map<GraphVertex, Integer> getIndexMap() {
		return indexMap;
	}

	public void setIndexMap(Map<GraphVertex, Integer> indexMap) {
		this.indexMap = indexMap;
	}

	public int getTotal_edge_num() {
		return total_edge_num;
	}

	public void setTotal_edge_num(int total_edge_num) {
		this.total_edge_num = total_edge_num;
	}

	public static int getBufferLimit() {
		return BUFFER_LIMIT;
	}

	public static String getDelimiter() {
		return Delimiter;
	}

	public TypeStateGraphInliner getInliner() {
		return inliner;
	}

	public File getOutputFile() {
		return outputFile;
	}

	public Map<IntraGraphIdentifier, Integer> getSizeMap() {
		return sizeMap;
	}
}

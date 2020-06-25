package edu.uci.inline.export;

import java.io.File;
import java.util.ArrayList;
import java.util.Map;

import edu.uci.inline.client.GraphInliner;
import edu.uci.inline.datastructures.GraphVertex;
import edu.uci.inline.intragraph.IntraGraph;

public class PointsToExporter extends GraphExporter {
	Map<GraphVertex, ArrayList<Boolean>> edgesLists = null;
	Map<GraphVertex, ArrayList<GraphVertex>> adjList = null;
	ArrayList<GraphVertex> adjVertices = null;
	ArrayList<Boolean> edgeValues = null;

	public PointsToExporter(GraphInliner inliner, File outFile) {
		super(inliner, outFile);
	}

	public PointsToExporter(GraphInliner inliner, File outFile, Map<GraphVertex, ArrayList<Boolean>> edgesLists) {
		super(inliner, outFile);
		this.edgesLists = edgesLists;
	}

	@Override
	protected void generateOneEdge(IntraGraph intra, GraphVertex src, int src_index, GraphVertex dst, int dst_index,
			StringBuilder builder) {

		GraphVertex tempSrc = new GraphVertex(src.getId(), 0);
		boolean val;
		if (edgesLists.containsKey(tempSrc)) {
			edgeValues = edgesLists.get(tempSrc);
			adjList = intra.getAdjacencyList();
			assert (edgeValues != null && adjList != null);
			if (adjList.containsKey(src)) {
				adjVertices = adjList.get(src);
				for (int index = 0; index < adjVertices.size(); index++) {
					if (dst.getId() == adjVertices.get(index).getId()) {
						if (index >= 0 && index < edgeValues.size()) {
							val = edgeValues.get(index);
						} else {
							val = false;
						}

						if (val) {
							builder.append(src_index).append(Delimiter).append(dst_index).append(Delimiter).append("d")
									.append(Delimiter).append("\n");

							// add reverse edge
							builder.append(dst_index).append(Delimiter).append(src_index).append(Delimiter).append("-d")
									.append(Delimiter).append("\n");
						} else {
							builder.append(src_index).append(Delimiter).append(dst_index).append(Delimiter).append("a")
									.append(Delimiter).append("\n");

							// add reverse edge
							builder.append(dst_index).append(Delimiter).append(src_index).append(Delimiter).append("-a")
									.append(Delimiter).append("\n");
						}
						return;
					}
				}

				// can't find the vertex, this is an inter assignment
				builder.append(src_index).append(Delimiter).append(dst_index).append(Delimiter).append("a")
						.append(Delimiter).append("\n");

				// add reverse edge
				builder.append(dst_index).append(Delimiter).append(src_index).append(Delimiter).append("-a")
						.append(Delimiter).append("\n");
				return;

			} else {
				throw new RuntimeException("Src is not in adjList!");
			}

		} else {
			throw new RuntimeException("Src is not in edgesLists!");
		}

	}

}

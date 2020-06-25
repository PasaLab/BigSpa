package edu.uci.inline.export;

import java.io.File;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import edu.uci.inline.client.GraphInliner;
import edu.uci.inline.datastructures.GraphVertex;
import edu.uci.inline.intragraph.IntraGraph;

public class NullPointerDataflowExporter extends GraphExporter{
	
	private Set<Integer> zeroIDs;
	
	private Map<Integer, HashSet<Integer>> auxiliary_edges;

	public NullPointerDataflowExporter(GraphInliner inliner, File outFile, Set<Integer> zIDs, Map<Integer, HashSet<Integer>> aux_edges) {
		super(inliner, outFile);
		this.zeroIDs = zIDs;
		this.auxiliary_edges = aux_edges;
	}

	@Override
//	protected void generateOneEdge(GraphVertex src, int src_index, GraphVertex dst, int dst_index,
//			StringBuilder builder) {
//		// TODO Auto-generated method stub
//		builder.append(src_index).append(Delimiter).append(dst_index).append("\n");
//	}
	
	protected void generateOneEdge(IntraGraph intra, GraphVertex src, int src_index, GraphVertex dst, int dst_index,
			StringBuilder builder) {
		// TODO Auto-generated method stub
		if(this.zeroIDs.contains(src.getId())){
			builder.append(src_index).append(Delimiter).append(dst_index).append(Delimiter).append("n").append("\n");
		}
		else{
			builder.append(src_index).append(Delimiter).append(dst_index).append(Delimiter).append("e").append("\n");
		}
	}
	
	public void addAuxiliaryEdges(){
//		System.out.println("NullPointerDataflowExporter");
		StringBuilder builder = new StringBuilder(BUFFER_LIMIT);
		
		for(GraphVertex graphVertex: this.indexMap.keySet()){
			int id = graphVertex.getId();
			int version_num = graphVertex.getVersion_number();
			
			if(this.auxiliary_edges.containsKey(id)){
				assert(this.zeroIDs.contains(id));
				HashSet<Integer> dsts = this.auxiliary_edges.get(id);
				for(int dst: dsts){
					GraphVertex vertex = new GraphVertex(dst, version_num);
					if(this.indexMap.containsKey(vertex)){
						builder.append(this.indexMap.get(graphVertex)).append(Delimiter).append(this.indexMap.get(vertex)).append(Delimiter).append("m").append("\n");
					}
				}
				
				
				if(builder.length() >= BUFFER_LIMIT - 1024 * 1024){
					writeOut(builder);
					builder =  new StringBuilder(BUFFER_LIMIT);
				}
				
			}
		}
		
		writeOut(builder);
	}
}

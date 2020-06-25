package edu.uci.inline.datastructures;

import java.util.Map;

public class GraphVertex {
	
	//unique id
	private final int id;
	
	//number for recording the times of clones
	private final int version_number;
	
	public GraphVertex(int id, int version_num){
		this.id = id;
		this.version_number = version_num;
	}
	
	public GraphVertex(int id) {
		this.id = id;
		this.version_number = 0;
	}
	
	public static GraphVertex createInstance(int id, int version_num, Map<Integer, GraphVertex> map){
		if(map.containsKey(id)){
			return map.get(id);
		}
		else{
			GraphVertex vertex = new GraphVertex(id, version_num);
			map.put(id, vertex);
			return vertex;
		}
	}
	
	public int hashCode(){
		int hash = 17;
		hash = 31*hash + id;
		hash = 31*hash + version_number;
		
		return hash;
	}
	
	public boolean equals(Object obj){
		if(!(obj instanceof GraphVertex))
			return false;
		if(obj == this)
			return true;
		
		GraphVertex v = (GraphVertex)obj;
		return v.id == this.id && v.version_number == this.version_number;
	}

	public int getId() {
		return id;
	}

	public int getVersion_number() {
		return version_number;
	}
	
	@Override
	public String toString() {
		StringBuilder result = new StringBuilder();
		result.append(id).append(":").append(this.version_number);

		return result.toString();
	}
	
}

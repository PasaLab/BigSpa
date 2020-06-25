package edu.uci.inline.datastructures;

public class TypeStateEdge {
	
	private GraphVertex start;
	private GraphVertex end;
	private String label;
	private String constraint;
	
	public TypeStateEdge(){
		
	}
	
	public TypeStateEdge(GraphVertex start, GraphVertex end, String label, String constraint){
		this.start = start;
		this.end = end;
		this.label = label;
		this.constraint = constraint;
	}
	
	public GraphVertex getStart(){
		return start;
	}

	public GraphVertex getEnd(){
		return end;
	}
	
	public String getLabel(){
		return label;
	}
	
	public String getConstraint(){
		return constraint;
	}
}

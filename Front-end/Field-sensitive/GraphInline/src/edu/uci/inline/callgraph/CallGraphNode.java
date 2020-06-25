package edu.uci.inline.callgraph;

import java.util.ArrayList;
import java.util.List;

import edu.uci.inline.datastructures.IntraGraphIdentifier;

public class CallGraphNode {
	
	private final IntraGraphIdentifier caller;
	
	private final List<IntraGraphIdentifier> callees;

	public CallGraphNode(IntraGraphIdentifier caller, List<IntraGraphIdentifier> callees){
		this.caller = caller;
		this.callees = callees;
	}

	public IntraGraphIdentifier getCaller() {
		return caller;
	}

	public List<IntraGraphIdentifier> getCallees() {
		return callees;
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("caller: " + caller + "\t");
		sb.append("\n");
		sb.append("callees: ");
		for(IntraGraphIdentifier node : callees)
			sb.append(node + " ");
		sb.append("\n");
		
		return sb.toString();
	}
}

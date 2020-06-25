package edu.uci.inline.datastructures;

import java.util.ArrayList;
import java.util.List;

public class CallSite {

	private IntraGraphIdentifier callee;

	private List<GraphVertex> actualArgs;

	private List<GraphVertex> actualReturns;

	private List<TypeStateEdge> calledges;

	private String callNum;

	public CallSite(IntraGraphIdentifier callName, List<GraphVertex> returnValues, List<GraphVertex> actualArgs,
			String callNum) {
		this.callee = callName;
		this.actualReturns = returnValues;
		this.actualArgs = actualArgs;
		this.callNum = callNum;
	}

	public CallSite() {
		callee = new IntraGraphIdentifier();
		actualArgs = new ArrayList<GraphVertex>();
		actualReturns = new ArrayList<GraphVertex>();
		calledges = new ArrayList<TypeStateEdge>();
	}

	public IntraGraphIdentifier getCallee() {
		return callee;
	}

	public List<GraphVertex> getActualArgs() {
		return actualArgs;
	}

	public List<GraphVertex> getActualReturns() {
		return actualReturns;
	}

	public List<TypeStateEdge> getCallEdges() {
		return calledges;
	}

	public void addCallEdge(TypeStateEdge edge) {
		if (!calledges.contains(edge))
			calledges.add(edge);
	}

	public void setCallNum(String callnum) {
		this.callNum = callnum;
	}

	public String getCallNum() {
		return callNum;
	}

	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append(callee.toString()).append("").append(actualArgs.toString()).append(actualReturns.toString());
		return builder.toString();
	}

}

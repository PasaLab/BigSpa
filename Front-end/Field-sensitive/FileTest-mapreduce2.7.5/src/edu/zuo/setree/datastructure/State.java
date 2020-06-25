package edu.zuo.setree.datastructure;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import acteve.symbolic.integer.Expression;
import soot.Local;

public class State {

	private Map<Local, Expression> localsMap;

	public State() {
		localsMap = new LinkedHashMap<Local, Expression>();
	}

	public State(Map parentMap) {
		localsMap = new LinkedHashMap<Local, Expression>(parentMap);
	}

	public State(State s) {
		localsMap = new LinkedHashMap<Local, Expression>(s.localsMap);
	}

	public Map<Local, Expression> getLocalsMap() {
		return localsMap;
	}

	public void setLocalsMap(Map<Local, Expression> localsMap) {
		this.localsMap = localsMap;
	}

	public String toString() {
		return this.localsMap.toString();
	}

}

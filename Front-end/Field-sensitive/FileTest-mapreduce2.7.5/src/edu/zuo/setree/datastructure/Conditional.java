package edu.zuo.setree.datastructure;

import acteve.symbolic.integer.Expression;

public class Conditional {

	private final Expression condition;

	public Conditional(Expression cond) {
		this.condition = cond;
	}

	public Expression getCondition() {
		return this.condition;
	}

	public String toString() {
		return this.condition.toString();
		// return this.condition.toYicesString();
	}
}

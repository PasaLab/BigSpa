package edu.zuo.setree.datastructure;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import acteve.symbolic.integer.Expression;
import soot.Immediate;
import soot.Local;

public class CallSite {

	private String signature;

	private Tuple callee;

	private final Map<Immediate, Expression> argumentsMap;

	private Local retVar;

	private Expression retSym;

	public CallSite() {
		this.signature = null;
		this.callee = null;
		this.argumentsMap = new LinkedHashMap<Immediate, Expression>();
		this.retVar = null;
	}

	public String getSignature() {
		return signature;
	}

	public void setSignature(String signature) {
		this.signature = signature;
	}

	public Tuple getCallee() {
		return callee;
	}

	public String getCalleeString() {
		if (callee != null) {
			return callee.toString();
		} else
			return null;
	}

	public void setCallee(Immediate callee, Expression expr) {
		this.callee = new Tuple(callee, expr);
	}

	public Local getRetVar() {
		return retVar;
	}

	public void setRetVar(Local retVar) {
		this.retVar = retVar;
	}

	public Expression getRetSym() {
		return retSym;
	}

	public void setRetSym(Expression retSym) {
		this.retSym = retSym;
	}

	public void putArgsMap(Immediate im, Expression expr) {
		this.argumentsMap.put(im, expr);
	}

	public Map<Immediate, Expression> getArgumentsMap() {
		return argumentsMap;
	}

	static class Tuple {
		private final Immediate callee;

		private final Expression expression;

		public Tuple(Immediate callee, Expression expr) {
			this.callee = callee;
			this.expression = expr;
		}

		public String toString() {
			if (callee != null && expression != null) {
				return callee.toString() + " = " + expression.exprString();
			} else
				return null;
		}

	}
}

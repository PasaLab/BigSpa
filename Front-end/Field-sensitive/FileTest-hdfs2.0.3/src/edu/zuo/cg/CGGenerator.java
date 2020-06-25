package edu.zuo.cg;

import java.util.Map;

import soot.Scene;
import soot.SceneTransformer;
import soot.SootClass;
import soot.jimple.toolkits.callgraph.CallGraph;
import soot.util.Chain;

public class CGGenerator extends SceneTransformer {

	@Override
	protected void internalTransform(String phaseName, Map options) {
		// Chain<SootClass> classes = Scene.v().getApplicationClasses();
		// System.out.println("Application classes analyzed: " +
		// classes.toString());

		// call graph generation
		dump_call_graph();

	}

	private void dump_call_graph() {
		// TODO Auto-generated method stub
		CallGraph cg = Scene.v().getCallGraph();
		// TODO

	}

}

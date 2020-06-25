package edu.zuo.setree.export;

import java.io.*;
import java.util.*;

import acteve.symbolic.integer.*;
//import com.sun.org.apache.xpath.internal.operations.Bool;
import edu.zuo.setree.datastructure.CallSite;
import edu.zuo.setree.datastructure.StateNode;
import edu.zuo.setree.JSON.JSON;
import soot.*;

public class Exporter {

	public static final File setOutFile = new File(
			"intraOutput/set.conditional");
	public static final File stateNodeFile = new File(
			"intraOutput/stateNode.json");
	public static final File conditionalSmt2File = new File(
			"intraOutput/conditionalSmt2");
	public static final File consEdgeGraphFile = new File(
			"intraOutput/consEdgeGraph");
	public static final File var2indexMapFile = new File(
			"intraOutput/var2indexMap");

	private static Map<String, Stack<Long>> constraintEdgeMap = new LinkedHashMap<>();
	private static Map<String, Integer> var2indexMap = new LinkedHashMap<>();

	public static void run(StateNode root, Body mb) {
		// for debugging
		System.out.println("STATE ==>>");
		System.out.println("\n");

		// export symbolic execution tree info to output file
		System.out.println("Exporting...");
		export(root, mb);
		System.out.println("Finish exporting!!!");

	}

	private static void export(StateNode root, Body mb) {
		// printOutInfo(root,0);
		PrintWriter setOut = null;
		PrintWriter stateNodeOut = null;
		PrintWriter consEdgeGraphOut = null;
		PrintWriter var2indexMapOut = null;
		PrintWriter conditionalSmt2Out = null;
		try {
			if (!setOutFile.exists()) {
				setOutFile.createNewFile();
			}
			if (!stateNodeFile.exists()) {
				stateNodeFile.createNewFile();
			}
			if (!conditionalSmt2File.exists()) {
				conditionalSmt2File.createNewFile();
			}
			if (!consEdgeGraphFile.exists()) {
				consEdgeGraphFile.createNewFile();
			}
			if (!var2indexMapFile.exists()) {
				var2indexMapFile.createNewFile();
			}
			setOut = new PrintWriter(new BufferedWriter(new FileWriter(
					setOutFile, true)));
			stateNodeOut = new PrintWriter(new BufferedWriter(new FileWriter(
					stateNodeFile, true)));
			conditionalSmt2Out = new PrintWriter((new BufferedWriter(
					new FileWriter(conditionalSmt2File, true))));
			consEdgeGraphOut = new PrintWriter(new BufferedWriter(
					new FileWriter(consEdgeGraphFile, true)));
			var2indexMapOut = new PrintWriter(new BufferedWriter(
					new FileWriter(var2indexMapFile, true)));

			// Print Function Signature
			setOut.println(mb.getMethod().getSignature());
			stateNodeOut.println(mb.getMethod().getSignature());
			conditionalSmt2Out.println(mb.getMethod().getSignature());
			consEdgeGraphOut.println(mb.getMethod().getSignature());
			var2indexMapOut.println(mb.getMethod().getSignature());

			// Recursive
			System.out.println("Exporting set.conditional...");
			recursiveExport(root, 0, setOut);
			System.out.println("Exporting conditionalSmt2...");
			recursiveConditionalSmt2(root, 0, conditionalSmt2Out);
			System.out.println("Exporting stateNode.json...");
			recursiveStateNode(root, 0, stateNodeOut);
			//
			constraintEdgeMap.clear();
			var2indexMap.clear();
			System.out.println("Exporting consEdgeGraph...");
			recursiveConsEdgeGraph(root, 0, consEdgeGraphOut);
			System.out.println("Exporting var2indexMap...");
			printVar2indexMap(var2indexMapOut);

			// Output End
			setOut.println();
			stateNodeOut.println();
			conditionalSmt2Out.println();
			consEdgeGraphOut.println();
			var2indexMapOut.println();

			// Output close
			setOut.close();
			stateNodeOut.close();
			conditionalSmt2Out.close();
			consEdgeGraphOut.close();
			var2indexMapOut.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static void putVar2indexMap(String s) {
		if (!var2indexMap.containsKey(s)) {
			var2indexMap.put(s, var2indexMap.size());
		}
	}

	private static void recursiveExport(StateNode root, long index,
			PrintWriter out) {
		// termination
		if (root == null) {
			return;
		}

		// export operation
		if (root.getConditional() != null) {
			out.println(index + ":" + root.getConditional().toSmt2String());
		}

		// recursive operation
		recursiveExport(root.getFalseChild(), 2 * index, out);
		recursiveExport(root.getTrueChild(), 2 * index + 1, out);
	}

	private static void recursiveConditionalSmt2(StateNode root, long index,
			PrintWriter conditionalSmt2Out) {
		// termination
		if (root == null) {
			return;
		}

		// export operation
		if (root.getConditional() != null) {
			conditionalSmt2Out.println(index + ":"
					+ root.getConditional().toSmt2String());
			List<CallSite> callSites = root.getCallsites();
			if (callSites != null) {
				for (CallSite cs : callSites) {
					Map<Immediate, Expression> map = cs.getArgumentsMap();
					conditionalSmt2Out.print(index + ":#c#" + cs.getRetSym()
							+ "#" + cs.getSignature());
					for (Immediate im : map.keySet()) {
						conditionalSmt2Out.print("#"
								+ map.get(im).toSmt2String());
					}
					conditionalSmt2Out.println();
				}
			}
		}
		if (root.getReturnExpr() != null) {
			conditionalSmt2Out.println(index + ":#r#"
					+ root.getReturnExpr().toSmt2String());
		}
		if (index == 0) {
			Map<Local, Expression> localExpressionMap = root.getLocalsMap();
			conditionalSmt2Out.print(index + ":#p");
			for (Local l : localExpressionMap.keySet()) {
				String expr = localExpressionMap.get(l).toSmt2String();
				if (!expr.contains(" ") && expr.contains("@para")) {
					conditionalSmt2Out.print("#" + l.toString() + ":"
							+ localExpressionMap.get(l).toSmt2String());
				}
			}
			conditionalSmt2Out.println();
		}

		// recursive operation
		recursiveConditionalSmt2(root.getFalseChild(), 2 * index,
				conditionalSmt2Out);
		recursiveConditionalSmt2(root.getTrueChild(), 2 * index + 1,
				conditionalSmt2Out);
	}

	private static void recursiveStateNode(StateNode root, int index,
			PrintWriter stateNodeOut) {
		// termination
		if (root == null) {
			return;
		}

		// export stateNode
		List<String> stringList = new ArrayList<>();
		// print conditional
		if (root.getConditional() != null) {
			stringList.add(JSON.toJson("conditional", root.getConditional()
					.toString()));
		} else {
			stringList.add(JSON.toJson("conditional", null));
		}
		// print callSites
		if (root.getCallsites() != null) {
			List<String> callSiteStringList = new ArrayList<>();
			List<CallSite> callSiteList = root.getCallsites();
			for (int i = 0; i < callSiteList.size(); i++) {
				callSiteStringList.add(getCallSite(callSiteList.get(i)));
			}
			stringList.add(JSON.toJsonArray("callsites", callSiteStringList));
		} else {
			stringList.add(JSON.toJson("callsites", null));
		}
		// print returnExpr
		if (root.getReturnExpr() != null) {
			stringList.add(JSON.toJson("returnExp", root.getReturnExpr()
					.toString()));
		} else {
			stringList.add(JSON.toJson("returnExp", null));
		}
		stateNodeOut.println(JSON.toJsonSet("stateNode", stringList));

		// recursive operation
		recursiveStateNode(root.getFalseChild(), 2 * index, stateNodeOut);
		recursiveStateNode(root.getTrueChild(), 2 * index + 1, stateNodeOut);
	}

	private static void recursiveConsEdgeGraph(StateNode root, long index,
			PrintWriter consEdgeGraphOut) {
		// if(index<0)System.exit(-1);
		if (root == null) {
			return;
		}
		Set<String> Vars = root.getPegIntra_blockVars();
		// push
		for (String s : Vars) {
			// if(root.getLocalsMap().containsKey(s)){
			// s = root.getLocalsMap().get(s).toSmt2String();
			// putVar2indexMap(index+"."+s);
			// }else{
			// putVar2indexMap(index+"."+s);
			// }
			putVar2indexMap(index + "." + s);
			if (!constraintEdgeMap.containsKey(s)) {
				constraintEdgeMap.put(s, new Stack<Long>());
			} else if (constraintEdgeMap.get(s).size() != 0) {
				long start = constraintEdgeMap.get(s).peek();
				consEdgeGraphOut.println(var2indexMap.get(start + "." + s)
						+ ", " + var2indexMap.get(index + "." + s) + ", ["
						+ start + ", " + index + "]");
			}
			constraintEdgeMap.get(s).push(index);
			// System.out.print(index);
		}
		// params rets
		// consEdgeGraphOut.println(root.getPeg_intra_block().getCallSites().size());
		// consEdgeGraphOut.println("----in----");
		consEdgeGraphOut.print(root.getPeg_intra_block().toString(var2indexMap,
				index));
		// consEdgeGraphOut.println("----out----");
		List<CallSite> callSites = root.getCallsites();
		if (callSites != null) {
			for (CallSite cs : callSites) {
				// consEdgeGraphOut.println("#" + cs.getSignature());
			}
		}
		// recursive operation
		recursiveConsEdgeGraph(root.getFalseChild(), 2 * index,
				consEdgeGraphOut);
		recursiveConsEdgeGraph(root.getTrueChild(), 2 * index + 1,
				consEdgeGraphOut);
		// pop
		for (String s : Vars) {
			constraintEdgeMap.get(s).pop();
			// System.out.print(index);
		}
	}

	private static void printVar2indexMap(PrintWriter var2indexMapOut) {
		for (String s : var2indexMap.keySet()) {
			var2indexMapOut.println(var2indexMap.get(s) + " : " + s);
		}
	}

	private static String getCallSite(CallSite callSite) {
		List<String> stringList = new ArrayList<>();
		// print signature
		if (callSite.getSignature() != null) {
			stringList.add(JSON.toJson("signature", callSite.getSignature()));
		} else {
			stringList.add(JSON.toJson("signature", null));
		}
		// print callee
		stringList.add(JSON.toJson("callee", callSite.getCalleeString()));
		// print argumentsMap
		List<String> argStringList = new ArrayList<>();
		for (Immediate im : callSite.getArgumentsMap().keySet()) {
			Expression expr = callSite.getArgumentsMap().get(im);
			if (im != null && im != null) {
				argStringList.add(JSON.toJson(im.toString() + " = "
						+ expr.toString()));
			}
		}
		if (argStringList.size() != 0) {
			stringList.add(JSON.toJsonArray("argumentsMap", argStringList));
		} else {
			stringList.add(JSON.toJson("argumentsMap", null));
		}
		// print retVar
		if (callSite.getRetVar() != null) {
			stringList.add(JSON.toJson("retVar", callSite.getRetVar()
					.toString()));
		} else {
			stringList.add(JSON.toJson("retVar", null));
		}
		return JSON.toJsonSet(stringList);
	}

	/**
	 * print out state information
	 * 
	 * @param root
	 * @param id
	 */
	private static void printOutInfo(StateNode root, long id) {
		// TODO Auto-generated method stub
		if (root == null) {
			return;
		}
		System.out.println(id + ": " + root.toString());
		System.out.println("local2local size:"
				+ root.getPeg_intra_block().getLocal2Local().size());
		printOutInfo(root.getFalseChild(), 2 * id);
		printOutInfo(root.getTrueChild(), 2 * id + 1);
	}

}

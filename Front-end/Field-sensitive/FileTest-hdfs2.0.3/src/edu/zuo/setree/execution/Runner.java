package edu.zuo.setree.execution;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;

import acteve.instrumentor.LoopTransformer;
import acteve.instrumentor.SwitchTransformer;
import acteve.symbolic.integer.BinaryOperator;
import acteve.symbolic.integer.BooleanBinaryOperator;
import acteve.symbolic.integer.DoubleBinaryOperator;
import acteve.symbolic.integer.Expression;
import acteve.symbolic.integer.FloatBinaryOperator;
import acteve.symbolic.integer.IntegerBinaryOperator;
import acteve.symbolic.integer.LongBinaryOperator;
import edu.zuo.pegraph.PEGGenerator_block;
import edu.zuo.pegraph.datastructure.PegIntra_block;
import edu.zuo.setree.datastructure.Conditional;
import edu.zuo.setree.datastructure.StateNode;
import edu.zuo.setree.export.Exporter;
import soot.Body;
import soot.BooleanType;
import soot.ByteType;
import soot.CharType;
import soot.DoubleType;
import soot.FloatType;
import soot.Immediate;
import soot.IntType;
import soot.Local;
import soot.LongType;
import soot.PrimType;
import soot.ShortType;
import soot.SootClass;
import soot.SootMethod;
import soot.Type;
import soot.Unit;
import soot.Value;
import soot.jimple.AbstractStmtSwitch;
import soot.jimple.BinopExpr;
import soot.jimple.CaughtExceptionRef;
import soot.jimple.ConditionExpr;
import soot.jimple.Constant;
import soot.jimple.IdentityStmt;
import soot.jimple.IfStmt;
import soot.jimple.Stmt;
import soot.jimple.toolkits.annotation.logic.Loop;
import soot.toolkits.graph.Block;
import soot.toolkits.graph.BriefBlockGraph;
import soot.toolkits.graph.LoopNestTree;
import soot.util.Chain;

public class Runner {

	private final StateNode root;

	private final StateNode funcEntry;

	public Runner() {
		this.root = new StateNode();
		this.funcEntry = new StateNode();
	}

	public void run(Chain<SootClass> classes) {
		for (SootClass klass : classes) {
			List<SootMethod> origMethods = klass.getMethods();
			for (SootMethod m : origMethods) {
				if (m.isConcrete()) {
					run(m.retrieveActiveBody());
				}
			}
		}
	}
	
	protected void writeOut(StringBuilder info, String fileName) {
		PrintWriter out = null;
		try {
			out = new PrintWriter(new BufferedWriter(new FileWriter(new File(fileName), true)));
			out.println(info.toString());
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (out != null) {
				out.close();
			}
		}
	}

	public void run(Body mb) {
		StringBuilder infoBuilder = new StringBuilder();
		System.out.println("\n\n");
		System.out.println("Method: "
				+ mb.getMethod().getSubSignature().toString());
		infoBuilder.append(mb.getMethod().getDeclaringClass().toString());
		writeOut(infoBuilder, "class.txt");
		System.out
				.println("---------------------------------------------------");

		// transform the body
		System.out.println("\nBefore transform()");
		transform(mb.getMethod());

		// confirm that there's no loop at all before executing it symbolically
		System.out.println("\nBefore confirm_no_loop()");
		confirm_no_loop(mb);

		// execute the body symbolically
		System.out.println("\nBefore execute()");
		execute(mb);
		// printOutInfo(root,1);

		// separate func entry
		System.out.println("\nBefore separate()");
		separate();

		// export the symbolic execution tree
		System.out.println("\nBefore export()");
		export(mb);
	}

	/* Add by wefcser */
	private void separate() {
		PegIntra_block first_peg_block = root.getPeg_intra_block();
		PegIntra_block peg_block = new PegIntra_block();
		peg_block.setFormalCallee(first_peg_block.getFormal_callee());
		List<Local> formal_paras = first_peg_block.getFormal_paras();
		for (Local loc : formal_paras) {
			peg_block.addFormalParameter(loc);
		}
		// root.getPeg_intra_block().clearFormal_callee();
		// root.getPeg_intra_block().clearFormal_paras();
		Map<Local, Expression> localExpressionMap = root.getLocalsMap();
		for (Local l : localExpressionMap.keySet()) {
			funcEntry.putToLocalsMap(l, localExpressionMap.get(l));
		}
		funcEntry.setTrueChild(root);
		funcEntry.setPeg_intra_block(peg_block);
	}

	private void export(Body mb) {
		Exporter.run(funcEntry, mb);
	}

	private void confirm_no_loop(Body mb) {
		// TODO Auto-generated method stub
		LoopNestTree loopNestTree = new LoopNestTree(mb);
		List<Loop> need2Remove = new ArrayList<>();
		for (Loop l : loopNestTree) {
			if (l.getHead() instanceof IdentityStmt
					&& ((IdentityStmt) l.getHead()).getRightOp() instanceof CaughtExceptionRef) {
				need2Remove.add(l);
			}
		}
		loopNestTree.removeAll(need2Remove);
		if (!loopNestTree.isEmpty()) {
			throw new RuntimeException("Unexpected loops existing!!!");
		}
	}

	private void execute(Body mb) {
		BriefBlockGraph cfg = new BriefBlockGraph(mb);
		System.out.println("\n\nCFG before executing ==>>");
		System.out.println(cfg.toString());

		// List<Block> entries = cfg.getHeads();
		List<Block> entries = new ArrayList<Block>(cfg.getHeads());
		filterEntries(entries);

		assert (entries.size() == 1);
		Block entry = entries.get(0);
		// recursive construct stateNode tree
		traverseCFG(entry, root);
	}

	private void transform(SootMethod method) {
		BriefBlockGraph cfg = new BriefBlockGraph(method.getActiveBody());
		System.out.println("\nCFG before transforming ==>>");
		System.out.println(cfg.toString());

		// switch transform: transform lookupswitch and tableswitch into if
		SwitchTransformer.transform(method);

		// loop transform: unroll the loop twice
		LoopTransformer.transform(method);
	}

	/**
	 * filter out the entry blocks which are catching exceptions
	 * 
	 * @param entries
	 */
	private void filterEntries(List<Block> entries) {
		// TODO Auto-generated method stub
		for (Iterator<Block> it = entries.listIterator(); it.hasNext();) {
			Block b = it.next();
			if (b.getHead() instanceof IdentityStmt
					&& ((IdentityStmt) b.getHead()).getRightOp() instanceof CaughtExceptionRef) {
				it.remove();
			}
		}
		// System.out.println(entries.size());
		assert (entries.size() == 1);
	}

	private void traverseCFG(Block block, StateNode node) {
		// propagate the execution symbolically
		operate(block, node);

		// branching
		List<Block> succs = block.getSuccs();
		if (succs.size() == 2) {
			// branch
			assert (block.getTail() instanceof IfStmt);

			// //set conditional
			// IfStmt ifstmt = (IfStmt) block.getTail();
			// setConditional(ifstmt, node);

			StateNode nTrue = new StateNode(node);
			node.setTrueChild(nTrue);
			traverseCFG(succs.get(1), nTrue);

			StateNode nFalse = new StateNode(node);
			node.setFalseChild(nFalse);
			traverseCFG(succs.get(0), nFalse);
		} else if (succs.size() == 1) {
			// fall-through
			// StateNode nTrue = new StateNode(node);
			// node.setTrueChild(nTrue);
			// traverseCFG(succs.get(0), nTrue);
			traverseCFG(succs.get(0), node);
		} else if (succs.size() == 0) {
			// end
		} else if (succs.size() > 2) {
			// error
			System.err.println("unexpected case!!!");
		}
	}

	// public static BinaryOperator getConditionOperator(ConditionExpr
	// conditionExpr) {
	// // TODO Auto-generated method stub
	// String binExprSymbol = conditionExpr.getSymbol().trim();
	//
	// Type binType = conditionExpr.getType();
	// // assert(binType == binExpr.getOp2().getType());
	//
	// if(binType instanceof IntType || binType instanceof ShortType || binType
	// instanceof CharType || binType instanceof ByteType){
	// return new IntegerBinaryOperator(binExprSymbol);
	// }
	// else if(binType instanceof LongType){
	// return new LongBinaryOperator(binExprSymbol);
	// }
	// else if(binType instanceof FloatType){
	// return new FloatBinaryOperator(binExprSymbol);
	// }
	// else if(binType instanceof DoubleType){
	// return new DoubleBinaryOperator(binExprSymbol);
	// }
	// else if(binType instanceof BooleanType){
	// return new BooleanBinaryOperator(binExprSymbol);
	// }
	// else{
	// System.err.println("wrong type: " + binType.toString());
	// }
	//
	// return null;
	// }

	private void operate(Block block, StateNode node) {
		// ---------------------------------------------------------
		// generate symbolic execution graph (SEG)
		Propagator p = new Propagator(node);
		for (Iterator<Unit> it = block.iterator(); it.hasNext();) {
			Stmt stmt = (Stmt) it.next();
			// //for debugging
			// System.out.println(stmt);
			stmt.apply(p);
		}

		// ---------------------------------------------------------
		// generate peg_block (PEG) for alias analysis
		PegIntra_block peg_block = node.getPeg_intra_block() == null ? new PegIntra_block()
				: node.getPeg_intra_block();
		PEGGenerator_block generator_block = new PEGGenerator_block(block,
				peg_block);
		generator_block.process();
		node.setPeg_intra_block(peg_block);

	}

	// /** get the unique entry block starting with Parameter or This rather
	// than CaughtException
	// * @param cfg
	// * @return
	// */
	// private Block getEntryBlock(BriefBlockGraph cfg) {
	// // TODO Auto-generated method stub
	// List<Block> entries = cfg.getHeads();
	// if(entries.size() == 1){
	// return entries.get(0);
	// }
	//
	// for(Block b: entries){
	// if(b.getHead() instanceof IdentityStmt &&
	// ((IdentityStmt)b.getHead()).getRightOp() instanceof CaughtExceptionRef){
	// continue;
	// }
	// return b;
	// }
	// return null;
	// }
	/**
	 * print out state information
	 * 
	 * @param root
	 * @param id
	 */
	private static void printOutInfo(StateNode root, int id) {
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

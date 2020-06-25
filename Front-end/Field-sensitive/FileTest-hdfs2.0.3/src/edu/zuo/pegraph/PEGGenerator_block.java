package edu.zuo.pegraph;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.zuo.pegraph.datastructure.PegIntra_block;
import edu.zuo.pegraph.datastructure.PegIntra_block.CallSite;
import soot.ArrayType;
import soot.Body;
import soot.BodyTransformer;
import soot.Immediate;
import soot.Local;
import soot.RefType;
import soot.SootMethod;
import soot.Type;
import soot.Unit;
import soot.Value;
import soot.jimple.ArrayRef;
import soot.jimple.AssignStmt;
import soot.jimple.BinopExpr;
import soot.jimple.CastExpr;
import soot.jimple.CaughtExceptionRef;
import soot.jimple.ClassConstant;
import soot.jimple.DefinitionStmt;
import soot.jimple.FieldRef;
import soot.jimple.GotoStmt;
import soot.jimple.IdentityRef;
import soot.jimple.IdentityStmt;
import soot.jimple.IfStmt;
import soot.jimple.InstanceFieldRef;
import soot.jimple.InstanceInvokeExpr;
import soot.jimple.InstanceOfExpr;
import soot.jimple.InvokeExpr;
import soot.jimple.LookupSwitchStmt;
import soot.jimple.MonitorStmt;
import soot.jimple.NewArrayExpr;
import soot.jimple.NewExpr;
import soot.jimple.NewMultiArrayExpr;
import soot.jimple.NopStmt;
import soot.jimple.ParameterRef;
import soot.jimple.RetStmt;
import soot.jimple.ReturnStmt;
import soot.jimple.ReturnVoidStmt;
import soot.jimple.Stmt;
import soot.jimple.StringConstant;
import soot.jimple.TableSwitchStmt;
import soot.jimple.ThisRef;
import soot.jimple.ThrowStmt;
import soot.jimple.UnopExpr;
import soot.toolkits.graph.Block;

public class PEGGenerator_block {

	// private SootMethod sm;

	private Block block;

	private PegIntra_block intra_graph_block;

	// @Override
	// protected void internalTransform(Body arg0, String arg1, Map arg2) {
	// // TODO Auto-generated method stub
	// sm = arg0.getMethod();
	// intra_graph = new PegIntra(sm);
	//
	// if (!sm.hasActiveBody()) {
	// sm.retrieveActiveBody();
	// }
	//
	// // first of all, flow edges are added by inspecting the statements in the
	// // method one by one
	// for (Iterator stmts = sm.getActiveBody().getUnits().iterator(); stmts
	// .hasNext();) {
	// Stmt st = (Stmt) stmts.next();
	// processStmt(st);
	// }
	//
	// }
	public PEGGenerator_block(Block block, PegIntra_block peg_intra_block) {
		// this.intra_graph_block = new PegIntra_block();
		this.block = block;
		this.intra_graph_block = peg_intra_block;
	}

	public void process() {
		for (Iterator<Unit> it = block.iterator(); it.hasNext();) {
			Stmt stmt = (Stmt) it.next();
			// //for debugging
			// System.out.println(stmt);
			processStmt(stmt);
		}
	}

	/**
	 * Ignores certain types of statements, and calls addFlowEdges()
	 * 
	 * @param s
	 */
	private void processStmt(Stmt s) {
		if (s instanceof ReturnVoidStmt)
			return;
		if (s instanceof GotoStmt)
			return;
		if (s instanceof IfStmt)
			return;
		if (s instanceof TableSwitchStmt)
			return;
		if (s instanceof LookupSwitchStmt)
			return;
		if (s instanceof MonitorStmt)
			return;
		if (s instanceof RetStmt)
			return;
		if (s instanceof NopStmt)
			return;
		addFlowEdges(s);
	}

	private boolean isJavaObjectNew(InvokeExpr invoke) {
		SootMethod static_target = invoke.getMethod();
		String sig = static_target.getSubSignature();
		String cls = static_target.getDeclaringClass().getName();

		return (sig.equals("java.lang.Object newInstance()") && cls
				.equals("java.lang.Class"))
				|| (sig.equals("java.lang.Object newInstance(java.lang.Object[])") && cls
						.equals("java.lang.reflect.Constructor"))
				|| (static_target.getSignature()
						.equals("<java.lang.reflect.Array: java.lang.Object newInstance(java.lang.Class,int)>"))
				|| (sig.equals("java.lang.Object invoke(java.lang.Object,java.lang.Object[])") && cls
						.equals("java.lang.reflect.Method"))
				|| (sig.equals("java.lang.Object newProxyInstance(java.lang.ClassLoader,java.lang.Class[],java.lang.reflect.InvocationHandler)") && cls
						.equals("java.lang.reflect.Proxy"));

	}

	private void addFlowEdges(Stmt s) {
		// case 0: call site
		if (s.containsInvokeExpr()) {
			InvokeExpr ie = s.getInvokeExpr();

			// local = invokeExpr()
			if (s instanceof AssignStmt) {
				Local lhs = (Local) ((AssignStmt) s).getLeftOp();

				// deals with certain special cases and since they are special,
				// the parameters of them are not handled
				if (isJavaObjectNew(ie)) {
					intra_graph_block.addJavaClassObj2Local(ie, lhs);
					return;
				}
			}

			// deals with actual arguments
			CallSite callsite = intra_graph_block.createCallSite(ie);

			// add receiver
			if (s.getInvokeExpr() instanceof InstanceInvokeExpr) {
				Immediate base = (Immediate) ((InstanceInvokeExpr) s
						.getInvokeExpr()).getBase();
				callsite.setReceiver(base);
			}

			// add actual arguments
			for (Value arg : s.getInvokeExpr().getArgs()) {
				if ((arg instanceof Local && isTypeofInterest(arg))
						|| (arg instanceof StringConstant)
						|| (arg instanceof ClassConstant)) {
					callsite.addArg((Immediate) arg);
				}
			}

			// deals with return values (which matters only for AssignStmt)
			if (s instanceof AssignStmt) {
				Value lhs = ((AssignStmt) s).getLeftOp();
				if (isTypeofInterest(lhs)) {
					callsite.setActualReturn((Local) lhs);
				}
			}

			return;
		}

		// case 1: ReturnStmt
		if (s instanceof ReturnStmt) {
			Immediate v = (Immediate) ((ReturnStmt) s).getOp();
			if ((v instanceof Local && isTypeofInterest(v))
					|| (v instanceof StringConstant)
					|| (v instanceof ClassConstant)) {
				intra_graph_block.setFormalReturn(v);
			}
			return;
		}

		// case 2: ThrowStmt
		if (s instanceof ThrowStmt) {
			return;
		}

		Value lhs = ((DefinitionStmt) s).getLeftOp();
		Value rhs = ((DefinitionStmt) s).getRightOp();

		// case 3: IdentityStmt
		if (s instanceof IdentityStmt) {
			// if (rhs instanceof CaughtExceptionRef) {
			//
			// }

			if (rhs instanceof ThisRef) {
				intra_graph_block.setFormalCallee((Local) lhs);
			}

			if ((rhs instanceof ParameterRef && isTypeofInterest(rhs))
					|| (rhs instanceof StringConstant)
					|| (rhs instanceof ClassConstant)) {
				intra_graph_block.addFormalParameter((Local) lhs);
			}
			return;
		}

		// case 4: AssignStmt
		if (s instanceof AssignStmt) {
			// case 4.1: lhs is array access
			if (lhs instanceof ArrayRef) {
				// if rhs is local
				if (rhs instanceof Local && isTypeofInterest(rhs)) {
					intra_graph_block.addLocal2ArrayRef((Local) rhs,
							(ArrayRef) lhs);
				}
				// rhs is a string constant
				if (rhs instanceof StringConstant) {
					intra_graph_block.addStringConst2ArrayRef(
							(StringConstant) rhs, (ArrayRef) lhs);
				}
				if (rhs instanceof ClassConstant) {
					intra_graph_block.addClassConst2ArrayRef(
							(ClassConstant) rhs, (ArrayRef) lhs);
				}
				return;
			}

			// case 4.2: lhs is a field access
			if (lhs instanceof FieldRef) {

				if (rhs instanceof Local && isTypeofInterest(rhs)) {
					intra_graph_block.addLocal2FieldRef((Local) rhs,
							(FieldRef) lhs);
				}
				// if rhs is a string constant
				if (rhs instanceof StringConstant) {
					intra_graph_block.addStringConst2FieldRef(
							(StringConstant) rhs, (FieldRef) lhs);
				}
				// if rhs is a class constant
				if (rhs instanceof ClassConstant) {
					intra_graph_block.addClassConst2FieldRef(
							(ClassConstant) rhs, (FieldRef) lhs);
				}
				return;
			}

			if (!isTypeofInterest(lhs))
				return;

			// case 4.3: local := local
			if (rhs instanceof Local && isTypeofInterest(rhs)) {
				intra_graph_block.addLocal2Local((Local) rhs, (Local) lhs);
				return;
			}

			// case 4.4.1: local := string const
			if (rhs instanceof StringConstant) {
				intra_graph_block.addStringConst2Local((StringConstant) rhs,
						(Local) lhs);
				return;
			}
			// case 4.4.2: local := class const
			if (rhs instanceof ClassConstant) {
				intra_graph_block.addClassConst2Local((ClassConstant) rhs,
						(Local) lhs);
				return;
			}

			// case 4.5: local := new X
			if (rhs instanceof NewExpr) {
				intra_graph_block.addNewExpr2Local((NewExpr) rhs, (Local) lhs);
				return;
			}

			// case 4.6: new array: e.g. x := new Y[5];
			if (rhs instanceof NewArrayExpr) {
				intra_graph_block.addNewArrayExpr2Local((NewArrayExpr) rhs,
						(Local) lhs);
				return;
			}

			// case 4.7: new multi-dimensional array
			if (rhs instanceof NewMultiArrayExpr) {
				intra_graph_block.addNewMultiArrayExpr2Local(
						(NewMultiArrayExpr) rhs, (Local) lhs);
				return;
			}

			// case 4.8: rhs is field access x.f or X.f
			if (rhs instanceof FieldRef && isTypeofInterest(rhs)) {
				intra_graph_block.addField2Local((FieldRef) rhs, (Local) lhs);
				return;
			}

			// case 4.9: cast
			if (rhs instanceof CastExpr && isTypeofInterest(rhs)) {
				Value y = ((CastExpr) rhs).getOp();
				// possibleTypes.add(lhs.getType());
				if (y instanceof Local && isTypeofInterest(y)) {
					intra_graph_block.addLocal2Local((Local) y, (Local) lhs);
				}
				if (y instanceof StringConstant) {
					intra_graph_block.addStringConst2Local((StringConstant) y,
							(Local) lhs);
				}
				if (y instanceof ClassConstant) {
					intra_graph_block.addClassConst2Local((ClassConstant) y,
							(Local) lhs);
				}
				return;
			}

			// case 4.10: rhs is array reference
			if (rhs instanceof ArrayRef && isTypeofInterest(rhs)) {
				intra_graph_block
						.addArrayRef2Local((ArrayRef) rhs, (Local) lhs);
				return;
			}

			return;

		} // AssignStmt

	}

	public static boolean isTypeofInterest(Value v) {
		return (v.getType() instanceof RefType || v.getType() instanceof ArrayType);
	}
}

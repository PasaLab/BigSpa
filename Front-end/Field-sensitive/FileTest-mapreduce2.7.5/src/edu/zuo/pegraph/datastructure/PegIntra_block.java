package edu.zuo.pegraph.datastructure;

import java.util.*;

import edu.zuo.setree.datastructure.CallSite;
import soot.Immediate;
import soot.Local;
import soot.SootMethod;
import soot.Value;
import soot.jimple.AnyNewExpr;
import soot.jimple.ArrayRef;
import soot.jimple.ClassConstant;
import soot.jimple.ConcreteRef;
import soot.jimple.Constant;
import soot.jimple.FieldRef;
import soot.jimple.InstanceFieldRef;
import soot.jimple.InvokeExpr;
import soot.jimple.NewArrayExpr;
import soot.jimple.NewExpr;
import soot.jimple.NewMultiArrayExpr;
import soot.jimple.StaticFieldRef;
import soot.jimple.StringConstant;

public class PegIntra_block {

	// private SootMethod soot_method;

	// --formal parameters
	private Local formal_callee = null;

	private List<Local> formal_paras = new ArrayList<Local>();

	// --formal return: could be {Local, StringConstant, ClassConstant}
	private Immediate formal_return = null;

	// --call sites
	private Map callSites = new HashMap<InvokeExpr, CallSite>();

	// --edges
	private Map<Local, HashSet<Local>> local2Local = new HashMap<Local, HashSet<Local>>();

	// obj could be {StringConstant, ClassConstant, NewExpr, NewArrayExpr,
	// NewMultiArrayExpr, NewInstanceInvoke(which is special invokeExpr)}
	private Map<Value, HashSet<Local>> obj2Local = new HashMap<Value, HashSet<Local>>();

	// ref could be {InstanceFieldRef or StaticFieldRef or ArrayRef}
	private Map<ConcreteRef, HashSet<Local>> ref2Local = new HashMap<ConcreteRef, HashSet<Local>>();

	// ref could be {InstanceFieldRef or StaticFieldRef or ArrayRef}
	private Map<Local, HashSet<ConcreteRef>> local2Ref = new HashMap<Local, HashSet<ConcreteRef>>();

	// const could be {StringConstant or ClassConstant}, ref could be
	// {InstanceFieldRef or StaticFieldRef or ArrayRef}
	private Map<Constant, HashSet<ConcreteRef>> const2Ref = new HashMap<Constant, HashSet<ConcreteRef>>();

	public PegIntra_block() {

	}

	public void exportIntraGraph(String file_name) {
		// TODO

	}

	public Set<String> getVars() {
		Set<String> Vars = new HashSet<>();
		// --formal parameters
		if (formal_callee != null) {
			// System.out.println("formal_callee:"+formal_callee.toString());
			Vars.add(formal_callee.toString());
		}
		// System.out.println("formal_paras_size:"+formal_paras.size());
		for (Local loc : formal_paras) {
			// System.out.println(loc.toString());
			Vars.add(loc.toString());
		}
		// --formal return: could be {Local, StringConstant, ClassConstant}
		if (formal_return != null) {
			// System.out.println("formal_return:"+formal_return.toString());
			Vars.add(formal_return.toString());
		}
		// //--call sites
		// System.out.println("callsite"+callSites.size());

		Iterator<InvokeExpr> iter = callSites.keySet().iterator();
		while (iter.hasNext()) {
			// System.out.println("----------");
			InvokeExpr key = iter.next();
			// System.out.println(key.toString());
			CallSite callsite = (CallSite) callSites.get(key);
			Immediate actual_callee = callsite.getActual_callee();

			// arg could be {Local or StringConstant or ClassConstant}
			List<Immediate> actual_args = callsite.getActual_args();

			Local actural_return = callsite.getActural_return();

			// Vars.add(key.getMethod().getSignature());
			if (actual_callee != null) {
				// System.out.println("callee"+actual_callee);
				Vars.add(actual_callee.toString());
			}
			// System.out.println("actual_args");
			for (Immediate im : actual_args) {
				// System.out.println(im.toString());
				Vars.add(im.toString());
			}
			// for(Value val:key.getArgs()){
			// System.out.println(val.toString());
			// Vars.add(val.toString());
			// }
			if (actural_return != null) {
				// System.out.println("return"+actural_return.toString());
				Vars.add(actural_return.toString());
			}
		}

		// local2local: Assign
		for (Value loc1 : this.local2Local.keySet()) {
			// System.out.print("local2local: "+loc1.toString()+" 2");
			HashSet<Local> locs = this.local2Local.get(loc1);
			Vars.add(loc1.toString());
			for (Value loc2 : locs) {
				// System.out.println(" "+loc2.toString());
				Vars.add(loc2.toString());
			}
			// System.out.println();
		}

		// obj2local: New
		for (Value v : this.obj2Local.keySet()) {
			// System.out.print("obj2local: "+v.toString()+" 2");
			HashSet<Local> locs = this.obj2Local.get(v);
			Vars.add(v.toString());
			for (Value loc : locs) {
				// System.out.print(" "+loc.toString());
				Vars.add(loc.toString());
			}
			// System.out.println();
		}

		// ref2local
		for (ConcreteRef ref : this.ref2Local.keySet()) {
			HashSet<Local> locs = this.ref2Local.get(ref);
			Vars.add(ref.toString());
			for (Value loc : locs) {
				Vars.add(loc.toString());
			}
		}

		// local2ref
		for (Local local : this.local2Ref.keySet()) {
			HashSet<ConcreteRef> refs = this.local2Ref.get(local);
			Vars.add(local.toString());
			for (ConcreteRef ref : refs) {
				// System.out.println("-------------"+local.toString()+"2"+ref.toString());
				if (ref instanceof InstanceFieldRef) {
					Vars.add(((InstanceFieldRef) ref).getBase().toString());
				} else if (ref instanceof ArrayRef) {
					Vars.add(((ArrayRef) ref).getBase().toString());
				}
				Vars.add(ref.toString());
			}
		}

		// const2ref: New & Store
		for (Constant cons : this.const2Ref.keySet()) {
			HashSet<ConcreteRef> refs = this.const2Ref.get(cons);
			Vars.add(cons.toString());
			for (ConcreteRef ref : refs) {
				if (ref instanceof InstanceFieldRef) {
					Vars.add(((InstanceFieldRef) ref).getBase().toString());
				} else if (ref instanceof ArrayRef) {
					Vars.add(((ArrayRef) ref).getBase().toString());
				}
				Vars.add(ref.toString());
			}
		}
		// System.out.println("Vars: "+Vars.size());
		return Vars;
	}

	public String toString(Map<String, Integer> var2indexMap, long index) {
		StringBuilder builder = new StringBuilder();
		// System.out.println("local2Local: "+local2Local.size()
		// +"\nobj2local: "+ obj2Local.size() +"\nref2Local: "+ ref2Local.size()
		// +"\nlocal2Ref: "+ local2Ref.size() +"\nconst2Ref: "+
		// const2Ref.size());
		// --method signature

		// --formal parameters
		if (formal_callee != null) {
			builder.append(var2indexMap.get(index + "."
					+ formal_callee.toString())
					+ ", [Callee]\n");
		}
		for (Local loc : formal_paras) {
			builder.append(var2indexMap.get(index + "." + loc.toString())
					+ ", [Para" + formal_paras.indexOf(loc) + "]\n");
		}
		// --formal return
		if (formal_return != null) {
			builder.append(var2indexMap.get(index + "."
					+ formal_return.toString())
					+ ", [Return" + index + "]\n");
		}
		// --call sites
		Iterator<InvokeExpr> iter = callSites.keySet().iterator();
		while (iter.hasNext()) {
			InvokeExpr key = iter.next();
			String method = key.getMethod().getSignature();
			CallSite callsite = (CallSite) callSites.get(key);
			Immediate actual_callee = callsite.getActual_callee();

			// arg could be {Local or StringConstant or ClassConstant}
			List<Immediate> actual_args = callsite.getActual_args();

			Local actural_return = callsite.getActural_return();
			if (actual_callee != null) {
			}
			String args = "";
			for (Immediate val : actual_args) {
				args = args
						+ ", "
						+ var2indexMap.get(index + "." + val.toString())
								.toString();
			}
			if (args.length() > 1)
				args = "[" + args.substring(2) + "]";
			else
				args = "[]";
			String callee = "[";
			if (actual_callee != null) {
				callee = callee
						+ var2indexMap.get(
								index + "." + actual_callee.toString())
								.toString();
			}
			callee = callee + "]";
			String ret = "[";
			if (actural_return != null) {
				ret = ret
						+ var2indexMap.get(
								index + "." + actural_return.toString())
								.toString();
			}
			ret = ret + "]";
			builder.append(ret + ", " + index + ", " + method + ", [Call], "
					+ callee + ", " + args + "\n");
		}

		// --edges
		// local2local: Assign
		for (Value loc1 : this.local2Local.keySet()) {
			HashSet<Local> locs = this.local2Local.get(loc1);
			for (Value loc2 : locs) {
				builder.append(var2indexMap.get(index + "." + loc2.toString())
						+ ", "
						+ var2indexMap.get(index + "." + loc1.toString())
						+ ", [Assign]\n");
			}
		}

		// obj2local: New
		for (Value v : this.obj2Local.keySet()) {
			HashSet<Local> locs = this.obj2Local.get(v);
			for (Value loc : locs) {
				if (v instanceof StringConstant) {
					builder.append(var2indexMap.get(index + "."
							+ loc.toString())
							+ ", "
							+ var2indexMap.get(index + "." + v.toString())
							+ ", [New]\n");
				} else if (v instanceof ClassConstant) {
					builder.append(var2indexMap.get(index + "."
							+ loc.toString())
							+ ", "
							+ var2indexMap.get(index + "." + v.toString())
							+ ", [New]\n");
				} else if (v instanceof AnyNewExpr) {
					assert (v instanceof NewExpr || v instanceof NewArrayExpr || v instanceof NewMultiArrayExpr);
					builder.append(var2indexMap.get(index + "."
							+ loc.toString())
							+ ", "
							+ var2indexMap.get(index + "." + v.toString())
							+ ", [New]\n");
				} else if (v instanceof InvokeExpr) {
					builder.append(var2indexMap.get(index + "."
							+ loc.toString())
							+ ", "
							+ var2indexMap.get(index + "." + v.toString())
							+ ", [New]\n");
				} else {
					System.err.println("obj type error!!!");
				}
			}
		}

		// ref2local
		for (ConcreteRef ref : this.ref2Local.keySet()) {
			HashSet<Local> locs = this.ref2Local.get(ref);
			for (Value loc : locs) {
				if (ref instanceof InstanceFieldRef) {// Load
					builder.append(var2indexMap.get(index + "."
							+ loc.toString())
							+ ", "
							+ var2indexMap.get(index + "." + ref.toString())
							+ ", [g(" + ((InstanceFieldRef)ref).getField().getName() + ")]\n");
				} else if (ref instanceof StaticFieldRef) {// Assign
					builder.append(var2indexMap.get(index + "."
							+ loc.toString())
							+ ", "
							+ var2indexMap.get(index + "." + ref.toString())
							+ ", [Assign]\n");
				} else if (ref instanceof ArrayRef) {// Load
					builder.append(var2indexMap.get(index + "."
							+ loc.toString())
							+ ", "
							+ var2indexMap.get(index + "." + ref.toString())
							+ ", [g(array)]\n");
				} else {
					System.err.println("ref type error!!!");
				}
			}
		}

		// local2ref
		for (Local local : this.local2Ref.keySet()) {
			HashSet<ConcreteRef> refs = this.local2Ref.get(local);
			for (ConcreteRef ref : refs) {
				if (ref instanceof InstanceFieldRef) {// putfield
					// builder.append(var2indexMap.get(index+"."+ref.toString())
					// + ", " + var2indexMap.get(index+"."+local.toString()) +
					// ", [Store]\n");
					builder.append(var2indexMap.get(index + "."
							+ ((InstanceFieldRef) ref).getBase().toString())
							+ ", "
							+ var2indexMap.get(index + "." + local.toString())
							+ ", [p(" + ((InstanceFieldRef)ref).getField().getName() + ")]\n");
				} else if (ref instanceof StaticFieldRef) {// Assign
					builder.append(var2indexMap.get(index + "."
							+ ref.toString())
							+ ", "
							+ var2indexMap.get(index + "." + local.toString())
							+ ", [Assign]\n");
				} else if (ref instanceof ArrayRef) {// putfield
					// builder.append(var2indexMap.get(index+"."+ref.toString())
					// + ", " + var2indexMap.get(index+"."+local.toString()) +
					// ", [Store]\n");
					builder.append(var2indexMap.get(index + "."
							+ ((ArrayRef) ref).getBase().toString())
							+ ", "
							+ var2indexMap.get(index + "." + local.toString())
							+ ", [p(array)]\n");
				} else {
					System.err.println("ref type error!!!");
				}
			}
		}

		// const2ref: New & putfield
		for (Constant cons : this.const2Ref.keySet()) {
			HashSet<ConcreteRef> refs = this.const2Ref.get(cons);
			for (ConcreteRef ref : refs) {
				if (ref instanceof InstanceFieldRef) {// New & putfield: (x.f =
														// constant) <==> (x
														// <-putfield[f]- tmp
														// <-New- constant)
					// builder.append(var2indexMap.get(index+"."+ref.toString())
					// + ", " + var2indexMap.get(index+"."+cons.toString()) +
					// ", [New & Store]\n");
					var2indexMap.put(index + ".tmp" + var2indexMap.size(),
							var2indexMap.size());
					int tmpIndex = var2indexMap.size() - 1;
					builder.append(tmpIndex + ", "
							+ var2indexMap.get(index + "." + cons.toString())
							+ ", [New]\n");
					builder.append(var2indexMap.get(index + "."
							+ ((InstanceFieldRef) ref).getBase().toString())
							+ ", " + tmpIndex + ", [p(" + ((InstanceFieldRef)ref).getField().getName() + ")]\n");
				} else if (ref instanceof StaticFieldRef) {// New: (X.f =
															// constant) <==> (f
															// <-New- constant)
					builder.append(var2indexMap.get(index + "."
							+ ref.toString())
							+ ", "
							+ var2indexMap.get(index + "." + cons.toString())
							+ ", [New]\n");
				} else if (ref instanceof ArrayRef) {// New & putfield: (array[*] =
														// constant) <==> (array
														// <-putfield[E]- tmp
														// <-New- constant)
					// builder.append(var2indexMap.get(index+"."+ref.toString())
					// + ", " + var2indexMap.get(index+"."+cons.toString()) +
					// ", [New & Store]\n");
					var2indexMap.put(index + ".tmp" + var2indexMap.size(),
							var2indexMap.size());
					int tmpIndex = var2indexMap.size() - 1;
					builder.append(tmpIndex + ", "
							+ var2indexMap.get(index + "." + cons.toString())
							+ ", [New]\n");
					builder.append(var2indexMap.get(index + "."
							+ ((ArrayRef) ref).getBase().toString())
							+ ", " + tmpIndex + ", [p(array)]\n");
				} else {
					System.err.println("ref type error!!!");
				}
			}
		}

		return builder.toString();
	}

	public void addJavaClassObj2Local(InvokeExpr newie, Local lhs) {
		// TODO Auto-generated method stub
		addObj2Local(newie, lhs);
	}

	public CallSite createCallSite(InvokeExpr ie) {
		// TODO Auto-generated method stub
		CallSite callsite = new CallSite();
		this.callSites.put(ie, callsite);
		return callsite;
	}

	public void setFormalReturn(Immediate v) {
		// TODO Auto-generated method stub
		if (v instanceof Local) {

		} else if (v instanceof StringConstant) {

		} else if (v instanceof ClassConstant) {

		} else {
			System.err.println("error!!!");
		}
		this.formal_return = v;
	}

	public void setFormalCallee(Local lhs) {
		this.formal_callee = lhs;
	}

	public void addFormalParameter(Local lhs) {
		// TODO Auto-generated method stub
		this.formal_paras.add(lhs);
	}

	public void addLocal2ArrayRef(Local rhs, ArrayRef lhs) {
		// TODO Auto-generated method stub
		addLocal2Ref(rhs, lhs);
	}

	public void addStringConst2ArrayRef(StringConstant rhs, ArrayRef lhs) {
		// TODO Auto-generated method stub
		addConst2Ref(rhs, lhs);
	}

	public void addClassConst2ArrayRef(ClassConstant rhs, ArrayRef lhs) {
		// TODO Auto-generated method stub
		addConst2Ref(rhs, lhs);
	}

	public void addLocal2FieldRef(Local rhs, FieldRef lhs) {
		// TODO Auto-generated method stub
		assert (lhs instanceof InstanceFieldRef || lhs instanceof StaticFieldRef);
		addLocal2Ref(rhs, lhs);
	}

	public void addStringConst2FieldRef(StringConstant rhs, FieldRef lhs) {
		// TODO Auto-generated method stub
		assert (lhs instanceof InstanceFieldRef || lhs instanceof StaticFieldRef);
		addConst2Ref(rhs, lhs);
	}

	public void addClassConst2FieldRef(ClassConstant rhs, FieldRef lhs) {
		assert (lhs instanceof InstanceFieldRef || lhs instanceof StaticFieldRef);
		addConst2Ref(rhs, lhs);
	}

	public void addLocal2Local(Local rhs, Local lhs) {
		// TODO Auto-generated method stub
		if (this.local2Local.containsKey(rhs)) {
			this.local2Local.get(rhs).add(lhs);
		} else {
			HashSet<Local> set = new HashSet<Local>();
			set.add(lhs);
			this.local2Local.put(rhs, set);
		}
	}

	public void addStringConst2Local(StringConstant rhs, Local lhs) {
		// TODO Auto-generated method stub
		addObj2Local(rhs, lhs);
	}

	public void addClassConst2Local(ClassConstant rhs, Local lhs) {
		// TODO Auto-generated method stub
		addObj2Local(rhs, lhs);
	}

	public void addNewExpr2Local(NewExpr rhs, Local lhs) {
		// TODO Auto-generated method stub
		addObj2Local(rhs, lhs);
	}

	public void addNewArrayExpr2Local(NewArrayExpr rhs, Local lhs) {
		// TODO Auto-generated method stub
		addObj2Local(rhs, lhs);
	}

	public void addNewMultiArrayExpr2Local(NewMultiArrayExpr rhs, Local lhs) {
		// TODO Auto-generated method stub
		addObj2Local(rhs, lhs);
	}

	public void addField2Local(FieldRef rhs, Local lhs) {
		assert (rhs instanceof InstanceFieldRef || rhs instanceof StaticFieldRef);
		addRef2Local(rhs, lhs);
	}

	public void addArrayRef2Local(ArrayRef rhs, Local lhs) {
		// TODO Auto-generated method stub
		addRef2Local(rhs, lhs);
	}

	public void addObj2Local(Value obj, Local l) {
		if (this.obj2Local.containsKey(obj)) {
			this.obj2Local.get(obj).add(l);
		} else {
			HashSet<Local> set = new HashSet<Local>();
			set.add(l);
			this.obj2Local.put(obj, set);
		}
	}

	// public void addStatic2Local(StaticFieldRef sfield, Local l){
	// if(this.static2Local.containsKey(sfield)){
	// this.static2Local.get(sfield).add(l);
	// }
	// else{
	// HashSet<Local> set = new HashSet<Local>();
	// set.add(l);
	// this.static2Local.put(sfield, set);
	// }
	// }

	public void addRef2Local(ConcreteRef ref, Local l) {
		assert (ref instanceof InstanceFieldRef || ref instanceof ArrayRef || ref instanceof StaticFieldRef);
		if (this.ref2Local.containsKey(ref)) {
			this.ref2Local.get(ref).add(l);
		} else {
			HashSet<Local> set = new HashSet<Local>();
			set.add(l);
			this.ref2Local.put(ref, set);
		}
	}

	public void addLocal2Ref(Local l, ConcreteRef ref) {
		// System.out.println("-------------"+l.toString()+"2"+ref.toString());
		if (this.local2Ref.containsKey(l)) {
			this.local2Ref.get(l).add(ref);
		} else {
			HashSet<ConcreteRef> set = new HashSet<ConcreteRef>();
			set.add(ref);
			this.local2Ref.put(l, set);
		}
	}

	// public void addLocal2Static(Local l, StaticFieldRef sfield){
	// if(this.local2Static.containsKey(l)){
	// this.local2Static.get(l).add(sfield);
	// }
	// else{
	// HashSet<StaticFieldRef> set = new HashSet<StaticFieldRef>();
	// set.add(sfield);
	// this.local2Static.put(l, set);
	// }
	// }

	public void addConst2Ref(Constant cons, ConcreteRef ref) {
		// System.out.println("-------------"+cons.toString()+"2"+ref.toString());
		if (this.const2Ref.containsKey(cons)) {
			this.const2Ref.get(cons).add(ref);
		} else {
			HashSet<ConcreteRef> set = new HashSet<ConcreteRef>();
			set.add(ref);
			this.const2Ref.put(cons, set);
		}
	}

	public Local getFormal_callee() {
		return formal_callee;
	}

	public void clearFormal_callee() {
		this.formal_callee = null;
	}

	public List<Local> getFormal_paras() {
		return formal_paras;
	}

	public void clearFormal_paras() {
		this.formal_paras.clear();
	}

	public Immediate getFormal_return() {
		return formal_return;
	}

	public Map getCallSites() {
		return callSites;
	}

	public Map<Local, HashSet<Local>> getLocal2Local() {
		return local2Local;
	}

	public Map<Value, HashSet<Local>> getObj2Local() {
		return obj2Local;
	}

	public Map<ConcreteRef, HashSet<Local>> getRef2Local() {
		return ref2Local;
	}

	public Map<Local, HashSet<ConcreteRef>> getLocal2Ref() {
		return local2Ref;
	}

	public Map<Constant, HashSet<ConcreteRef>> getConst2Ref() {
		return const2Ref;
	}

	public class CallSite {

		private Immediate actual_callee = null;

		// arg could be {Local or StringConstant or ClassConstant}
		private List<Immediate> actual_args = new ArrayList<Immediate>();

		private Local actural_return = null;

		public CallSite() {

		}

		public void setReceiver(Immediate base) {
			// TODO Auto-generated method stub
			this.actual_callee = base;
		}

		public void setActualReturn(Local lhs) {
			// TODO Auto-generated method stub
			this.actural_return = lhs;
		}

		public void addArg(Immediate arg) {
			// TODO Auto-generated method stub
			if (arg instanceof Local) {

			} else if (arg instanceof StringConstant) {

			} else if (arg instanceof ClassConstant) {

			} else {
				System.err.println("error!!!");
			}
			this.actual_args.add(arg);
		}

		public Immediate getActual_callee() {
			return actual_callee;
		}

		public List<Immediate> getActual_args() {
			return actual_args;
		}

		public Local getActural_return() {
			return actural_return;
		}

	}

}

/*
  Copyright (c) 2011,2012, 
   Saswat Anand (saswat@gatech.edu)
   Mayur Naik  (naik@cc.gatech.edu)
  All rights reserved.
  
  Redistribution and use in source and binary forms, with or without
  modification, are permitted provided that the following conditions are met: 
  
  1. Redistributions of source code must retain the above copyright notice, this
  list of conditions and the following disclaimer. 
  2. Redistributions in binary form must reproduce the above copyright notice,
  this list of conditions and the following disclaimer in the documentation
  and/or other materials provided with the distribution. 
  
  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
  ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
  WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
  DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
  ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
  (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
  LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
  ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
  (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
  SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
  
  The views and conclusions contained in the software and documentation are those
  of the authors and should not be interpreted as representing official policies, 
  either expressed or implied, of the FreeBSD Project.
 */

package acteve.instrumentor;

import soot.SootMethod;
import soot.Unit;
import soot.UnitBox;
import soot.Body;
import soot.Local;
import soot.Immediate;
import soot.jimple.TableSwitchStmt;
import soot.jimple.toolkits.annotation.logic.Loop;
import soot.toolkits.graph.BriefBlockGraph;
import soot.toolkits.graph.BriefUnitGraph;
import soot.toolkits.graph.LoopNestTree;
import soot.toolkits.graph.UnitGraph;
import soot.util.Chain;
import soot.jimple.LookupSwitchStmt;
import soot.jimple.NopStmt;
import soot.jimple.Stmt;
import soot.jimple.IfStmt;
import soot.jimple.Constant;
import soot.jimple.IntConstant;
import soot.jimple.Jimple;
import soot.jimple.GotoStmt;
import soot.jimple.EqExpr;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class LoopTransformer {

	public static final Jimple jimple = Jimple.v();

	public static final BodyEditor editor = new BodyEditor();

	private static Chain STMTS;

	public static final int mode = 0;

	private LoopTransformer() {
	}

	/**
	 * @param method
	 */
	public static void transform(SootMethod method) {
		Body body = method.retrieveActiveBody();
		editor.newBody(body, method);
		STMTS = body.getUnits();

		//
		switch (mode) {
		case 0:// break off the loop
			transform0(method);
			break;
		case 1:// nested loop: break off the loop; other loop: unroll once
			transform1(method);
			break;
		case 2: // unroll once for all loops
			transform2(method);
			break;
		}

	}

	private static void transform0(SootMethod method) {
		LoopNestTree loopNestTree = new LoopNestTree(
				method.retrieveActiveBody());
		for (Loop loop : loopNestTree) {
			// for debugging
			printLoopInfo(loop);

			// infinite loop
			if (loop.loopsForever()) {
				throw new RuntimeException("unexpected infinite loop!!!");
			}

			// break off the loop
			breakLoop(loop, method);
		}
	}

	private static void transform1(SootMethod method) {
		LoopNestTree loopNestTree = new LoopNestTree(
				method.retrieveActiveBody());
		if (hasNestedLoops(loopNestTree)) {
			System.err.println("Having nested loops.");
			for (Loop loop : loopNestTree) {
				// for debugging
				printLoopInfo(loop);

				// infinite loop
				if (loop.loopsForever()) {
					throw new RuntimeException("unexpected infinite loop!!!");
				}

				// break off the loop
				breakLoop(loop, method);
			}
		} else {
			for (Loop loop : loopNestTree) {
				// for debugging
				printLoopInfo(loop);

				// infinite loop
				if (loop.loopsForever()) {
					throw new RuntimeException("unexpected infinite loop!!!");
				}

				// unroll loop once more
				unrollLoop(loop, method);
			}
		}
	}

	private static void transform2(SootMethod method) {
		LoopNestTree loopNestTree = new LoopNestTree(
				method.retrieveActiveBody());
		if (hasNestedLoops(loopNestTree)) {
			while (!loopNestTree.isEmpty()) {
				Loop loop = loopNestTree.first();
				// for debugging
				System.out.println("\n\n# of loops: " + loopNestTree.size());
				printLoopInfo(loop);

				// infinite loop
				if (loop.loopsForever()) {
					throw new RuntimeException("unexpected infinite loop!!!");
				}

				// unroll loop once more
				unrollLoop(loop, method);

				loopNestTree = new LoopNestTree(method.retrieveActiveBody());
			}
		} else {
			for (Loop loop : loopNestTree) {
				// for debugging
				printLoopInfo(loop);

				// infinite loop
				if (loop.loopsForever()) {
					throw new RuntimeException("unexpected infinite loop!!!");
				}

				// unroll loop once more
				unrollLoop(loop, method);
			}
		}
	}

	private static void breakLoop(Loop loop, SootMethod method) {
		// Stmt target = getTargetOfLoopExits(loop);
		BriefUnitGraph cfg = new BriefUnitGraph(method.getActiveBody());

		Chain stmts = method.retrieveActiveBody().getUnits()
				.getNonPatchingChain();
		Stmt header = loop.getHead();
		Stmt exit = loop.getLoopExits().iterator().next();
		Stmt loopExitTarget = loop.targetsOfLoopExit(exit).iterator().next();

		for (Stmt stmt : loop.getLoopStatements()) {
			if (reachHeader(stmt, cfg, loop.getHead())) {
				System.err.println(stmt);

				Stmt backJump = stmt;

				if (backJump instanceof IfStmt) {// do-while
					System.out.println("BackJump is IfStmt: " + backJump);
					// assert(((IfStmt) backJump).getTarget() == header);
					if (((IfStmt) backJump).getTarget() == header) {
						((IfStmt) backJump).setTarget(loopExitTarget);
					} else {
						GotoStmt newGoto = jimple.newGotoStmt(loopExitTarget);
						stmts.insertAfter(newGoto, backJump);
					}

					// stmts.remove(backJump);
				} else if (backJump instanceof GotoStmt) {// true-while
					System.out.println("BackJump is GotoStmt: " + backJump);
					assert (((GotoStmt) backJump).getTarget() == header);
					((GotoStmt) backJump).setTarget(loopExitTarget);

					// stmts.remove(backJump);
				} else {// while
					System.out.println("BackJump is OtherStmt: " + backJump);
					// assert(target == stmts.getSuccOf(header));

					GotoStmt newGoto = jimple.newGotoStmt(loopExitTarget);
					stmts.insertAfter(newGoto, backJump);
				}
			}
		}

	}

	// determine whether a loop statement back jumps to header
	private static boolean reachHeader(Stmt stmt, BriefUnitGraph cfg, Stmt head) {
		// TODO Auto-generated method stub
		for (Unit suc : cfg.getSuccsOf(stmt)) {
			if (suc == head) {
				return true;
			}
		}
		return false;
	}

	private static void unrollLoop(Loop loop, SootMethod method) {
		// //get the exit target of the loop
		// Stmt target = getTargetOfLoopExits(loop);

		Chain stmts = method.retrieveActiveBody().getUnits()
				.getNonPatchingChain();

		/*--unroll the loop once--*/
		// clone loop statements
		Map<Stmt, Stmt> unitsMap = new HashMap<Stmt, Stmt>();
		Set<Stmt> newStmts = new HashSet<Stmt>();
		for (Stmt loopstmt : loop.getLoopStatements()) {
			Stmt newStmt = (Stmt) loopstmt.clone();
			unitsMap.put(loopstmt, newStmt);
			newStmts.add(newStmt);
		}

		// insert cloned statements
		for (Iterator<Stmt> it = stmts.snapshotIterator(); it.hasNext();) {
			Stmt s = it.next();
			if (unitsMap.containsKey(s)) {
				Stmt newStmt = unitsMap.get(s);
				stmts.insertAfter(newStmt, stmts.getLast());

				Stmt suc = (Stmt) stmts.getSuccOf(s);
				if (!unitsMap.containsKey(suc) && !newStmts.contains(suc)) {
					Stmt newGoto = jimple.newGotoStmt(suc);
					stmts.insertAfter(newGoto, newStmt);
				}
			}
		}

		// change targets accordingly
		for (Unit oldStmt : unitsMap.keySet()) {
			if (oldStmt instanceof IfStmt) {
				IfStmt newIfStmt = (IfStmt) unitsMap.get(oldStmt);
				Stmt iftarget = ((IfStmt) oldStmt).getTarget();
				if (unitsMap.containsKey(iftarget)) {
					newIfStmt.setTarget(unitsMap.get(iftarget));
				}
			} else if (oldStmt instanceof GotoStmt) {
				GotoStmt newGotoStmt = (GotoStmt) unitsMap.get(oldStmt);
				Stmt gototarget = (Stmt) ((GotoStmt) oldStmt).getTarget();
				if (unitsMap.containsKey(gototarget)) {
					newGotoStmt.setTarget(unitsMap.get(gototarget));
				}
			}
		}

		// redirect backJump
		Stmt backJump = loop.getBackJumpStmt();
		Stmt newBackJump = unitsMap.get(backJump);
		Stmt header = loop.getHead();
		Stmt newHeader = unitsMap.get(header);

		if (backJump instanceof IfStmt) {// do-while
			System.out.println("BackJump is IfStmt: " + backJump);
			assert (((IfStmt) backJump).getTarget() == header);
			((IfStmt) backJump).setTarget(newHeader);

			stmts.remove(newBackJump);
		} else if (backJump instanceof GotoStmt) {// true-while
			System.out.println("BackJump is GotoStmt: " + backJump);
			assert (((GotoStmt) backJump).getTarget() == header);
			((GotoStmt) backJump).setTarget(newHeader);

			stmts.remove(newBackJump);
		} else {// while
			System.out.println("BackJump is OtherStmt: " + backJump);
			GotoStmt newGoto = jimple.newGotoStmt(newHeader);
			stmts.insertAfter(newGoto, backJump);

			// assert(target == stmts.getSuccOf(header));
			GotoStmt newGoto2 = jimple.newGotoStmt((Stmt) stmts
					.getSuccOf(header));
			stmts.insertAfter(newGoto2, newBackJump);
		}
	}

	/**
	 * get the exit target of the loop it's no necessary to be the target of
	 * loop exit, e.g., while(true) instead, it should be the right next stmt in
	 * the chain after the loop.
	 * 
	 * @param loop
	 * @return
	 */
	private static Stmt getTargetOfLoopExits(Loop loop) {
		assert (!loop.getLoopExits().isEmpty());
		Stmt header = loop.getHead();
		Stmt backJump = loop.getBackJumpStmt();
		// while loop
		if (header instanceof IfStmt) {
			assert (loop.getLoopExits().contains(header));
			assert (loop.targetsOfLoopExit(header).size() == 1);
			return loop.targetsOfLoopExit(header).iterator().next();
		}

		if (backJump instanceof IfStmt) {// do-while
			assert (loop.getLoopExits().contains(backJump));
			assert (loop.targetsOfLoopExit(backJump).size() == 1);
			return loop.targetsOfLoopExit(backJump).iterator().next();
		} else if (backJump instanceof GotoStmt) {// while(true)
			Stmt next = (Stmt) STMTS.getSuccOf(backJump);
			return next;
		} else {// should be while loop
			System.err.println("unexpected loop case!!!");
			System.err.println("loop: " + loop.getLoopStatements());
			throw new RuntimeException();
			// return loop.targetsOfLoopExit(backJump).iterator().next();
		}

	}

	// private static Stmt getTargetOfLoopExits(Loop loop) {
	// //get all the targets of the loop exits
	// assert(!loop.getLoopExits().isEmpty());
	// Set<Stmt> targets = new HashSet<Stmt>();
	// for(Stmt exit: loop.getLoopExits()) {
	// targets.addAll(loop.targetsOfLoopExit(exit));
	// }
	//
	// //filter out GotoStmt
	// for(Iterator<Stmt> it = targets.iterator(); it.hasNext();) {
	// Stmt target = it.next();
	// if(target instanceof GotoStmt) {
	// it.remove();
	// Stmt finalTarget = getGotoTarget((GotoStmt)target);
	// targets.add(finalTarget);
	// }
	// }
	//
	// //return the exit target
	// assert(targets.size() > 0);
	// if(targets.size() > 1) {
	// System.err.println("Multiple exit targets: " + targets);
	// }
	// return targets.iterator().next();
	//
	// }
	//
	// private static Stmt getGotoTarget(GotoStmt it) {
	// // TODO Auto-generated method stub
	// Stmt target = (Stmt) it.getTarget();
	// while(target instanceof GotoStmt) {
	// target = (Stmt)((GotoStmt) target).getTarget();
	// }
	// return target;
	// }

	private static void printLoopInfo(Loop loop) {
		System.out.println("\nLoop ===>");
		System.out.println(loop.getLoopStatements());
		Stmt head = loop.getHead();
		System.out.println("Loop head: " + head.toString());
		Stmt backJump = loop.getBackJumpStmt();
		System.out.println("Loop backjump: " + backJump.toString());

		for (Stmt exit : loop.getLoopExits()) {
			System.out.println("Loop exit: " + exit.toString());
			System.out.println("Loop targets: "
					+ loop.targetsOfLoopExit(exit).toString());
		}
	}

	private static boolean hasNestedLoops(LoopNestTree loops) {
		MyLoopNestTreeComparator comp = new MyLoopNestTreeComparator();
		for (Loop loop1 : loops) {
			for (Loop loop2 : loops) {
				int r = comp.compare(loop1, loop2);
				if (r == 1 || r == -1) {
					return true;
				}
			}
		}
		return false;
	}

	private static class MyLoopNestTreeComparator implements Comparator<Loop> {

		public int compare(Loop loop1, Loop loop2) {
			Collection<Stmt> stmts1 = loop1.getLoopStatements();
			Collection<Stmt> stmts2 = loop2.getLoopStatements();
			if (stmts1.equals(stmts2)) {
				assert loop1.getHead().equals(loop2.getHead()); // should really
																// have the same
																// head then
				// equal (same) loops
				return 0;
			} else if (stmts1.containsAll(stmts2)) {
				// 1 superset of 2
				return 1;
			} else if (stmts2.containsAll(stmts1)) {
				// 1 subset of 2
				return -1;
			}
			// overlap (?) or disjoint: order does not matter;
			// however we must *not* return 0 as this would only keep one of the
			// two loops;
			// hence, return 1
			return 2;
		}
	}

}

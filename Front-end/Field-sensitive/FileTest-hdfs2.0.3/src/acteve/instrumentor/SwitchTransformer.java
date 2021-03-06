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
import soot.Body;
import soot.Local;
import soot.Immediate;
import soot.jimple.TableSwitchStmt;
import soot.jimple.LookupSwitchStmt;
import soot.jimple.Stmt;
import soot.jimple.IfStmt;
import soot.jimple.Constant;
import soot.jimple.IntConstant;
import soot.jimple.Jimple;
import soot.jimple.GotoStmt;
import soot.jimple.EqExpr;

import java.util.ArrayList;
import java.util.List;

public final class SwitchTransformer {

	public static final Jimple jimple = Jimple.v();

	public static final BodyEditor editor = new BodyEditor();

	private SwitchTransformer() {
	}

	public static void transform(SootMethod method) {
		Body body = method.retrieveActiveBody();
		editor.newBody(body, method);

		while (editor.hasNext()) {
			Stmt s = editor.next();
			// System.out.println(">> " + s);

			if (s.branches() && !(s instanceof GotoStmt)) {
				if (s instanceof LookupSwitchStmt) {
					LookupSwitchStmt ls = (LookupSwitchStmt) s;
					handleLookupSwitchStmt(ls);
				} else if (s instanceof TableSwitchStmt) {
					TableSwitchStmt ts = (TableSwitchStmt) s;
					handleTableSwitchStmt(ts);
				} else {
					if (!(s instanceof IfStmt))
						throw new RuntimeException(
								"Unknown kind of branch stmt: " + s);
				}
			}
		}
	}

	private static void handleTableSwitchStmt(TableSwitchStmt ts) {
		Immediate key = (Immediate) ts.getKey();
		if (key instanceof Constant)
			return;
		int high = ts.getHighIndex();
		int low = ts.getLowIndex();
		List<IntConstant> values = new ArrayList();
		for (int i = low; i <= high; i++)
			values.add(IntConstant.v(i));
		List<Unit> targets = ts.getTargets();
		Unit defaultTarget = ts.getDefaultTarget();
		convertSwitchToBranches((Local) key, values, targets, defaultTarget);
	}

	private static void handleLookupSwitchStmt(LookupSwitchStmt ls) {
		Immediate key = (Immediate) ls.getKey();
		if (key instanceof Constant)
			return;
		List<IntConstant> values = ls.getLookupValues();
		List<Unit> targets = ls.getTargets();
		Unit defaultTarget = ls.getDefaultTarget();
		convertSwitchToBranches((Local) key, values, targets, defaultTarget);
	}

	private static void convertSwitchToBranches(Local key,
			List<IntConstant> values, List<Unit> targets, Unit defaultTarget) {
		int n = values.size();
		if (targets.size() != n)
			throw new RuntimeException(
					"Number of values and targets do not match.");
		for (int i = 0; i < n; i++) {
			EqExpr cond = jimple.newEqExpr(key, (IntConstant) values.get(i));
			IfStmt ifStmt = jimple.newIfStmt(cond, targets.get(i));
			editor.insertStmt(ifStmt);
		}
		editor.insertStmt(jimple.newGotoStmt(defaultTarget));
		editor.removeOriginalStmt();
	}
}

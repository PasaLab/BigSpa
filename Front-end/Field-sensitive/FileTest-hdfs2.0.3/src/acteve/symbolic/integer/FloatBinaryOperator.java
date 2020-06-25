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
package acteve.symbolic.integer;

public class FloatBinaryOperator extends BinaryOperator {
	public FloatBinaryOperator(String op) {
		super(op);
	}

	public Expression apply(Expression leftOp, Expression rightOp) {
		if (leftOp instanceof SymbolicLong) {
			leftOp = ((SymbolicLong) leftOp)._cast(Types.FLOAT);
		} else if (leftOp instanceof SymbolicInteger) {
			leftOp = ((SymbolicInteger) leftOp)._cast(Types.FLOAT);
		} else if (leftOp instanceof BinaryLongExpression) {
			leftOp = ((BinaryLongExpression)leftOp)._cast(Types.FLOAT);
		} else if (leftOp instanceof BinaryDoubleExpression) {
			leftOp = ((BinaryDoubleExpression)leftOp)._cast(Types.FLOAT);
		} else if (leftOp instanceof BinaryIntegerExpression) {
			leftOp = ((BinaryIntegerExpression) leftOp)._cast(Types.FLOAT);
		}
		FloatExpression left = (FloatExpression) leftOp;

		if (rightOp instanceof SymbolicInteger) {
			rightOp = ((SymbolicInteger) rightOp)._cast(Types.FLOAT);
		} else if (rightOp instanceof SymbolicLong) {
			rightOp = ((SymbolicLong) rightOp)._cast(Types.FLOAT);
		} else if (rightOp instanceof BinaryLongExpression) {
			rightOp = ((BinaryLongExpression) rightOp)._cast(Types.FLOAT);
		} else if (rightOp instanceof IntegerConstant) {
			rightOp = ((IntegerConstant)rightOp)._cast(Types.FLOAT);
		} else if (rightOp instanceof BinaryIntegerExpression) {
			rightOp = ((BinaryIntegerExpression)rightOp)._cast(Types.FLOAT);
		} else if (rightOp instanceof LongConstant) {
			rightOp = ((LongConstant)rightOp)._cast(Types.FLOAT);
		}
		FloatExpression right = (FloatExpression) rightOp;
		// if(left instanceof Constant && right instanceof Constant)
		// assert false;
		return new BinaryFloatExpression(this, left, right);
	}
}

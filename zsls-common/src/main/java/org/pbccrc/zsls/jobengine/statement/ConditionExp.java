package org.pbccrc.zsls.jobengine.statement;

public class ConditionExp implements ExpObj {
	
	public static ConditionExp EXP_CODE_OK		= ExpParser.parse("CODE = OK");
	public static ConditionExp EXP_CODE_FAIL	= ExpParser.parse("CODE = FAIL");
	
	private Operator op;
	
	private ExpObj left;
	
	private ExpObj right;
	
	public Operator getOp() {
		return op;
	}

	public void setOp(Operator op) {
		this.op = op;
	}

	public ExpObj getLeft() {
		return left;
	}

	public void setLeft(ExpObj left) {
		this.left = left;
	}

	public ExpObj getRight() {
		return right;
	}

	public void setRight(ExpObj right) {
		this.right = right;
	}

	public boolean getValue(Param param) {
		return op.apply(left, right, param);
	}

}

package org.pbccrc.zsls.jobengine.statement;

public class Operator implements ExpObj {
	public static final int OP_AND	= 1;
	public static final int OP_OR		= 2;
	public static final int OP_EQ		= 3;
	public static final int OP_CT		= 4;
	public static final int OP_NOT	= 5;
	
	private int type;
	
	private boolean revert;
	
	public Operator(String type) {
		if (KW_OP_AND.equals(type)) {
			this.type = OP_AND;
		} else if (KW_OP_OR.equals(type)) {
			this.type = OP_OR;
		} else if (KW_OP_NEQ.equals(type)) {
			this.type = OP_EQ;
			this.revert = true;
		} else if (KW_OP_EQ.equals(type)) {
			this.type = OP_EQ;
		} else if (KW_OP_CT.equals(type)) {
			this.type = OP_CT;
		} else if (KW_OP_NOT.equals(type)) {
			this.type = OP_NOT;
		}
		assert(this.type >= OP_AND && this.type <= OP_CT);
	}
	
	public void setRevert(boolean revert) {
		this.revert = revert;
	}

	public boolean apply(ExpObj left, ExpObj right, Param param) {
		switch (type) {
		case OP_AND:
			ConditionExp cleft = (ConditionExp)left;
			ConditionExp cright = (ConditionExp)right;
			return cleft.getValue(param) && cright.getValue(param);
			
		case OP_OR:
			cleft = (ConditionExp)left;
			cright = (ConditionExp)right;
			return cleft.getValue(param) || cright.getValue(param);
			
		case OP_EQ:
			VarExp vleft = (VarExp)left;
			VarExp vright = (VarExp)right;
			String value = param.get(vleft.getValue());
			return revert ? 
					!(value != null && value.equals(vright.getValue())) :
					value != null && value.equals(vright.getValue());
			
		case OP_CT:
			vleft = (VarExp)left;
			vright = (VarExp)right;
			value = param.get(vleft.getValue());
			return revert ?
					!(value != null && value.contains(vright.getValue())) :
					value != null && value.contains(vright.getValue());
					
		case OP_NOT:
			cright = (ConditionExp)right;
			return !cright.getValue(param);
		}
		
		return false;
	}

}

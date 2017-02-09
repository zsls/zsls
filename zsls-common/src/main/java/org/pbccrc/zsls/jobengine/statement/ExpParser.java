package org.pbccrc.zsls.jobengine.statement;

import java.util.Arrays;

import org.pbccrc.zsls.exception.ZslsRuntimeException;

public class ExpParser {
	
	private static final int DISABLED = -1;
	private static final int MATCH_CONTINUE = -1;
	private static final int MATCH_FAIL = -2;
	private static final int DEFAULT_SIZE = 50;
	private static final int DEFAULT_INCREMENT = 50;
	
	//symbolExp can't contain '(', ')', ' ', ''', '"',
	final static char[][] SYMBOL_EXPS = {
		ExpObj.KW_OP_EQ.toCharArray(),
		ExpObj.KW_OP_NEQ.toCharArray()
	};
	
	final static char[][] COMPOSITE_OP = {
		ExpObj.KW_OP_AND.toCharArray(),
		ExpObj.KW_OP_OR.toCharArray(),
		ExpObj.KW_OP_NOT.toCharArray()
	};
	final static char[][] ABLE_UNDER_NOT_OP = {
		ExpObj.KW_OP_CT.toCharArray()
	};
	final static char[][] NORMAL_OP = {
		ExpObj.KW_OP_CT.toCharArray(),
		ExpObj.KW_OP_EQ.toCharArray(),
		ExpObj.KW_OP_NEQ.toCharArray()
	};
	
	final static char[][] VAR_EXPS = {
		ExpObj.KW_VAR_CODE.toCharArray(),
		ExpObj.KW_VAR_MSG.toCharArray()
	};
	final static char[] NOT_OP = ExpObj.KW_OP_NOT.toCharArray();
		
	public static ConditionExp parse(String statement) throws ZslsRuntimeException {
		char[][] expNodes = splitWords(statement, SYMBOL_EXPS);
		return createCondition(expNodes);
	}
	
	private static ConditionExp createCondition(char[][] expNodes) {
		
		ConditionExp result = new ConditionExp();
		int cOpIndex = -1, nOpIndex = -1, nOpCount = 0, brackets = 0, errIndex = -1;
		
		expNodes = unWrapBracket(0, expNodes.length - 1, expNodes);
		
		if (expNodes == null || expNodes.length == 0)
			throw new ZslsRuntimeException("errors occur in word parsing phase");
		
		for (int i = 0; i < expNodes.length; i++) {
			char[] exp = expNodes[i];
			if (exp.length == 1 && exp[0] == '(') 
				brackets ++;
			if (exp.length == 1 && exp[0] == ')') 
				brackets --;
			if (matchExpression(exp, COMPOSITE_OP) && brackets == 0) {
				cOpIndex = i;
				if (!isNOT_OP(exp))
					break;
			}
			if (matchExpression(exp, NORMAL_OP)) {
				nOpIndex = i;
				nOpCount ++;
			}
		}
		
		if (brackets == 0) {
			if (cOpIndex == -1 && nOpCount == 1 && nOpIndex != -1) {
				if (matchExpression(expNodes[nOpIndex], NORMAL_OP) 
						&& nOpIndex - 1 >= 0
						&& matchExpression(expNodes[nOpIndex - 1], VAR_EXPS) 
						&& nOpIndex + 1 < expNodes.length
						&& isParam(expNodes[nOpIndex + 1])
						&& nOpIndex + 1 == expNodes.length - 1
					) {
					Operator op = new Operator(String.valueOf(expNodes[nOpIndex]));
					result.setOp(op);
					VarExp var = new VarExp(String.valueOf(expNodes[nOpIndex - 1]));
					result.setLeft(var);
					VarExp param = new VarExp(String.valueOf(expNodes[nOpIndex + 1]));
					result.setRight(param);
					return result;
				}
				errIndex = nOpIndex;
			} else if (cOpIndex >= 0 && isNOT_OP(expNodes[cOpIndex])) {
				if (legalNOT_OP(cOpIndex, expNodes)) {
					//transform like MSG NOT CONTAINS OK -> NOT MSG CONTAINS OK
					if (cOpIndex > 0 && matchExpression(expNodes[cOpIndex - 1], VAR_EXPS)) {
						char[] notOp = expNodes[cOpIndex];
						char[] varExp = expNodes[cOpIndex - 1];
						expNodes[cOpIndex] = varExp;
						expNodes[cOpIndex - 1] =  notOp;
						return createCondition(expNodes);
					} else {
						Operator op = new Operator(String.valueOf(expNodes[cOpIndex]));
						result.setOp(op);
						result.setRight(createCondition(Arrays.copyOfRange(expNodes, cOpIndex + 1, expNodes.length)));
						result.setLeft(null);
						return result;
					}
				} 
				errIndex = cOpIndex;
			} else if (cOpIndex == -1 && nOpCount > 1) {
				errIndex = nOpIndex;
			} else if (cOpIndex == -1)
				throw new ZslsRuntimeException("Invalid expression: no operator");
			
			if (errIndex >= 0) {
				StringBuffer errMsg = new StringBuffer();
				errMsg.append("error occurs near: ")
						.append(errIndex - 2>= 0 ? String.valueOf(expNodes[errIndex - 2]) + " " : "" )
						.append(errIndex - 1>= 0 ? String.valueOf(expNodes[errIndex - 1]) + " " : "" )
						.append(String.valueOf(expNodes[errIndex]) + " ")
						.append(errIndex + 1 < expNodes.length ? String.valueOf(expNodes[errIndex + 1]) + " " : "");
				throw new ZslsRuntimeException(errMsg.toString());
			}
				
			int startL = 0, endL = cOpIndex, startR = cOpIndex + 1, endR = expNodes.length;
			result.setLeft(createCondition(Arrays.copyOfRange(expNodes, startL, endL)));
			result.setRight(createCondition(Arrays.copyOfRange(expNodes, startR, endR)));
			result.setOp(new Operator(String.valueOf(expNodes[cOpIndex])));
			return result;
		} else {	
			throw new ZslsRuntimeException("brackets is uncompleted");
		}
	}
	
	private static boolean isNOT_OP(char[] operator) {
		if (operator == null || operator.length != NOT_OP.length)
			return false;
		for (int i = 0 ; i < operator.length; i++) {
			if (operator[i] != NOT_OP[i])
				return false;
		}
		return true;
	}
	
	private static boolean legalNOT_OP(int notIndex, char[][] expNodes) {
		if (notIndex >= 1 && notIndex + 2 < expNodes.length 
				&& matchExpression(expNodes[notIndex - 1], VAR_EXPS) 
				&& ableUnderNOT_OP(expNodes[notIndex + 1]) 
				&& isParam(expNodes[notIndex + 2]))
			return true;
		int brackets = 0;
		for (int i = notIndex + 1; i < expNodes.length; i ++) {
			if (expNodes[i].length == 1 && expNodes[i][0] == '(')
				brackets ++;
			if (expNodes[i].length == 1 && expNodes[i][0] == ')')
				brackets --;
			if (brackets == 0)
				return true;
		}
		return false; 
	}
	
	private static boolean ableUnderNOT_OP(char[] operator) {
		return matchExpression(operator, ABLE_UNDER_NOT_OP);
	}
	private static char[][] unWrapBracket(int start, int end, char[][] expNodes) throws ZslsRuntimeException {
		if (expNodes == null || start >= end || start < 0 || end >= expNodes.length)
			throw new ZslsRuntimeException("invalid param");
		int i = 0, firstEnd = 0, brackets = 0;
		for (; i < expNodes.length; i++) {
			if (expNodes[start + i] != null && expNodes[start + i].length == 1 && expNodes[start + i][0] == '(')
				brackets ++;
			if (expNodes[i] != null && expNodes[i].length == 1 && expNodes[i][0] == ')')
				brackets --;
			if (brackets == 0) {
				firstEnd = i;
				break;
			}
		}
		if (firstEnd == end)
			return unWrapBracket(start + 1, end - 1, expNodes);
		return Arrays.copyOfRange(expNodes, start, end + 1);
	}
	
	private static boolean isParam(char[] param) {
		if (matchExpression(param, NORMAL_OP) || matchExpression(param, COMPOSITE_OP) || matchExpression(param, VAR_EXPS))
			return false;
		return true;
	}
	
	private static boolean matchExpression(char[] operator,  final char[][] src) {
		byte[] filter = new byte[src.length];
		int result;
		for (int i = 0; i < filter.length; i++) {
			if (operator.length != src[i].length)
				filter[i] = DISABLED;
		}
		for (int i = 0; i < operator.length; ) {
			result = matchStream(operator[i], filter, i++, src);
			if (result >= 0)
				return true;
			if (result == MATCH_FAIL)
				return false;
		}
		return false;
	}
	
	private static int matchStream(char c, byte[] filter, int symbolIndex, final char[][] src){
		int sum = 0, i = 0;
		for (; i < src.length; i++) {
			if (src[i].length > symbolIndex &&  filter[i] != DISABLED) {
				filter[i] = (byte) (c != src[i][symbolIndex] ? DISABLED : filter[i] + 1);
			}
			sum += filter[i];
		}
		if (sum > (DISABLED * src.length)) {
			int maxPos = maxMatch(filter, src);
			if (maxPos >= 0) 
				return maxPos;
		}
		return sum <= (DISABLED * src.length) ? MATCH_FAIL : MATCH_CONTINUE;
	}
	
	private static int maxMatch(byte[] filter, final char[][] src) {
		int max = -1, index = 0, maxPos = -1;
		for (; index < src.length; index ++) {
			if (filter[index] == -1)
				continue; 
			if (filter[index] < src[index].length)
				return -1;
			if (max < filter[index]) {
				max = filter[index];
				maxPos = index;
			}
		}
		return maxPos;
	}
	
	private static int matchSymbolExp(char c, byte[] filter, int index) {	
		return matchStream(c, filter, index, SYMBOL_EXPS);
	}
	
	private static char[][] addElement(char[] c, char[][] src, int pos) {
		if (pos >= src.length) {
			char[][] data = new char[src.length + DEFAULT_INCREMENT][];
			System.arraycopy(src, 0, data, 0, src.length);
			src = data;
		}
		src[pos] = c;
		return src;
	}
	
	private static char[][] splitWords(String statement, char[][] keyWords) throws ZslsRuntimeException {
		if (statement == null)
			throw new ZslsRuntimeException("statement is null");
		
		char[] cs = statement.toCharArray();
		char[][] data = new char[DEFAULT_SIZE][];
		byte[] symbolFilter = new byte[keyWords.length];
		
		int brackets = 0, symbolIndex = 0, wIndex = 0;
		boolean waitForQuot1 = false, waitForQuot2 = false;
		
		int wStart = 0, wEnd = 0;
		for (int i = 0; wEnd < cs.length; i++) {
			char c = cs[i];
			switch(c) {
			case '\'': 
				if (!waitForQuot2) {
					waitForQuot1 = !waitForQuot1;
					// if wait for quote, should add the possible word before, either should add the word it contains
					if (wStart < wEnd) 
						data = addElement(Arrays.copyOfRange(cs, wStart, wEnd), data, wIndex ++);
					wStart = i + 1;
					wEnd = wStart;
				} else 
					wEnd ++;
				break;
			case '"': 
				if (!waitForQuot1) {
					waitForQuot2 = !waitForQuot2; 
					if (wStart < wEnd) 
						data = addElement(Arrays.copyOfRange(cs, wStart, wEnd), data, wIndex ++);
					wStart = i + 1;
					wEnd = wStart;
				} else 
					wEnd ++;
				break;
			default :
				if (waitForQuot1 || waitForQuot2) {
					wEnd ++;
				} else {
					if (brackets < 0)
						throw new ZslsRuntimeException("syntax error: bracket is uncompleted.");
					
					if (c == '(' || c == ')') {
						if (wStart < wEnd) 
							data = addElement(Arrays.copyOfRange(cs, wStart, wEnd), data, wIndex ++);
						brackets += c== '('? 1 : -1;
						data = addElement(new char[]{c}, data, wIndex ++);
						wStart = i + 1;
						wEnd = wStart;
						continue;
					}
					
					if (c == ' ') {
						if (wStart < wEnd) {
							data = addElement(Arrays.copyOfRange(cs, wStart, wEnd), data, wIndex ++);
						}
						wStart = i + 1;
						wEnd = wStart;
						continue;
					}
					
					int matchPos = matchSymbolExp(c, symbolFilter,symbolIndex++);
					if (matchPos >= 0) {
						if (wStart < (wEnd - (keyWords[matchPos].length - 1 ))) 
							data = addElement(Arrays.copyOfRange(cs, wStart, wEnd + 1 - keyWords[matchPos].length), data, wIndex ++);
						data = addElement(Arrays.copyOfRange(cs, wEnd + 1 - keyWords[matchPos].length, wEnd + 1), data, wIndex ++);
						wStart = i + 1;
						wEnd = wStart;
						Arrays.fill(symbolFilter, 0, keyWords.length, (byte)0);
						symbolIndex = 0;
						continue;
					}
					
					if (matchPos == MATCH_FAIL) {
						Arrays.fill(symbolFilter, 0, keyWords.length, (byte)0);
						symbolIndex = 0;
					}
					wEnd ++;
				}
			}
		}
		if (wStart < wEnd) 
			data = addElement(Arrays.copyOfRange(cs, wStart, wEnd), data, wIndex ++);
		
		if (brackets != 0 || waitForQuot1 || waitForQuot2)
			throw new ZslsRuntimeException("syntax error: bracket or quote is uncompleted.");
		return Arrays.copyOf(data, wIndex);
	}
 }

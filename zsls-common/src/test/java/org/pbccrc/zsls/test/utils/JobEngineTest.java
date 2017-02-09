package org.pbccrc.zsls.test.utils;

import org.pbccrc.zsls.jobengine.statement.ConditionExp;
import org.pbccrc.zsls.jobengine.statement.Param;
import org.pbccrc.zsls.jobengine.statement.ExpParser;

public class JobEngineTest {
	public static void main(String[] args) throws Exception {
		Param param = new Param();
		param.add("MSG", "IT OK");
		param.add("CODE", "2");
		String s1 = "MSG CONTAINS \"'OK'\" AND CODE =1";//->ON NOT MSG CONTAINS OK AND CODE = 2 DO TASK123
		String s2 = "MSG NOT CONTAINS '\"OK\"' AND CODE!=2 ";
		String s3 = "NOT (MSG CONTAINS 'OK' AND CODE = 2)";
		String s4 = "MSG CONTAINS OK AND (CODE = 1 OR CODE = 2)";
		String s5 = "(MSG CONTAINS ERROR OR MSG CONTAINS OK) AND (CODE = 1 OR CODE = 2)";
		String s6 = "NOT (MSG CONTAINS OK) OR CODE = 2";
		String s7 = "(NOT (((MSG NOT CONTAINS OK)) OR ((CODE = 2))))";
		ConditionExp ce = ExpParser.parse(s1);
		
		//System.out.println(ce.toString());
		assert(ce.getValue(param) == true);
		System.out.println(s1 + " : "+ce.getValue(param));
		
		ce = ExpParser.parse(s2);
		assert(ce.getValue(param) == false);
		System.out.println(s2 + " : "+ce.getValue(param));
		
		ce = ExpParser.parse(s3);
		assert(ce.getValue(param) == false);
		System.out.println(s3 + " : "+ce.getValue(param));
		
		ce = ExpParser.parse(s4);
		assert(ce.getValue(param) == true);
		System.out.println(s4 + " : "+ce.getValue(param));
		
		ce = ExpParser.parse(s5);
		assert(ce.getValue(param) == true);
		System.out.println(s5 + " : "+ ce.getValue(param));
		
		ce = ExpParser.parse(s6);
		assert(ce.getValue(param) == true);
		System.out.println(s6 + " : "+ ce.getValue(param));
		
		ce = ExpParser.parse(s7);
		assert(ce.getValue(param) == false);
		System.out.println(s7 + " : "+ ce.getValue(param));
	
	}
}

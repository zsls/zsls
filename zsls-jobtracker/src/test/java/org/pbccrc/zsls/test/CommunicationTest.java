package org.pbccrc.zsls.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.pbccrc.zsls.api.client.old.IReEntry;
import org.pbccrc.zsls.api.client.old.IRelation;
import org.pbccrc.zsls.api.client.old.IScheduleUnit;
import org.pbccrc.zsls.api.client.old.IUserTask;
import org.pbccrc.zsls.api.client.old.SchedResult;
import org.pbccrc.zsls.api.thrift.utils.SchedClient;
import org.pbccrc.zsls.utils.ThreadLocalBuffer;

import com.google.gson.Gson;

public class CommunicationTest {
	
	public static void main(String[] args) throws IOException {
//		testGson();
		testClient();
//		testCheckUnitExist("ORIG_INTEG_DECOM1","ORIG_INTEG_DECOM");
//		testUnitRelation("ORIG_INTEG_DECOM");
	}
	
	public static List<IScheduleUnit> genUnitList() {
		List<IScheduleUnit> unitList = new ArrayList<IScheduleUnit>();
		unitList.add(genUnit("original"));
		unitList.add(genUnit("original"));
		unitList.add(genUnit("original"));
		unitList.add(genUnit("original"));
		/*unitList.add(genUnit("file"));
		unitList.add(genUnit("file"));
		unitList.add(genUnit("file"));*/
		return unitList;
	}
	
	public static IScheduleUnit genSimpleUnit(String domain, int index) {
		IUserTask t1 = new IUserTask("1");
		t1.params.put("param1", "中文参数");
		t1.timeout = 15000;
		IUserTask t2 = new IUserTask("2");
		t2.params.put("param1", "PARAM1");
		t2.timeout = 15000;
		IScheduleUnit unit = new IScheduleUnit(domain);
		unit.swiftNum = "swift" + index;
		unit.independentTasks.add(t1);
		unit.independentTasks.add(t2);
		return unit;
	}
	
	public static void testUnitRelation(String domain) throws IOException {
		SchedClient client = new SchedClient("127.0.0.1:5555;127.0.0.1:5556");
		
		IScheduleUnit unit = genSimpleUnit(domain, 0);
		unit.preUnit = "unit000010001";
		SchedResult result = client.send2Schedule(unit);
		String firstId = result.generatedId;
		System.out.println("Send 1th unit: " + result.generatedId);
		
		unit = genSimpleUnit(domain, 1);
		unit.preUnit = firstId;
		result = client.send2Schedule(unit);
		System.out.println("Send 2th unit: " + result.generatedId);
	}

	public static IScheduleUnit genUnit(String domain) {
		
		IUserTask t1 = new IUserTask("1");
		t1.params.put("param1", "PARAM1");
		t1.retryOp = "CODE != OK; 1";
		IUserTask t2 = new IUserTask("2");
		t2.params.put("param2", "PARAM2");
		t2.retryOp = "CODE != OK; 1";
		IUserTask t3 = new IUserTask("3");
		t3.params.put("param3", "PARAM3");
		IUserTask t4 = new IUserTask("4");
		t4.params.put("param4", "PARAM4");
		IUserTask t5 = new IUserTask("5");
		t5.params.put("param5", "PARAM5");
		IUserTask t6 = new IUserTask("6");
		t6.params.put("param6", "PARAM6");
		
		IRelation relation = new IRelation();
		IReEntry e = new IReEntry("1");
		e.tasks.add(t1);
		relation.preTasks = e;
		e = new IReEntry("2");
		e.tasks.add(t2);
		relation.postTasks= e;
		
		//IRelation r2 = new IRelation();
		/*r2.preTasks = e;
		e = new IReEntry("3");
		e.tasks.add(t5);
		e.tasks.add(t6);
		r2.postTasks = e;*/

		IScheduleUnit unit = new IScheduleUnit(domain);
		unit.swiftNum = domain + "swift1";
		unit.relations.add(relation);
		unit.independentTasks.add(t3);
		unit.independentTasks.add(t4);
		unit.independentTasks.add(t5);
		unit.independentTasks.add(t6);
		unit.timeout = 20000;
//		unit.preUnit = "unit50";
		/*String preUnitId = DBHelperTest.testGetDBLastUndoneData(domain);
		System.out.println("#########preUnitId : " + preUnitId);
		if (preUnitId != null) {
			unit.preUnit = preUnitId;
		}*/
		return unit;
	}
	public static void testGson() {
		IScheduleUnit unit = genUnit("ORIG_DATA_LOAD");
		Gson gson = ThreadLocalBuffer.getGson();
		String str = gson.toJson(unit);
		System.out.println("gson string: ");
		System.out.println(str);
		unit = gson.fromJson(str, IScheduleUnit.class);
		System.out.println(unit);
	}
	public static void testClient() throws IOException {
		int unitNum = 3;
		IScheduleUnit unit = genUnit("TEST_DEV");
		SchedClient client = new SchedClient("127.0.0.1:5555;127.0.0.1:5556");
		for (int i = 0; i < unitNum; i++) {
			SchedResult result = client.send2Schedule(unit);
			System.out.println(result + "");	
		}
	}
	public static void testCheckUnitExist(String swiftNum, String domain) {
		List<String> ipList = new ArrayList<String>();
		ipList.add("127.0.0.1");
		SchedClient client = new SchedClient(ipList, 5555);
		try {
			SchedResult schedResult = client.checkUnitExist(domain, swiftNum);	
			if (schedResult.generatedId == null)
				System.out.println("#####" + swiftNum + " has not been accepted" );
			else
				System.out.println("#####" + swiftNum + " has been accepted, generatedId is " + schedResult.generatedId );
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}

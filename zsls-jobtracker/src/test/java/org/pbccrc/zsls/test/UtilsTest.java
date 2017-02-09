package org.pbccrc.zsls.test;

import java.io.IOException;

import org.pbccrc.zsls.api.client.IJobFlow;
import org.pbccrc.zsls.front.request.utils.ParamValidator;
import org.pbccrc.zsls.state.ZslsStateMachine;
import org.pbccrc.zsls.state.ZslsStateMachineFactory;
import org.pbccrc.zsls.tasks.dt.ServerQuartzJob.QJobStat;

public class UtilsTest {
	
	public static void main(String[] args) throws IOException {
		//testStateMachine();
		testCheckJobFlow();
	}
	
	public static void testStateMachine() {
		ZslsStateMachineFactory<QJobStat> stateFactory =
			new ZslsStateMachineFactory<QJobStat>(QJobStat.Init)
			.addTransition(QJobStat.Init, QJobStat.Cancel)
			.addTransition(QJobStat.Init, QJobStat.Run)
			.addTransition(QJobStat.Run, QJobStat.Finish)
			.addTransition(QJobStat.Finish, QJobStat.Run)
			.addTransition(QJobStat.Finish, QJobStat.Cancel)
			.build();
		ZslsStateMachine<QJobStat> jobStat = stateFactory.makeStateMachine();
		System.out.println(jobStat.doTransition(QJobStat.Run));
		System.out.println(jobStat.doTransition(QJobStat.Cancel));
		System.out.println(jobStat.doTransition(QJobStat.Finish));
	}

	public static void testCheckJobFlow() throws IOException {
		IJobFlow jobFlow = EngineTest.readXMLJobFlow();
		boolean result = ParamValidator.checkValid(jobFlow);
		System.out.println("JobFlow is validate: " + result);
	}
}

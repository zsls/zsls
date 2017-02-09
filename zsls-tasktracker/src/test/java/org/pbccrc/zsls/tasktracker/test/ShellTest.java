package org.pbccrc.zsls.tasktracker.test;

public class ShellTest {
	
	public static void main(String[] args) {
		testSystem();
	}
	
	public static void testSystem() {
		String osName = System.getProperty("os.name");
		System.out.println(osName);
		String java = System.getProperty("java.version");
		System.out.println(java);
	}

}

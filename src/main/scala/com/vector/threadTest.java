package com.vector;

public class threadTest {

	public static void main(String[] args) throws InterruptedException {
		app2.mt=Thread.currentThread();
		app2 t = new app2();
		t.start();
		System.out.println("Main starts");
		Thread.sleep(2000);
		System.out.println("Main ends");
	}
}

class app2 extends Thread{
	static Thread mt;
	public void run(){
		try {
			mt.join();//waits till main thread dies.
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("child thread muere");
		try {
			Thread.sleep(1000);
			System.out.println("child thread 1");
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		System.out.println("child thread 2");
		System.out.println("child thread murió");
	}
}

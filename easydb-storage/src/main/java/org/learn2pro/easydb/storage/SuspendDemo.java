package org.learn2pro.easydb.storage;

import java.util.concurrent.locks.ReentrantLock;

public class SuspendDemo {

    public static Object u = new Object();
    public static ReentrantLock lock = new ReentrantLock();
    static ChangeObjectThread t1 = new ChangeObjectThread("t1");
    static ChangeObjectThread t2 = new ChangeObjectThread("t2");

    public static class ChangeObjectThread extends Thread {

        public ChangeObjectThread(String name) {
            super.setName(name);
        }

        @Override
        public void run() {
            lock.lock();
            try {
                System.out.println("in " + getName());
                Thread.sleep(1000);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                lock.unlock();
            }
        }
    }

    public static void main(String[] args) throws InterruptedException {
        t1.start();
//        Thread.sleep(100);
        t2.start();
        Thread.sleep(2000);
    }
}

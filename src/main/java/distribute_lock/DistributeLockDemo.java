package distribute_lock;

import java.util.concurrent.TimeUnit;

public class DistributeLockDemo implements Runnable {

    private DistributedLock lock = new DistributedLock();

    public static void main(String[] args) {
        for (int i = 0; i < 5; i++) { // 开启100个线程
            //模拟分布式锁的场景
            new Thread(new DistributeLockDemo(), "Thread--" + i).start();
        }
    }

    public void run() {
        try {

            if (lock.tryLock()) {
                TimeUnit.SECONDS.sleep(3);
                System.out.println(Thread.currentThread().getName() + " ===>  处理业务逻辑");
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            lock.unlock();
        }
    }



}

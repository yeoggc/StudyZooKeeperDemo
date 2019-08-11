package distribute_lock;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

public class DistributedLock implements Watcher, Lock {

    private ZooKeeper zk;
    private String root = "/locks";//根
    private String lockName = "dl";//竞争资源标志,lockName中不能包含单词lock
    private String waitNode;//等待前一个锁
    private String myZnode;//当前锁
    private CountDownLatch latch;//计数器
    private List<Exception> exception = new ArrayList<Exception>();

    /**
     * 创建分布式锁,使用前请确认config配置的zookeeper服务可用
     */
    DistributedLock() {
        // 创建一个与服务器的连接
        try {
            zk = new ZooKeeper("localhost:2181", 5000, this);
            Stat stat = zk.exists(root, false);
            if (stat == null) {
                // 创建根节点
                zk.create(root, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        } catch (Exception e) {
            exception.add(e);
        }
    }

    /**
     * zookeeper节点的监视器
     */
    public void process(WatchedEvent event) {

        if (event.getType() == Event.EventType.NodeDeleted) {
            System.out.println(Thread.currentThread().getName() +
                    " ===> 收到监听事件NodeDeleted ，Node=" + waitNode +
                    " 接下来是唤醒等待执行的工作线程");
            if (this.latch != null) {
                this.latch.countDown();
            }
        }

    }

    public void lock() {
        if (exception.size() > 0) {
            throw new LockException(exception.get(0));
        }
        this.tryLock();
    }

    boolean waitForLock() throws InterruptedException, KeeperException {
        Stat stat = zk.exists(root + "/" + waitNode, true);
        //判断比自己小一个数的节点是否存在,如果不存在则无需等待锁,同时注册监听
        if (stat != null) {
            System.out.println(Thread.currentThread().getName() + " ===> 等待 ：" + waitNode);
            this.latch = new CountDownLatch(1);
            this.latch.await();
            this.latch = null;
        }
        return true;
    }




    private boolean tryLockInner() {
        try {
            String splitStr = "_lock_";
            if (lockName.contains(splitStr))
                throw new LockException("lockName can not contains \\u000B");
            //创建临时子节点
            myZnode = zk.create(root + "/" + lockName + splitStr, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            System.out.println(Thread.currentThread().getName() + " ==> 临时有序节点被创建: " + myZnode);
            //取出所有子节点
            List<String> subNodes = zk.getChildren(root, false);
            //取出所有lockName的锁
            List<String> lockObjNodes = new ArrayList<String>();
            for (String node : subNodes) {
                String _node = node.split(splitStr)[0];
                if (_node.equals(lockName)) {
                    lockObjNodes.add(node);
                }
            }
            Collections.sort(lockObjNodes);
            if (myZnode.equals(root + "/" + lockObjNodes.get(0))) {
                //如果是最小的节点,则表示取得锁
                return true;
            }
            //如果不是最小的节点，找到比自己小1的节点
            String subMyZNode = myZnode.substring(myZnode.lastIndexOf("/") + 1);
            waitNode = lockObjNodes.get(Collections.binarySearch(lockObjNodes, subMyZNode) - 1);
            System.out.println(Thread.currentThread().getName() + " ==> 线程没有获得锁： " + myZnode + " ， 等待前面Znode: " + waitNode);
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
        return false;
    }

    public boolean tryLock() {
        try {
            if (this.tryLockInner()) {
                return true;
            }
            return waitForLock();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }



    public void unlock() {
        try {
            System.out.println(Thread.currentThread().getName() + " ===>  线程解锁：" + myZnode);
            zk.delete(myZnode, -1);
            myZnode = null;
            zk.close();
        } catch (InterruptedException | KeeperException e) {
            e.printStackTrace();
        }
    }

    @Deprecated
    public void lockInterruptibly() throws InterruptedException {
        this.lock();
    }

    @Deprecated
    public Condition newCondition() {
        return null;
    }
    @Override
    @Deprecated
    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
        return false;
    }

    public static class LockException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        public LockException(Exception e) {
            super(e);
        }

        public LockException(String e) {
            super(e);
        }
    }

}

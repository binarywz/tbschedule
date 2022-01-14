package com.taobao.pamirs.schedule.strategy;

import com.taobao.pamirs.schedule.ConsoleManager;
import com.taobao.pamirs.schedule.IScheduleTaskDeal;
import com.taobao.pamirs.schedule.ScheduleUtil;
import com.taobao.pamirs.schedule.taskmanager.IScheduleDataManager;
import com.taobao.pamirs.schedule.taskmanager.TBScheduleManagerStatic;
import com.taobao.pamirs.schedule.zk.ScheduleDataManager4ZK;
import com.taobao.pamirs.schedule.zk.ScheduleStrategyDataManager4ZK;
import com.taobao.pamirs.schedule.zk.ZKManager;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Timer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

/**
 * 调度服务器构造器
 *
 * @author xuannan
 */
public class TBScheduleManagerFactory implements ApplicationContextAware {

    protected static transient Logger logger = LoggerFactory.getLogger(TBScheduleManagerFactory.class);

    private Map<String, String> zkConfig;

    protected ZKManager zkManager;

    /**
     * 是否启动调度管理，如果只是做系统管理，应该设置为false
     */
    public boolean start = true;
    private int timerInterval = 2000;
    /**
     * ManagerFactoryTimerTask上次执行的时间戳。<br/>
     * zk环境不稳定，可能导致所有task自循环丢失，调度停止。<br/>
     * 外层应用，通过jmx暴露心跳时间，监控这个tbschedule最重要的大循环。<br/>
     */
    public volatile long timerTaskHeartBeatTS = System.currentTimeMillis();

    /**
     * 调度配置中心客服端
     */
    private IScheduleDataManager scheduleDataManager;
    private ScheduleStrategyDataManager4ZK scheduleStrategyManager;

    /**
     * 当前任务调度服务器(Factory)实例中Strategy关联的IStrategyTask列表
     */
    private Map<String, List<IStrategyTask>> managerMap = new ConcurrentHashMap<>();

    private ApplicationContext applicationcontext;
    private String uuid;
    private String ip;
    private String hostName;

    private Timer timer;
    private ManagerFactoryTimerTask timerTask;
    protected Lock lock = new ReentrantLock();

    volatile String errorMessage = "No config Zookeeper connect infomation";
    private InitialThread initialThread;

    public TBScheduleManagerFactory() {
        this.ip = ScheduleUtil.getLocalIP();
        this.hostName = ScheduleUtil.getLocalHostName();
    }

    public void init() throws Exception {
        Properties properties = new Properties();
        for (Map.Entry<String, String> e : this.zkConfig.entrySet()) {
            properties.put(e.getKey(), e.getValue());
        }
        this.init(properties);
    }

    public void reInit(Properties p) throws Exception {
        if (this.start == true || this.timer != null || this.managerMap.size() > 0) {
            throw new Exception("调度器有任务处理，不能重新初始化");
        }
        this.init(p);
    }

    public void init(Properties p) throws Exception {
        if (this.initialThread != null) {
            this.initialThread.stopThread();
        }
        this.lock.lock();
        try {
            this.scheduleDataManager = null;
            this.scheduleStrategyManager = null;
            ConsoleManager.setScheduleManagerFactory(this);
            if (this.zkManager != null) {
                this.zkManager.close();
            }
            this.zkManager = new ZKManager(p); // 连接Zookeeper
            this.errorMessage = "Zookeeper connecting ......" + this.zkManager.getConnectStr();
            initialThread = new InitialThread(this);
            initialThread.setName("TBScheduleManagerFactory-initialThread");
            initialThread.start();
        } finally {
            this.lock.unlock();
        }
    }

    /**
     * 在Zk状态正常后回调数据初始化
     */
    public void initialData() throws Exception {
        /**
         * 递归创建永久根节点"/schedule/demo"，并写入版本信息
         */
        this.zkManager.initial();
        /**
         * 创建永久子节点"/schedule/demo/baseTaskType"
         */
        this.scheduleDataManager = new ScheduleDataManager4ZK(this.zkManager);
        /**
         * 创建永久子节点"/schedule/demo/strategy"和"/schedule/demo/factory"
         */
        this.scheduleStrategyManager = new ScheduleStrategyDataManager4ZK(this.zkManager);
        if (this.start == true) {
            /**
             * 注册调度管理器
             * 创建临时顺序子节点，节点表示主机的注册信息
             */
            this.scheduleStrategyManager.registerManagerFactory(this);
            if (timer == null) {
                timer = new Timer("TBScheduleManagerFactory-Timer");
            }
            if (timerTask == null) {
                /**
                 * 启动一个定时器检测Zookeeper状态
                 * 如果连接失败，停止所有任务后，重新连接Zookeeper服务器
                 */
                timerTask = new ManagerFactoryTimerTask(this);
                timer.schedule(timerTask, 2000, this.timerInterval);
            }
        }
    }

    /**
     * 创建调度服务器
     */
    public IStrategyTask createStrategyTask(ScheduleStrategy strategy) {
        IStrategyTask result = null;
        try {
            if (ScheduleStrategy.Kind.Schedule == strategy.getKind()) {
                String baseTaskType = ScheduleUtil.splitBaseTaskTypeFromTaskType(strategy.getTaskName());
                String ownSign = ScheduleUtil.splitOwnsignFromTaskType(strategy.getTaskName());
                result = new TBScheduleManagerStatic(this, baseTaskType, ownSign, scheduleDataManager);
            } else if (ScheduleStrategy.Kind.Java == strategy.getKind()) {
                result = (IStrategyTask) Class.forName(strategy.getTaskName()).newInstance();
                result.initialTaskParameter(strategy.getStrategyName(), strategy.getTaskParameter());
            } else if (ScheduleStrategy.Kind.Bean == strategy.getKind()) {
                result = (IStrategyTask) this.getBean(strategy.getTaskName());
                result.initialTaskParameter(strategy.getStrategyName(), strategy.getTaskParameter());
            }
        } catch (Exception e) {
            logger.error("strategy 获取对应的java or bean 出错,schedule并没有加载该任务,请确认" + strategy.getStrategyName(), e);
        }
        return result;
    }

    public void refresh() throws Exception {
        this.lock.lock();
        try {
            // 判断状态是否终止
            ManagerFactoryInfo stsInfo = null;
            boolean isException = false;
            try {
                // 获取任务管理器信息
                stsInfo = this.getScheduleStrategyManager().loadManagerFactoryInfo(this.getUuid());
            } catch (Exception e) {
                isException = true;
                logger.error("获取服务器信息有误：uuid=" + this.getUuid(), e);
            }
            if (isException == true) {
                try {
                    // 停止所有的调度任务
                    stopServer(null);
                    this.getScheduleStrategyManager().unRregisterManagerFactory(this);
                } finally {
                    reRegisterManagerFactory();
                }
            } else if (stsInfo.isStart() == false) {
                // 停止所有的调度任务
                stopServer(null);
                this.getScheduleStrategyManager().unRregisterManagerFactory(this);
            } else {
                reRegisterManagerFactory();
            }
        } finally {
            this.lock.unlock();
        }
    }

    public void reRegisterManagerFactory() throws Exception {
        /**
         * 过滤当前任务调度服务器(Factory)实例不能处理的Strategy，并停掉正在运行的StrategyTask
         */
        List<String> stopList = this.getScheduleStrategyManager().registerManagerFactory(this);
        for (String strategyName : stopList) {
            this.stopServer(strategyName);
        }
        /**
         * 重新分配调度任务
         */
        this.assignScheduleServer();
        /**
         * 根据重新分配的Strategy配置调度任务
         */
        this.reRunScheduleServer();
    }

    /**
     * 根据配置重新分配调度任务
     */
    public void assignScheduleServer() throws Exception {
        /**
         * 遍历跟当前任务调度服务器(Factory)实例相关的Strategy
         * 1.根据UUID查询本任务调度服务器(Factory)的所有相关Strategy配置
         * 2.遍历Strategy配置，重新分配调度任务
         */
        for (ScheduleStrategyRunntime run : this.scheduleStrategyManager
            .loadAllScheduleStrategyRunntimeByUUID(this.uuid)) {
            /**
             * 根据StrategyName获取FactoryList，Factory:调度服务器
             *
             * 1.Factory在启动的时候会检查自己能处理哪些Strategy，如果能处理
             * 则在"/schedule/demo/SampleTask-strategy/"路径下注册自己
             * 2.此处TaskType就是StrategyName: "SampleTask-strategy"
             * 3.ScheduleStrategyRunntime可以理解为Strategy的运行信信息(个人理解)
             */
            List<ScheduleStrategyRunntime> factoryList = this.scheduleStrategyManager
                .loadAllScheduleStrategyRunntimeByTaskType(run.getStrategyName());
            /**
             * 判断是否为Strategy的leader任务调度服务器(Factory)，即每个Strategy配置只能由leader任务调度服务器修改
             */
            if (factoryList.size() == 0 || this.isLeader(this.uuid, factoryList) == false) {
                continue;
            }
            /**
             * 根据StrategyName获取ScheduleStrategy
             */
            ScheduleStrategy scheduleStrategy = this.scheduleStrategyManager.loadStrategy(run.getStrategyName());
            /**
             * 分配factory任务数量: 计算分配到每一台server上的任务数量
             * assignTaskNumber(int serverNum, int taskItemNum, int maxNumOfOneServer)
             * int serverNum: 总的服务器数量
             * int taskItemNum: 任务项数量
             * int maxNumOfOneServer: 每个server最大任务项数目
             */
            int[] nums = ScheduleUtil.assignTaskNumber(factoryList.size(), scheduleStrategy.getAssignNum(),
                scheduleStrategy.getNumOfSingleServer());
            for (int i = 0; i < factoryList.size(); i++) {
                ScheduleStrategyRunntime factory = factoryList.get(i);
                // 更新Factory的任务数量
                this.scheduleStrategyManager
                    .updateStrategyRunntimeReqestNum(run.getStrategyName(), factory.getUuid(), nums[i]);
            }
        }
    }

    /**
     * 判断是否为leader
     * @param uuid
     * @param factoryList
     * @return
     */
    public boolean isLeader(String uuid, List<ScheduleStrategyRunntime> factoryList) {
        try {
            /**
             * 选举出每个Strategy的leader任务调度服务器(Factory)实例
             */
            long no = Long.parseLong(uuid.substring(uuid.lastIndexOf("$") + 1));
            for (ScheduleStrategyRunntime server : factoryList) {
                if (no > Long.parseLong(server.getUuid().substring(server.getUuid().lastIndexOf("$") + 1))) {
                    return false;
                }
            }
            return true;
        } catch (Exception e) {
            logger.error("判断Leader出错：uuif=" + uuid, e);
            return true;
        }
    }

    /**
     * 根据重新分配的Strategy配置调度任务
     * @throws Exception
     */
    public void reRunScheduleServer() throws Exception {
        /**
         * 遍历跟当前任务调度服务器(Factory)实例相关的Strategy
         */
        for (ScheduleStrategyRunntime run : this.scheduleStrategyManager
            .loadAllScheduleStrategyRunntimeByUUID(this.uuid)) {
            /**
             * 获取当前任务调度服务器(Factory)实例中Strategy关联的IStrategyTask列表
             */
            List<IStrategyTask> list = this.managerMap.get(run.getStrategyName());
            if (list == null) {
                list = new ArrayList<>();
                this.managerMap.put(run.getStrategyName(), list);
            }
            while (list.size() > run.getRequestNum() && list.size() > 0) {
                IStrategyTask task = list.remove(list.size() - 1);
                try {
                    task.stop(run.getStrategyName());
                } catch (Throwable e) {
                    logger.error("注销任务错误：strategyName=" + run.getStrategyName(), e);
                }
            }
            // 不足，增加调度器
            ScheduleStrategy strategy = this.scheduleStrategyManager.loadStrategy(run.getStrategyName());
            while (list.size() < run.getRequestNum()) {
                IStrategyTask result = this.createStrategyTask(strategy);
                if (null == result) {
                    logger.error("strategy 对应的配置有问题。strategy name=" + strategy.getStrategyName());
                }
                list.add(result);
            }
        }
    }

    /**
     * 终止一类任务
     */
    public void stopServer(String strategyName) {
        if (strategyName == null) {
            String[] nameList = this.managerMap.keySet().toArray(new String[0]);
            for (String name : nameList) {
                for (IStrategyTask task : this.managerMap.get(name)) {
                    try {
                        task.stop(strategyName);
                    } catch (Throwable e) {
                        logger.error("注销任务错误：strategyName=" + strategyName, e);
                    }
                }
                this.managerMap.remove(name);
            }
        } else {
            List<IStrategyTask> list = this.managerMap.get(strategyName);
            if (list != null) {
                for (IStrategyTask task : list) {
                    try {
                        task.stop(strategyName);
                    } catch (Throwable e) {
                        logger.error("注销任务错误：strategyName=" + strategyName, e);
                    }
                }
                this.managerMap.remove(strategyName);
            }

        }
    }

    /**
     * 停止所有调度资源
     */
    public void stopAll() {
        try {
            lock.lock();
            this.start = false;
            if (this.initialThread != null) {
                this.initialThread.stopThread();
            }
            if (this.timer != null) {
                if (this.timerTask != null) {
                    this.timerTask.cancel();
                    this.timerTask = null;
                }
                this.timer.cancel();
                this.timer = null;
            }
            this.stopServer(null);
            if (this.zkManager != null) {
                this.zkManager.close();
            }
            if (this.scheduleStrategyManager != null) {
                try {
                    ZooKeeper zk = this.scheduleStrategyManager.getZooKeeper();
                    if (zk != null) {
                        zk.close();
                    }
                } catch (Exception e) {
                    logger.error("stopAll zk getZooKeeper异常！", e);
                }
            }
            this.uuid = null;
            logger.info("stopAll 停止服务成功！");
        } catch (Throwable e) {
            logger.error("stopAll 停止服务失败：" + e.getMessage(), e);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 重启所有的服务
     */
    public void reStart() {
        try {
            if (this.timer != null) {
                if (this.timerTask != null) {
                    this.timerTask.cancel();
                    this.timerTask = null;
                }
                this.timer.purge();
            }
            this.stopServer(null);
            if (this.zkManager != null) {
                this.zkManager.close();
            }
            this.uuid = null;
            this.init();
        } catch (Throwable e) {
            logger.error("重启服务失败：" + e.getMessage(), e);
        }
    }

    public boolean isZookeeperInitialSucess() {
        return this.zkManager.checkZookeeperState();
    }

    public String[] getScheduleTaskDealList() {
        return applicationcontext.getBeanNamesForType(IScheduleTaskDeal.class);

    }

    public IScheduleDataManager getScheduleDataManager() {
        if (this.scheduleDataManager == null) {
            throw new RuntimeException(this.errorMessage);
        }
        return scheduleDataManager;
    }

    public ScheduleStrategyDataManager4ZK getScheduleStrategyManager() {
        if (this.scheduleStrategyManager == null) {
            throw new RuntimeException(this.errorMessage);
        }
        return scheduleStrategyManager;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        applicationcontext = applicationContext;
    }

    public Object getBean(String beanName) {
        return applicationcontext.getBean(beanName);
    }

    public String getUuid() {
        return uuid;
    }

    public String getIp() {
        return ip;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public String getHostName() {
        return hostName;
    }

    public void setStart(boolean isStart) {
        this.start = isStart;
    }

    public void setTimerInterval(int timerInterval) {
        this.timerInterval = timerInterval;
    }

    public void setZkConfig(Map<String, String> zkConfig) {
        this.zkConfig = zkConfig;
    }

    public ZKManager getZkManager() {
        return this.zkManager;
    }

    public Map<String, String> getZkConfig() {
        return zkConfig;
    }
}

class ManagerFactoryTimerTask extends java.util.TimerTask {

    private static transient Logger log = LoggerFactory.getLogger(ManagerFactoryTimerTask.class);
    TBScheduleManagerFactory factory;
    int count = 0;

    public ManagerFactoryTimerTask(TBScheduleManagerFactory aFactory) {
        this.factory = aFactory;
    }

    @Override
    public void run() {
        try {
            Thread.currentThread().setPriority(Thread.MAX_PRIORITY);
            if (this.factory.zkManager.checkZookeeperState() == false) {
                if (count > 5) {
                    log.error("Zookeeper连接失败，关闭所有的任务后，重新连接Zookeeper服务器......");
                    this.factory.reStart();

                } else {
                    count = count + 1;
                }
            } else {
                count = 0;
                // 刷新调度任务
                this.factory.refresh();
            }

        } catch (Throwable ex) {
            log.error(ex.getMessage(), ex);
        } finally {
            factory.timerTaskHeartBeatTS = System.currentTimeMillis();
        }
    }
}

class InitialThread extends Thread {

    private static transient Logger log = LoggerFactory.getLogger(InitialThread.class);
    TBScheduleManagerFactory facotry;
    boolean isStop = false;

    public InitialThread(TBScheduleManagerFactory aFactory) {
        this.facotry = aFactory;
    }

    public void stopThread() {
        this.isStop = true;
    }

    @Override
    public void run() {
        facotry.lock.lock();
        try {
            int count = 0;
            /**
             * 等待建立Zookeeper连接
             */
            while (facotry.zkManager.checkZookeeperState() == false) {
                count = count + 1;
                if (count % 50 == 0) {
                    facotry.errorMessage =
                        "Zookeeper connecting ......" + facotry.zkManager.getConnectStr() + " spendTime:" + count * 20
                            + "(ms)";
                    log.error(facotry.errorMessage);
                }
                Thread.sleep(20);
                if (this.isStop == true) {
                    return;
                }
            }
            facotry.initialData();
        } catch (Throwable e) {
            log.error(e.getMessage(), e);
        } finally {
            facotry.lock.unlock();
        }

    }

}
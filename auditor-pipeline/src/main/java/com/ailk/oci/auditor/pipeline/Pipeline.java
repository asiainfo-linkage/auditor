package com.ailk.oci.auditor.pipeline;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
//import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;


/**
 * Created with IntelliJ IDEA.
 * User: shaoaq
 * Date: 13-10-29
 * Time: 下午5:01
 * To change this template use File | Settings | File Templates.
 */
public class Pipeline implements AutoCloseable {
    public static final String QUEUE_FULL_POLICY_SHUTDOWN = "shutdown";
    public static final String QUEUE_FULL_POLICY_DROP = "drop";
    private final static Logger LOG = LoggerFactory.getLogger(Pipeline.class);
    private String serviceName;
    private String queueFullPolicy;
    private InProcessor[] inProcessors = new InProcessor[0];
    private OutProcessor[] outProcessors = new OutProcessor[0];
    private Map<ActionCode, Action[]> actionMap = new HashMap<ActionCode, Action[]>();
    private AtomicReference<Queue> queue = new AtomicReference<Queue>();
    private File configFile;
    private long lastConfigFileModifyTime = -1;
    private final OutTask outTask = new OutTask(queue, outProcessors);

    public Pipeline(File configFile) {
    	LOG.debug("[ZhangLi]Pipeline new()");
//    	Logger logger = LoggerFactory.getLogger("org.apache.hadoop.hdfs.server.namenode.FSNamesystem.audit");
        this.configFile = configFile;
        if (!configFile.exists()) {
            LOG.warn("Pipeline config file is [" + configFile.getPath() + "], but the file is not exist.");
        }
        updateConfig(); //Commented by ZhangLi.no processors for in and out,should't update config.
        new Thread(outTask, "Pipeline " + this.serviceName).start();
    }

    private void updateConfig() {
        if (this.configFile.exists()) {
            long lastModified = this.configFile.lastModified();
            if (lastModified != lastConfigFileModifyTime) {
                //如果文件存在，并且做了修改，则更新
                LOG.info("Reading com.ailk.oci.auditor.pipeline config file.");
                Properties properties = new Properties();
                try {
                    properties.load(new InputStreamReader(new FileInputStream(this.configFile), "UTF-8"));
                    updateConfig(properties);
                    lastConfigFileModifyTime = lastModified;
                } catch (Exception e) {
                    //如果文件读取失败，则很可能是正在修改。则本次先不更新，也不更新lastConfigFileModifyTime
                    LOG.warn("Read com.ailk.oci.auditor.pipeline config file failed, perhaps is modifying.");
                }
            }
        } else {
            if (lastConfigFileModifyTime != -1) {
                //如果配置文件被删除，则改用默认配置
                LOG.info("Pipeline config file is deleted,use default configuration");
                updateConfig(new Properties());
                lastConfigFileModifyTime = -1;
            }
        }

    }
    
    public void updateConfig4Processors(){
    	Properties properties = new Properties();
        try {
            properties.load(new InputStreamReader(new FileInputStream(this.configFile), "UTF-8"));
            for (Processor processor : inProcessors) {
                try {
                    processor.updateConfig(properties);
                } catch (Exception e) {
                    LOG.warn("Error update com.ailk.oci.auditor.pipeline config for inprocessor.", e);
                }
            }
            for (Processor processor : outProcessors) {
                try {
                    processor.updateConfig(properties);
                } catch (Exception e) {
                    LOG.warn("Error update com.ailk.oci.auditor.pipeline config for outprocessor.", e);
                }
            }
        } catch (Exception e) {
            //如果文件读取失败，则很可能是正在修改。则本次先不更新，也不更新lastConfigFileModifyTime
            LOG.warn("Read com.ailk.oci.auditor.pipeline config file failed, perhaps is modifying.");
        }
    }

    private void updateConfig(Properties properties) {
        serviceName = properties.getProperty("oci.auditor.com.ailk.oci.auditor.pipeline.config.service.name", "unknown");
        LOG.warn("serviceName:" + serviceName + " (oci.auditor.com.ailk.oci.auditor.pipeline.config.service.name) ");
        queueFullPolicy = properties.getProperty("oci.auditor.com.ailk.oci.auditor.pipeline.config.queue.full.policy", "shutdown");
        LOG.warn("queueFullPolicy:" + queueFullPolicy + " (oci.auditor.com.ailk.oci.auditor.pipeline.config.queue.full.policy) " + "option:" + QUEUE_FULL_POLICY_SHUTDOWN + "," + QUEUE_FULL_POLICY_DROP);
        int queueSize = Integer.parseInt(properties.getProperty("oci.auditor.com.ailk.oci.auditor.pipeline.config.queue.size", "1024"));
        LOG.warn("queueSize:" + queueSize + " (oci.auditor.com.ailk.oci.auditor.pipeline.config.queue.size) ");
        Queue newQueue = new Queue(new LinkedBlockingQueue(queueSize), new Semaphore(0));
        Queue oldQueue = this.queue.getAndSet(newQueue);
//TODO:没看明白。。往queue里放什么了？
        while (oldQueue != null && oldQueue.getQueue().size() > 0) {
            oldQueue.getElementCountInQueue().tryAcquire();
            addToQueue(oldQueue.queue.poll());
        }

        for (Processor processor : inProcessors) {
            try {
                processor.updateConfig(properties);
            } catch (Exception e) {
                LOG.warn("Error update com.ailk.oci.auditor.pipeline config for processor.", e);
            }
        }
        for (Processor processor : outProcessors) {
            try {
                processor.updateConfig(properties);
            } catch (Exception e) {
                LOG.warn("Error update com.ailk.oci.auditor.pipeline config for processor.", e);
            }
        }
        outTask.updateConfig(properties);
    }

    public void in(Object... elements) {
        updateConfig();
        for (InProcessor inProcessor : inProcessors) {
            elements = inProcessor.onElements(elements);
        }
        addToQueue(elements);
    }

    private void addToQueue(Object... elements) {
        BlockingQueue<Object> elementQueue = queue.get().getQueue();
        Semaphore elementCountInQueue = queue.get().getElementCountInQueue();
        for (Object element : elements) {
            if (elementQueue.offer(element)) {
                elementCountInQueue.release();
            } else { //放不进去，说明Queue满了
                for (Action action : actionMap.get(ActionCode.QUEUE_FULL)) {
                    action.action(this, ActionCode.QUEUE_FULL);
                }
            }
        }
    }

    public String getServiceName() {
        return serviceName;
    }

    public String getQueueFullPolicy() {
        return queueFullPolicy;
    }

    public void setActions(ActionCode actionCode, Action... actions) {
        actionMap.put(actionCode, actions);
    }

    public void setInProcessors(InProcessor... inProcessors) {
        assert inProcessors != null : "InProcessor must not be null";
        this.inProcessors = inProcessors;
    }

    public void setOutProcessors(OutProcessor... outProcessors) {
        assert outProcessors != null : "OutProcessor must not be null";
        this.outProcessors = outProcessors;
        outTask.outProcessors = outProcessors;
    }

    @Override
    public void close() throws Exception {
        for (Processor processor : inProcessors) {
            try {
                processor.close();
            } catch (Exception e) {
                LOG.warn("failed to close processor", e);
            }
        }
        for (Processor processor : outProcessors) {
            try {
                processor.close();
            } catch (Exception e) {
                LOG.warn("failed to close processor", e);
            }
        }
        try {
            outTask.close();
        } catch (Exception e) {
            LOG.warn("failed to close processor", e);
        }

    }


    public static abstract interface Action {
        public abstract void action(Pipeline pipeline, ActionCode code);
    }

    public static enum ActionCode {
        QUEUE_FULL
    }

    public static abstract interface Processor extends AutoCloseable {
        Object[] onElements(Object... elements);

        void updateConfig(Properties properties);
    }

    public static abstract interface InProcessor extends Processor {
    }

    public static abstract interface OutProcessor extends Processor {
    }

    public static class Queue {
        //队列采用先进先出队列，即先收到的元素先处理
        private final BlockingQueue<Object> queue;
        private final Semaphore elementCountInQueue;

        public Queue(BlockingQueue<Object> queue, Semaphore elementCountInQueue) {
            this.queue = queue;
            this.elementCountInQueue = elementCountInQueue;
        }

        public BlockingQueue<Object> getQueue() {
            return queue;
        }

        public Semaphore getElementCountInQueue() {
            return elementCountInQueue;
        }
    }

    public static class OutTask implements Runnable {
        private long batchInterval;
        private int batchSize;
        private AtomicReference<Queue> queue;
        boolean isRunning = true;
        private OutProcessor[] outProcessors;

        public OutTask(AtomicReference<Queue> queue, OutProcessor[] outProcessors) {
            this.queue = queue;
            this.outProcessors = outProcessors;
        }

        @Override
        public void run() {
            //循环批量从queue里取出elements
            //1. 如果queue里的元素个数达到指定大小，则处理
            //2. 如果queue里的元素没有达到指定大小，但距离上次处理已经超出指定时常，也取出处理
            //3. 如果OutProcessor处理失败，则返回的elements不能从queue里移除，防止element丢失
            //4. 目前batchSize和batchInterval都是读取的配置值，目前未考虑根据负载自动调整
            int currentBatchSize;
            while (isRunning) {
            	currentBatchSize = batchSize;
                BlockingQueue<Object> elementQueue = queue.get().getQueue();
                Semaphore elementCountInQueue = queue.get().getElementCountInQueue();
                try {
                    if (elementCountInQueue.tryAcquire(currentBatchSize, batchInterval, TimeUnit.MILLISECONDS)) {
                        //到达指定的数量，触发处理，处理queue里所有的数据
//                        currentBatchSize = batchSize + elementCountInQueue.drainPermits();
                    	 currentBatchSize = elementCountInQueue.drainPermits();//modified by zhanlgi3
                        LOG.debug(String.format("elements in queue reached batchSize[%s],begin process all elements in queue, which count is now [%s]", batchSize, currentBatchSize));
                    } else {
                        //没有到达指定的数量，但到达指定的时间，也触发处理，处理 queue里所有的数据
                        currentBatchSize = elementCountInQueue.drainPermits();
                        LOG.debug(String.format("elements in queue not reached batchSize[%s],but the wait time reached batchInterval [%s],begin process all elements in queue, which count is now [%s]", batchSize, batchInterval, currentBatchSize));
                    }
                    //取出queue里所有的数据，但先不从queue里移除，因为outProcessor处理失败的元素要保留。
//                    currentBatchSize = batchSize + elementCountInQueue.drainPermits();
//                    currentBatchSize = elementCountInQueue.drainPermits();//modified by zhanlgi3
	                if(currentBatchSize > 0 ){
	                    Object[] elements = new Object[currentBatchSize];
	                    Iterator it = elementQueue.iterator();
	                    for (int i = 0; i < currentBatchSize && it.hasNext(); i++) {
	                        elements[i] = it.next();
	                    }
	                    
	                    //循环调用OutProcessor，最终得到的elements是要保留的element，其他的可以从queue里移除
	                    for (Processor p : outProcessors) {
	                        elements = p.onElements(elements);
	                    }
	                    
	                    if (elements.length > 0) {
	                        LOG.debug(String.format("there are [%s] elements failed to process by all out processor.these elements will retain in queue for next process", elements.length));
	                    }
	
	                    //遍历queue,对于可以移除的event，移除（因为其他线程只会往queue里添加元素，不会移除元素，所以能
	                    //保证queue队列顶的元素是不变的）
	                    Set<Object> remainElements = new HashSet<Object>();
	                    Collections.addAll(remainElements, elements);
	                    it = elementQueue.iterator();
	                    for (int i = 0; i < currentBatchSize; i++) {
	                        if (!remainElements.contains(it.next())) {
	                        	//remainElements没有的数据，表示上传成功，需要从queue中删除
	                        	elementQueue.remove(); 
	                        }else{
	                        	//remainElements有的数据，表示上传失败，不需要从queue中删除，此时将count加一
	                        	elementCountInQueue.release();
	                        }
	                    }
	                }
                } catch (InterruptedException e) {
                    Thread.interrupted();
                    if (!isRunning) {
                        return;
                    }
                }/*catch(Exception e){
                	StackTraceElement[] errs = e.getStackTrace();
                	for(int i=0;i<errs.length;i++){
                		LOG.error("[ERROR]" +errs[i].toString());
                	}
                }*/
            }
        }

        public void updateConfig(Properties properties) {
            batchSize = Integer.parseInt(properties.getProperty("oci.auditor.com.ailk.oci.auditor.pipeline.config.out.batch.size", "64"));
            batchInterval = Long.parseLong(properties.getProperty("oci.auditor.com.ailk.oci.auditor.pipeline.config.out.batch.interval", "3000"));
        }

        public void close() {
            isRunning = false;
            Thread.interrupted();
        }
    }
}

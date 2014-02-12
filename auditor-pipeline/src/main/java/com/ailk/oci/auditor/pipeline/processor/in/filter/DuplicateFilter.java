package com.ailk.oci.auditor.pipeline.processor.in.filter;

import com.ailk.oci.auditor.pipeline.Pipeline;
import org.drools.KnowledgeBase;
import org.drools.KnowledgeBaseFactory;
import org.drools.builder.KnowledgeBuilder;
import org.drools.builder.KnowledgeBuilderFactory;
import org.drools.builder.ResourceType;
import org.drools.definition.KnowledgePackage;
import org.drools.event.rule.DebugAgendaEventListener;
import org.drools.event.rule.DebugWorkingMemoryEventListener;
import org.drools.io.impl.ByteArrayResource;
import org.drools.runtime.StatefulKnowledgeSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created with IntelliJ IDEA.
 * User: shaoaq
 * Date: 13-10-30
 * Time: 下午1:46
 * To change this template use File | Settings | File Templates.
 */
public class DuplicateFilter implements Pipeline.InProcessor {
    public static final String DEBUG_KEY = "com.ailk.oci.auditor.pipeline.processor.in.filter.duplicate.debug";
    public static final String RULE_KEY = "com.ailk.oci.auditor.pipeline.processor.in.filter.duplicate.rule";
    public static final String WINDOW_SIZE_KEY = "com.ailk.oci.auditor.pipeline.processor.in.filter.duplicate.window.size";
    public static final String ACTION_ACCEPT = "ACCEPT";
    public static final String ACTION_DISCARD = "DISCARD";

    private final static Logger LOG = LoggerFactory.getLogger(DuplicateFilter.class);
    private String rule = "";
    private KnowledgeBase knowledgeBase;
    private boolean debug = false;
    private ConcurrentMap<String, BoundedConcurrentSkipListSet> elementWindow = new ConcurrentHashMap<String, BoundedConcurrentSkipListSet>();
    private AtomicInteger currentWindowSize = new AtomicInteger(0);
    private int windowSize;

    @Override

    public Object[] onElements(Object... elements) {
        if ("".equals(rule) || elements.length == 0) {
            return elements;
        } else {
            final StatefulKnowledgeSession session = knowledgeBase
                    .newStatefulKnowledgeSession();
            session.setGlobal("filter", this);
            if (debug) {
                session.addEventListener(new DebugAgendaEventListener());
                session.addEventListener(new DebugWorkingMemoryEventListener());
            }
            List<CandidateElement> candidateElements = new ArrayList<CandidateElement>();
            for (Object element : elements) {
                CandidateElement candidateElement = new CandidateElement(element, ACTION_ACCEPT);
                candidateElements.add(candidateElement);
                session.insert(candidateElement);
            }
            session.fireAllRules();
            session.dispose();
            List<Object> remainElements = new ArrayList<Object>();
            for (Iterator<CandidateElement> iterator = candidateElements.iterator(); iterator.hasNext(); ) {
                CandidateElement candidateElement = iterator.next();
                if (ACTION_ACCEPT.equals(candidateElement.getAction())) {
                    remainElements.add(candidateElement.getElement());
                }
            }
            return remainElements.toArray();
        }
    }

    @Override
    public void updateConfig(Properties properties) {
        debug = Boolean.parseBoolean(properties.getProperty(DEBUG_KEY, "false"));
        String rule = properties.getProperty(RULE_KEY, "");
        if (!rule.equals(this.rule)) {
            if ("".equals(rule)) {
                LOG.debug(String.format("empty rule, all event will be passed. (%s)", RULE_KEY));
            } else {
                LOG.info(String.format("rule:[%s],(%s)", rule, RULE_KEY));
                final KnowledgeBuilder knowledgeBuilder = KnowledgeBuilderFactory.newKnowledgeBuilder();
                knowledgeBuilder.add(new ByteArrayResource(rule.getBytes()), ResourceType.DRL);
                final Collection<KnowledgePackage> knowledgePackages = knowledgeBuilder.getKnowledgePackages();

                knowledgeBase = KnowledgeBaseFactory.newKnowledgeBase();
                knowledgeBase.addKnowledgePackages(knowledgePackages);
            }
            this.rule = rule;
        }
        try {
            int windowSize = Integer.parseInt(properties.getProperty(WINDOW_SIZE_KEY, "1024"));
            if (this.windowSize != windowSize) {
                this.windowSize = windowSize;
                LOG.info(String.format("window size: [%d] ,(%s)", windowSize, WINDOW_SIZE_KEY));
            }
        } catch (NumberFormatException e) {
            LOG.warn(String.format("window size format error,expect integer,continue use old window size [%d], (%s)", this.windowSize, WINDOW_SIZE_KEY));
        }


    }

    @Override
    public void close() throws Exception {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public void duplicateCheck(Object c, Object timeObject, long liveTime, List<Object> keys) {

        if (!(c instanceof CandidateElement)) {
            LOG.warn("fisrt parameter of duplicateCheck must be type of CandidateElement!");
            return;
        }
        CandidateElement candidateElement = (CandidateElement) c;
        if (liveTime <= 0) { //如果存活时间为0，那所有的event都不会重复
            candidateElement.setAction(ACTION_ACCEPT);
        }
        long time = 0;
        if (timeObject instanceof Date) {
            time = Date.class.cast(timeObject).getTime();
        } else if (timeObject instanceof Long) {
            time = Long.class.cast(timeObject).longValue();
        } else {
            LOG.warn("seconde parameter of duplicateCheck must be type of Long or Date!");
            return;
        }
        long keyTime = time / liveTime * liveTime; //重复时间段标示
        StringBuffer sb = new StringBuffer();
        sb.append(keyTime);
        for (Object key : keys) {
            sb.append(",");
            sb.append(key);
        }
        BoundedConcurrentSkipListSet set = new BoundedConcurrentSkipListSet(liveTime);
        BoundedConcurrentSkipListSet result = elementWindow.putIfAbsent(sb.toString(), set);
        if (result != null) {
            set = result;
        } else {
            if (currentWindowSize.incrementAndGet() > windowSize) {
                for (Map.Entry<String, BoundedConcurrentSkipListSet> entry : elementWindow.entrySet()) {
                    if (System.currentTimeMillis() - entry.getValue().last() > entry.getValue().getLiveTime()) {
                        //如果这个Set里最大的时间标识都已经超时了，这个Set就没啥用了，可以删除
                        elementWindow.remove(entry.getKey());
                        currentWindowSize.decrementAndGet();
                    }
                }
                if (currentWindowSize.get() > windowSize) {
                    //如果没有可以删除的，则强制删除一个
                    String key = elementWindow.entrySet().iterator().next().getKey();
                    elementWindow.remove(key);
                    LOG.warn(String.format("elementWindow is full and all elements in window is validate, force remote one of them.[%s]", key));
                }
            }
        }
        if (set.add(keyTime)) { //没有改重复时间标示，则不重复
            candidateElement.setAction(ACTION_ACCEPT);
        } else {
            candidateElement.setAction(ACTION_DISCARD);
        }
    }

    public static class BoundedConcurrentSkipListSet extends ConcurrentSkipListSet<Long> {
        private long liveTime;

        public BoundedConcurrentSkipListSet(long liveTime) {
            this.liveTime = liveTime;
        }

        @Override
        public boolean add(Long e) {
            boolean result = super.add(e);
            if ((result) && (size() > 3)) {
                Long first = first();
                remove(first);
                if (first == e) {
                    return false;
                }
            }
            return result;
        }

        public long getLiveTime() {
            return liveTime;
        }
    }

    public static class CandidateElement {
        private final Object element;
        private String action;

        public CandidateElement(Object element, String action) {
            this.element = element;
            this.action = action;
        }

        public Object getElement() {
            return element;
        }

        public String getAction() {
            return action;
        }

        public void setAction(String action) {
            this.action = action;
        }
    }
}

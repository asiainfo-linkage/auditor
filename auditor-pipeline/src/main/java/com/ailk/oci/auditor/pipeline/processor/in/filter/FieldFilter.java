package com.ailk.oci.auditor.pipeline.processor.in.filter;

import com.ailk.oci.auditor.pipeline.Pipeline;
import org.apache.commons.lang.ArrayUtils;
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

/**
 * 根据字段过滤，判断接受哪些元素，丢弃哪些元素
 * User: shaoaq
 * Date: 13-10-30
 * Time: 下午1:44
 * To change this template use File | Settings | File Templates.
 */
public class FieldFilter implements Pipeline.InProcessor {
    public static final String DEFAULT_ACTION_KEY = "com.ailk.oci.auditor.pipeline.processor.in.filter.field.default.action";
    public static final String DEBUG_KEY = "com.ailk.oci.auditor.pipeline.processor.in.filter.field.debug";
    public static final String RULE_KEY = "com.ailk.oci.auditor.pipeline.processor.in.filter.field.rule";
    public static final String ACTION_ACCEPT = "ACCEPT";
    public static final String ACTION_DISCARD = "DISCARD";

    private static final Object[] EMPTY = new Object[0];
    private final static Logger LOG = LoggerFactory.getLogger(FieldFilter.class);

    private String rule = "";
    private boolean debug = false;
    private static String defaultAction = "";

    private KnowledgeBase knowledgeBase;

    @Override
    public Object[] onElements(Object... elements) {
        if ("".equals(rule) || elements.length == 0) {
            if (defaultAction.equals(ACTION_DISCARD)) {
                return ArrayUtils.EMPTY_OBJECT_ARRAY;
            }
            return elements;
        } else {
            final StatefulKnowledgeSession session = knowledgeBase
                    .newStatefulKnowledgeSession();
            if (debug) {
                session.addEventListener(new DebugAgendaEventListener());
                session.addEventListener(new DebugWorkingMemoryEventListener());
            }
            List<CandidateElement> candidateElements = new ArrayList<CandidateElement>();
            for (Object element : elements) {
                CandidateElement candidateElement = new CandidateElement(element, defaultAction);
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
        String action = properties.getProperty(DEFAULT_ACTION_KEY, "ACCEPT");
        debug = Boolean.parseBoolean(properties.getProperty(DEBUG_KEY, "false"));
        String rule = properties.getProperty(RULE_KEY, "");
        if (!rule.equals(this.rule)) {
            if ("".equals(rule)) {
                LOG.debug(String.format("empty rule, all element will be passed. (%s)", RULE_KEY));
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
        if (!defaultAction.equals(action)) {
            LOG.info(String.format("defaultAction:[%s],(%s), options:([%s,%s])", action, DEFAULT_ACTION_KEY, ACTION_ACCEPT, ACTION_DISCARD));
            defaultAction = action;
        }
    }

    @Override
    public void close() throws Exception {
        //To change body of implemented methods use File | Settings | File Templates.
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

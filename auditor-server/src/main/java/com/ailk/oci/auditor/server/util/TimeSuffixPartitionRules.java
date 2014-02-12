package com.ailk.oci.auditor.server.util;

import com.ailk.oci.auditor.server.domain.HBaseEvent;
import com.ailk.oci.auditor.server.domain.HdfsEvent;
import com.ailk.oci.auditor.server.domain.HiveEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Component;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.text.SimpleDateFormat;
import java.util.*;

import static java.lang.String.format;

/**
 * Created with IntelliJ IDEA.
 * User: shaoaq
 * Date: 13-11-7
 * Time: 下午2:26
 * To change this template use File | Settings | File Templates.
 */
@Component
public class TimeSuffixPartitionRules {

    private Rule[] rules = new Rule[]{
            new Rule(HdfsEvent.class, "localTime", "_yyyy_MM_dd", 24 * 60 * 60 * 1000),
            new Rule(HBaseEvent.class, "localTime", "_yyyy_MM_dd", 24 * 60 * 60 * 1000),
            new Rule(HiveEvent.class, "localTime", "_yyyy_MM_dd", 24 * 60 * 60 * 1000),
    };
    private Map<Class<?>, Rule> ruleMap = new HashMap<Class<?>, Rule>();

    public TimeSuffixPartitionRules() {
        for (Rule rule : rules) {
            ruleMap.put(rule.getEntityClass(), rule);
        }
    }

    public String getPartitionId(Object entity) {
        Rule rule = ruleMap.get(entity.getClass());
        if (rule == null) {
            return null;
        } else {
            return rule.getPartitionId(entity);
        }
    }

    public Rule[] getRules() {
        return rules;
    }

    public String getPartitionId(Class<?> entityClass, Object partitionPropertyValue) {
        Rule rule = ruleMap.get(entityClass);
        if (rule == null) {
            return null;
        } else {
            return rule.getPartitionId(entityClass, partitionPropertyValue);
        }
    }

    public String getPartitionTable(String partionId, String baseTable) {
        return baseTable + partionId;
    }

    public String[] getRecentExpiredPartitionIds(Class<?> entityClass) {
        Rule rule = ruleMap.get(entityClass);
        if (rule == null) {
            return null;
        } else {
            String[] prePartitions = new String[rule.getRecentExpired()];
            for (int i = 0; i < prePartitions.length; i++) {
                Date date = new Date(System.currentTimeMillis() - (i + 1 + rule.getKeep()) * rule.getPeriod());
                prePartitions[i] = rule.getPartitionId(entityClass, date);
            }
            return prePartitions;
        }
    }

    public String[] getAllKeptPartitionIds(Class<?> entityClass) {
        Rule rule = ruleMap.get(entityClass);
        if (rule == null) {
            return null;
        } else {
            String[] prePartitions = new String[rule.getPre() + rule.getKeep()];
            for (int i = 0; i < prePartitions.length; i++) {
                Date date = new Date(System.currentTimeMillis() + (i - rule.getKeep()) * rule.getPeriod());
                prePartitions[i] = rule.getPartitionId(entityClass, date);
            }
            return prePartitions;
        }
    }

    public String[] getPartitionIds(Class<?> entityClass, PartitioningJpaRepository.Param[] appendParams) {
        Rule rule = ruleMap.get(entityClass);
        if (rule == null) {
            return null;
        } else {
            return rule.getPartitionId(entityClass, appendParams);
        }
    }

    public static class Rule {
        private final static Logger LOG = LoggerFactory.getLogger(Rule.class);
        private final Class<?> entityClass;
        private final String partitionProperty;
        private final SimpleDateFormat partitionFormat;
        private final long period;
        private int pre = 2; //the count of partition to be pre created
        private int keep = 10; //the count of partition to be kept
        private int recentExpired = 4; // the count of partition to check if it is expired

        private Rule(Class entityClass, String partitionProperty, String partitionPattern, long period) {
            this.entityClass = entityClass;
            this.partitionProperty = partitionProperty;
            this.partitionFormat = new SimpleDateFormat(partitionPattern);
            this.period = period;
        }

        public Class<?> getEntityClass() {
            return entityClass;
        }

        public String getPartitionId(Object entity) {
            PropertyDescriptor propertyDescriptor = BeanUtils.getPropertyDescriptor(entityClass, partitionProperty);
            if (propertyDescriptor == null) {
                LOG.error(format("partition property [%s] not exists in entityClass:[%s]", partitionProperty, entityClass.getName()));
                return null;
            }
            Method readMethod = propertyDescriptor.getReadMethod();
            if (!Modifier.isPublic(readMethod.getDeclaringClass().getModifiers())) {
                readMethod.setAccessible(true);
            }
            Object value = null;
            try {
                value = readMethod.invoke(entity);
            } catch (Exception e) {
                LOG.error(format("can not read partition property [%s] from entityClass:[%s]", partitionProperty, entityClass.getName()));
                return null;
            }
            return getPartitionId(entityClass, value);
        }

        public String getPartitionId(Class<?> entityClass, Object partitionPropertyValue) {
            if (partitionPropertyValue == null) {
                LOG.error("partition property of TimeSuffixPartitionRules.Rule cannot be null.");
                return null;
            }
            if (!(partitionPropertyValue instanceof Date)) {
                LOG.error("TimeSuffixPartitionRules.Rule only support partition by property type of Date! but this type is: " + partitionPropertyValue.getClass().getName());
                return null;
            }
            return partitionFormat.format(partitionPropertyValue);
        }

        public String[] getPartitionId(Class<?> entityClass, PartitioningJpaRepository.Param[] appendParams) {
            Date end = null;
            Date start = null;
            for (PartitioningJpaRepository.Param param : appendParams) {
                if (param.getProperty().equals(partitionProperty)) {
                    if (param.getOperation().startsWith(">") && param.getValue() instanceof Date) {
                        start = (Date) param.getValue();
                    } else if (param.getOperation().startsWith("<") && param.getValue() instanceof Date) {
                        end = (Date) param.getValue();
                    }
                }
            }
            Date earliest = new Date(System.currentTimeMillis() - (keep - 1) * period);
            if (start == null || start.before(earliest)) {
                start = earliest;
            }
            Date latest = new Date();
            if (end == null || end.after(latest)) {
                end = latest;
            }
            List<String> partitions = new ArrayList<String>();
            while (start.before(end)) {
                partitions.add(partitionFormat.format(start));
                start = new Date(start.getTime() + period);
            }
            String endPartition = partitionFormat.format(start);
            if (partitions.size() == 0 || !partitions.get(partitions.size()-1).equals(endPartition)) {
                partitions.add(endPartition);
            }
            return partitions.toArray(new String[partitions.size()]);
        }

        private int getPre() {
            return pre;
        }

        private long getPeriod() {
            return period;
        }

        public int getKeep() {
            return keep;
        }

        public int getRecentExpired() {
            return recentExpired;
        }
    }
}

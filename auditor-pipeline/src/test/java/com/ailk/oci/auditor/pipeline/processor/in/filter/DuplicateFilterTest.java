package com.ailk.oci.auditor.pipeline.processor.in.filter;

import com.ailk.oci.auditor.protocol.event.pipeline.avro.Event;
import com.ailk.oci.auditor.protocol.event.pipeline.avro.HBaseEvent;
import com.ailk.oci.auditor.protocol.event.pipeline.avro.HdfsEvent;
import com.ailk.oci.auditor.protocol.event.pipeline.avro.HiveEvent;
import com.ailk.oci.auditor.pipeline.processor.in.filter.DuplicateFilter;

import org.junit.Assert;
import org.junit.Test;

import java.util.Properties;

/**
 * Created with IntelliJ IDEA.
 * User: shaoaq
 * Date: 13-10-31
 * Time: 下午2:58
 * To change this template use File | Settings | File Templates.
 */
public class DuplicateFilterTest {
    @Test
    public void testEmptyFilter() {
        DuplicateFilter duplicateFilter = new DuplicateFilter();
        Properties properties = new Properties();
        properties.setProperty("oci.auditor.pipeline.config.processor.duplicate.rule", "");
        duplicateFilter.updateConfig(properties);
        Event[] events = getEvents();
        Object[] remainEvents = duplicateFilter.onElements(events);
        Assert.assertEquals("empty filter ,none events will be filtered.", events.length, remainEvents.length);
    }

    @Test
    public void testHdfs_2s_Filter() { //hdfs相同用户对相同目录的相同操作在2s内认为是相同的事件
        DuplicateFilter duplicateFilter = new DuplicateFilter();
        Properties properties = new Properties();
// properties.setProperty(DuplicateFilter.DEBUG_KEY,"true");
        properties.setProperty(DuplicateFilter.RULE_KEY,
                "package com.ailk.oci.auditor.plugin.audit.pipeline.processor.in.filter.field \n"
                        + "import com.ailk.oci.auditor.protocol.event.pipeline.avro.*;\n"
                        + "import com.ailk.oci.auditor.pipeline.processor.in.filter.DuplicateFilter.CandidateElement;\n"
                        + "global com.ailk.oci.auditor.pipeline.processor.in.filter.DuplicateFilter filter \n"
                        + "rule \"Hdfs User filter\"\n"
                        + " dialect \"mvel\"\n"
                        + " when\n"
                        + " c: CandidateElement(element#Event.event instanceof HdfsEvent,user:element#Event.user,localTime:element#Event.localTime,operation:element#Event.operation,src:element#Event.event#HdfsEvent.src) \n"
                        + " then\n"
                        + " filter.duplicateCheck(c,localTime,2000,[user,operation,src])\n"
                        + "end\n");
        duplicateFilter.updateConfig(properties);
        Event[] events = getEvents();
        Object[] remainEvents = duplicateFilter.onElements(events);
        Assert.assertEquals("empty filter ,none events will be filtered.", 18, remainEvents.length);
        for (Object o : remainEvents) {
            System.out.println(o);
        }
    }

    @Test
    public void testHdfs_4s_Filter() { //hdfs相同用户对相同目录的相同操作在4s内认为是相同的事件
        DuplicateFilter duplicateFilter = new DuplicateFilter();
        Properties properties = new Properties();
// properties.setProperty(DuplicateFilter.DEBUG_KEY,"true");
        properties.setProperty(DuplicateFilter.RULE_KEY,
                "package com.ailk.oci.auditor.plugin.audit.pipeline.processor.in.filter.field \n"
                        + "import com.ailk.oci.auditor.protocol.event.pipeline.avro.*;\n"
                        + "import com.ailk.oci.auditor.pipeline.processor.in.filter.DuplicateFilter.CandidateElement;\n"
                        + "global com.ailk.oci.auditor.pipeline.processor.in.filter.DuplicateFilter filter \n"
                        + "rule \"Hdfs User filter\"\n"
                        + " dialect \"mvel\"\n"
                        + " when\n"
                        + " c: CandidateElement(element#Event.event instanceof HdfsEvent,user:element#Event.user,localTime:element#Event.localTime,operation:element#Event.operation,src:element#Event.event#HdfsEvent.src) \n"
                        + " then\n"
                        + " filter.duplicateCheck(c,localTime,4000,[user,operation,src])\n"
                        + "end\n");
        duplicateFilter.updateConfig(properties);
        Event[] events = getEvents();
        Object[] remainEvents = duplicateFilter.onElements(events);
        Assert.assertEquals("empty filter ,none events will be filtered.", 16, remainEvents.length);
        for (Object o : remainEvents) {
            System.out.println(o);
        }
    }

    private Event[] getEvents() {
        Event event1 = Event.newBuilder().setUser("user1").setLocalTime(1383205964358L).setOperation("ls").setAllowed(true).setEvent(HdfsEvent.newBuilder().setSrc("/tmp").build()).build();
        Event event1_1 = Event.newBuilder().setUser("user1").setLocalTime(1383205965358L).setOperation("ls").setAllowed(true).setEvent(HdfsEvent.newBuilder().setSrc("/tmp").build()).build();
        Event event1_2 = Event.newBuilder().setUser("user1").setLocalTime(1383205965358L).setOperation("du").setAllowed(true).setEvent(HdfsEvent.newBuilder().setSrc("/tmp").build()).build();
        Event event1_3 = Event.newBuilder().setUser("user1").setLocalTime(1383205966358L).setOperation("ls").setAllowed(true).setEvent(HdfsEvent.newBuilder().setSrc("/tmp").build()).build();
        Event event1_4 = Event.newBuilder().setUser("user1").setLocalTime(1383205967358L).setOperation("ls").setAllowed(true).setEvent(HdfsEvent.newBuilder().setSrc("/tmp").build()).build();
        Event event2 = Event.newBuilder().setUser("user1").setLocalTime(1383205964358L).setOperation("ls").setAllowed(true).setEvent(HdfsEvent.newBuilder().setSrc("/user").build()).build();
        Event event2_1 = Event.newBuilder().setUser("user1").setLocalTime(1383205965358L).setOperation("ls").setAllowed(true).setEvent(HdfsEvent.newBuilder().setSrc("/user").build()).build();
        Event event2_2 = Event.newBuilder().setUser("user1").setLocalTime(1383205966358L).setOperation("ls").setAllowed(true).setEvent(HdfsEvent.newBuilder().setSrc("/user").build()).build();
        Event event2_3 = Event.newBuilder().setUser("user1").setLocalTime(1383205967358L).setOperation("ls").setAllowed(true).setEvent(HdfsEvent.newBuilder().setSrc("/user").build()).build();
        Event event3 = Event.newBuilder().setUser("user2").setLocalTime(1383205964358L).setOperation("ls").setAllowed(true).setEvent(HdfsEvent.newBuilder().setSrc("/tmp").build()).build();
        Event event4 = Event.newBuilder().setUser("user2").setLocalTime(1383205964358L).setOperation("ls").setAllowed(true).setEvent(HdfsEvent.newBuilder().setSrc("/user").build()).build();
        Event event5 = Event.newBuilder().setUser("user1").setLocalTime(1383205964358L).setAllowed(true).setEvent(HBaseEvent.newBuilder().setTable("table1").build()).build();
        Event event5_1 = Event.newBuilder().setUser("user1").setLocalTime(1383205965358L).setAllowed(true).setEvent(HBaseEvent.newBuilder().setTable("table1").build()).build();
        Event event5_2 = Event.newBuilder().setUser("user1").setLocalTime(1383205966358L).setAllowed(true).setEvent(HBaseEvent.newBuilder().setTable("table1").build()).build();
        Event event5_3 = Event.newBuilder().setUser("user1").setLocalTime(1383205967358L).setAllowed(true).setEvent(HBaseEvent.newBuilder().setTable("table1").build()).build();
        Event event6 = Event.newBuilder().setUser("user1").setLocalTime(1383205964358L).setAllowed(true).setEvent(HBaseEvent.newBuilder().setTable("table2").build()).build();
        Event event7 = Event.newBuilder().setUser("user2").setLocalTime(1383205964358L).setAllowed(true).setEvent(HBaseEvent.newBuilder().setTable("table1").build()).build();
        Event event8 = Event.newBuilder().setUser("user2").setLocalTime(1383205964358L).setAllowed(true).setEvent(HBaseEvent.newBuilder().setTable("table2").build()).build();
        Event event9 = Event.newBuilder().setUser("user1").setLocalTime(1383205964358L).setAllowed(true).setEvent(HiveEvent.newBuilder().setTable("table1").build()).build();
        Event event10 = Event.newBuilder().setUser("user1").setLocalTime(1383205964358L).setAllowed(true).setEvent(HiveEvent.newBuilder().setTable("table2").build()).build();
        Event event11 = Event.newBuilder().setUser("user2").setLocalTime(1383205964358L).setAllowed(true).setEvent(HiveEvent.newBuilder().setTable("table1").build()).build();
        Event event12 = Event.newBuilder().setUser("user2").setLocalTime(1383205964358L).setAllowed(true).setEvent(HiveEvent.newBuilder().setTable("table2").build()).build();
        return new Event[]{event1, event1_1, event1_2, event1_3, event1_4, event2, event2_1, event2_2, event2_3, event3, event4, event5, event5_1, event5_2, event5_3, event6, event7, event8, event9, event10, event11, event12};
    }
} 
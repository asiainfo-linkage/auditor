package com.ailk.oci.auditor.pipeline.processor.in.filter;

import com.ailk.oci.auditor.protocol.event.pipeline.avro.Event;
import com.ailk.oci.auditor.protocol.event.pipeline.avro.HBaseEvent;
import com.ailk.oci.auditor.protocol.event.pipeline.avro.HdfsEvent;
import com.ailk.oci.auditor.protocol.event.pipeline.avro.HiveEvent;
import com.ailk.oci.auditor.pipeline.processor.in.filter.FieldFilter;

import java.io.IOException;
import java.util.Properties;

import org.junit.Assert;
import org.junit.Test;


/**
 * Created with IntelliJ IDEA.
 * User: shaoaq
 * Date: 13-10-31
 * Time: 下午2:58
 * To change this template use File | Settings | File Templates.
 */
public class FieldFilterTest {
	
	 @Test
	    public void testUser1Filter() throws IOException {//过滤所有user1的日志
	        FieldFilter field = new FieldFilter();
	        Properties properties = new Properties();
	        properties.setProperty(FieldFilter.DEFAULT_ACTION_KEY, FieldFilter.ACTION_ACCEPT);
	        properties.setProperty(FieldFilter.RULE_KEY,
	                "package com.ailk.oci.auditor.plugin.audit.pipeline.processor.in.filter.field \n"
	                        + "import com.ailk.oci.auditor.protocol.event.pipeline.avro.*;\n"
	                        + "import com.ailk.oci.auditor.pipeline.processor.in.filter.FieldFilter.CandidateElement;\n"
	                        + "rule \"Hdfs User filter\"\n"
	                        + " dialect \"mvel\"\n"
	                        + " when\n"
	                        + " c: CandidateElement( element#Event.user == \"user1\" )\n"
	                        + " then\n"
	                        + " c.setAction(\"DISCARD\")\n"
	                        + "end\n");
	        System.out.println(properties.getProperty(FieldFilter.RULE_KEY));
	 }
    @Test
    public void testEmptyFilter() {
        FieldFilter field = new FieldFilter();
        Properties properties = new Properties();
        properties.setProperty("oci.auditor.pipeline.config.processor.filter.rule", "");
        field.updateConfig(properties);
        Event[] events = getEvents();
        Object[] remainEvents = field.onElements(events);
        Assert.assertEquals("empty filter ,none events will be filtered.", events.length, remainEvents.length);
    }

//    @Test
//    public void testUser1Filter() throws IOException {//过滤所有user1的日志
//        FieldFilter field = new FieldFilter();
//        Properties properties = new Properties();
//        properties.setProperty(FieldFilter.DEFAULT_ACTION_KEY, FieldFilter.ACTION_ACCEPT);
//        properties.setProperty(FieldFilter.RULE_KEY,
//                "package com.ailk.oci.auditor.plugin.audit.pipeline.processor.in.filter.field \n"
//                        + "import com.ailk.oci.auditor.protocol.event.pipeline.avro.*;\n"
//                        + "import com.ailk.oci.auditor.pipeline.processor.in.filter.FieldFilter.CandidateElement;\n"
//                        + "rule \"Hdfs User filter\"\n"
//                        + " dialect \"mvel\"\n"
//                        + " when\n"
//                        + " c: CandidateElement( element#Event.user == \"user1\" )\n"
//                        + " then\n"
//                        + " c.setAction(\"DISCARD\")\n"
//                        + "end\n");
//        System.out.println(properties.getProperty(FieldFilter.RULE_KEY));
//        properties.store(System.out, "test");
//        field.updateConfig(properties);
//        Event[] events = getEvents();
//        Object[] remainEvents = field.onElements(events);
//        Assert.assertEquals("user filter ,event of user1 will be filtered.", 6, remainEvents.length);
//        for (Object event : remainEvents) {
//            Assert.assertNotEquals("user of event must not equal to user1", "user1", ((Event) event).getUser());
//        }
//    }

    @Test
    public void testKeepUser1Filter() throws IOException {//过滤所有非user1的日志
        FieldFilter field = new FieldFilter();
        Properties properties = new Properties();
        properties.setProperty(FieldFilter.DEFAULT_ACTION_KEY, FieldFilter.ACTION_DISCARD);
        properties.setProperty(FieldFilter.RULE_KEY,
                "package com.ailk.oci.auditor.plugin.audit.pipeline.processor.in.filter.field \n"
                        + "import com.ailk.oci.auditor.protocol.event.pipeline.avro.*;\n"
                        + "import com.ailk.oci.auditor.pipeline.processor.in.filter.FieldFilter.CandidateElement;\n"
                        + "rule \"Hdfs User filter\"\n"
                        + " dialect \"mvel\"\n"
                        + " when\n"
                        + " c: CandidateElement( element#Event.user == \"user1\" )\n"
                        + " then\n"
                        + " c.setAction(\"ACCEPT\")\n"
                        + "end\n");
        System.out.println(properties.getProperty(FieldFilter.RULE_KEY));
        properties.store(System.out, "test");
        field.updateConfig(properties);
        Event[] events = getEvents();
        Object[] remainEvents = field.onElements(events);
        Assert.assertEquals("user filter ,event of user1 will be filtered.", 6, remainEvents.length);
        for (Object event : remainEvents) {
            Assert.assertEquals("user of event must not equal to user1", "user1", ((Event) event).getUser());
        }
    }

    @Test
    public void testTmpDirectoryFilter() throws IOException {//过滤所有tmp目录的访问日志
        FieldFilter field = new FieldFilter();
        Properties properties = new Properties();
        properties.setProperty(FieldFilter.DEFAULT_ACTION_KEY, FieldFilter.ACTION_ACCEPT);
        properties.setProperty(FieldFilter.RULE_KEY,
                "package com.ailk.oci.auditor.plugin.audit.pipeline.processor.in.filter.field \n"
                        + "import com.ailk.oci.auditor.protocol.event.pipeline.avro.*;\n"
                        + "import com.ailk.oci.auditor.pipeline.processor.in.filter.FieldFilter.CandidateElement;\n"
                        + "rule \"Hdfs tmp directory filter\"\n"
                        + " dialect \"mvel\"\n"
                        + " when\n"
                        + " c: CandidateElement(element#Event.event#HdfsEvent.src == \"/tmp\") \n"
                        + " then\n"
                        + " c.setAction(\"DISCARD\")\n"
                        + "end\n");
        System.out.println(properties.getProperty(FieldFilter.RULE_KEY));
        properties.store(System.out, "test");
        field.updateConfig(properties);
        Event[] events = getEvents();
        Object[] remainEvents = field.onElements(events);
        Assert.assertEquals("tmp directory filter ,event of tmp directory will be filtered.", 10, remainEvents.length);
        for (Object event : remainEvents) {
            if (((Event) event).getEvent() instanceof HdfsEvent) {
                Assert.assertNotEquals("user of event must not equal to user1", "/tmp", ((HdfsEvent) ((Event) event).getEvent()).getSrc());
            }
        }
    }

    @Test
    public void testHBaseFilter() throws IOException {//过滤所有HBase日志
        FieldFilter field = new FieldFilter();
        Properties properties = new Properties();
        properties.setProperty(FieldFilter.DEFAULT_ACTION_KEY, FieldFilter.ACTION_ACCEPT);
        properties.setProperty(FieldFilter.RULE_KEY,
                "package com.ailk.oci.auditor.plugin.audit.pipeline.processor.in.filter.field \n"
                        + "import com.ailk.oci.auditor.protocol.event.pipeline.avro.*;\n"
                        + "import com.ailk.oci.auditor.pipeline.processor.in.filter.FieldFilter.CandidateElement;\n"
                        + "rule \"HBase Filter\"\n"
                        + " dialect \"mvel\"\n"
                        + " when\n"
                        + " c: CandidateElement(element#Event.event instanceof HBaseEvent ) \n"
                        + " then\n"
                        + " c.setAction(\"DISCARD\")\n"
                        + "end\n");
        System.out.println(properties.getProperty(FieldFilter.RULE_KEY));
        properties.store(System.out, "test");
        field.updateConfig(properties);
        Event[] events = getEvents();
        Object[] remainEvents = field.onElements(events);
        Assert.assertEquals("HBase event filter ,all HBaseEvent must be filtered.", 8, remainEvents.length);
        for (Object event : remainEvents) {
            Assert.assertFalse("all HBaseEvent must be filtered", ((Event) event).getEvent() instanceof HBaseEvent);
        }
    }

    private Event[] getEvents() {
        Event event1 = Event.newBuilder().setUser("user1").setLocalTime(1383205964358L).setAllowed(true).setEvent(HdfsEvent.newBuilder().setSrc("/tmp").build()).build();
        Event event2 = Event.newBuilder().setUser("user1").setLocalTime(1383205964358L).setAllowed(true).setEvent(HdfsEvent.newBuilder().setSrc("/user").build()).build();
        Event event3 = Event.newBuilder().setUser("user2").setLocalTime(1383205964358L).setAllowed(true).setEvent(HdfsEvent.newBuilder().setSrc("/tmp").build()).build();
        Event event4 = Event.newBuilder().setUser("user2").setLocalTime(1383205964358L).setAllowed(true).setEvent(HdfsEvent.newBuilder().setSrc("/user").build()).build();
        Event event5 = Event.newBuilder().setUser("user1").setLocalTime(1383205964358L).setAllowed(true).setEvent(HBaseEvent.newBuilder().setTable("table1").build()).build();
        Event event6 = Event.newBuilder().setUser("user1").setLocalTime(1383205964358L).setAllowed(true).setEvent(HBaseEvent.newBuilder().setTable("table2").build()).build();
        Event event7 = Event.newBuilder().setUser("user2").setLocalTime(1383205964358L).setAllowed(true).setEvent(HBaseEvent.newBuilder().setTable("table1").build()).build();
        Event event8 = Event.newBuilder().setUser("user2").setLocalTime(1383205964358L).setAllowed(true).setEvent(HBaseEvent.newBuilder().setTable("table2").build()).build();
        Event event9 = Event.newBuilder().setUser("user1").setLocalTime(1383205964358L).setAllowed(true).setEvent(HiveEvent.newBuilder().setTable("table1").build()).build();
        Event event10 = Event.newBuilder().setUser("user1").setLocalTime(1383205964358L).setAllowed(true).setEvent(HiveEvent.newBuilder().setTable("table2").build()).build();
        Event event11 = Event.newBuilder().setUser("user2").setLocalTime(1383205964358L).setAllowed(true).setEvent(HiveEvent.newBuilder().setTable("table1").build()).build();
        Event event12 = Event.newBuilder().setUser("user2").setLocalTime(1383205964358L).setAllowed(true).setEvent(HiveEvent.newBuilder().setTable("table2").build()).build();
        return new Event[]{event1, event2, event3, event4, event5, event6, event7, event8, event9, event10, event11, event12};
    }
}
package com.ailk.oci.auditor.server.in;

import com.ailk.oci.auditor.pipeline.Pipeline;
import com.ailk.oci.auditor.server.domain.Event;
import com.ailk.oci.auditor.server.domain.HBaseEvent;
import com.ailk.oci.auditor.server.domain.HdfsEvent;
import com.ailk.oci.auditor.server.domain.HiveEvent;
import com.ailk.oci.auditor.server.repository.HBaseEventRepository;
import com.ailk.oci.auditor.server.repository.HdfsEventRepository;
import com.ailk.oci.auditor.server.repository.HiveEventRepository;

import org.apache.commons.lang.ArrayUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import javax.annotation.Resource;

/**
 * Created with IntelliJ IDEA.
 * User: shaoaq
 * Date: 13-11-5
 * Time: 上午10:29
 * To change this template use File | Settings | File Templates.
 */
@Component
public class EventPersistenceProcessor implements Pipeline.OutProcessor {
    @Autowired
    private HdfsEventRepository hdfsEventRepository;
    
    @Autowired
    private HBaseEventRepository hBaseEventRepository;

    @Autowired
    private HiveEventRepository hiveEventRepository;

    @Override
    @Transactional
    public Object[] onElements(Object... elements) {
        List<Event> hdfsEvents = new ArrayList<Event>();
        List<Event> hBaseEvents = new ArrayList<Event>();
        List<Event> hiveEvents = new ArrayList<Event>();
        for (Object element : elements) {
            if (element instanceof com.ailk.oci.auditor.protocol.event.pipeline.avro.Event) {
                com.ailk.oci.auditor.protocol.event.pipeline.avro.Event superEvent = com.ailk.oci.auditor.protocol.event.pipeline.avro.Event.class.cast(element);
                Object event = superEvent.getEvent();
                if (event instanceof com.ailk.oci.auditor.protocol.event.pipeline.avro.HdfsEvent) {
                    add(superEvent, new HdfsEvent(), hdfsEvents);
                } else if (event instanceof com.ailk.oci.auditor.protocol.event.pipeline.avro.HBaseEvent) {
                    add(superEvent, new HBaseEvent(), hBaseEvents);
                } else if (event instanceof com.ailk.oci.auditor.protocol.event.pipeline.avro.HiveEvent) {
                    add(superEvent, new HiveEvent(), hiveEvents);
                }
            }

        }
        hdfsEventRepository.persist(hdfsEvents.toArray(new HdfsEvent[hdfsEvents.size()]));
        hBaseEventRepository.persist(hBaseEvents.toArray(new HBaseEvent[hBaseEvents.size()]));
        hiveEventRepository.persist(hiveEvents.toArray(new HiveEvent[hiveEvents.size()]));
        return ArrayUtils.EMPTY_OBJECT_ARRAY;
    }

    private void add(com.ailk.oci.auditor.protocol.event.pipeline.avro.Event superEvent, Event event, List<Event> events) {
        event.setAllowed(superEvent.getAllowed());
        event.setHostAddress(superEvent.getHostAddress());
        event.setHostName(superEvent.getHostName());
        event.setImpersonator(superEvent.getImpersonator());
        event.setLocalTime(new Date(superEvent.getLocalTime()));
        event.setOperation(superEvent.getOperation());
        event.setService(superEvent.getService());
        event.setUser(superEvent.getUser());
        Object avroEvent = superEvent.getEvent();
        if (event instanceof HdfsEvent) {
            HdfsEvent hdfsEvent = (HdfsEvent) event;
            com.ailk.oci.auditor.protocol.event.pipeline.avro.HdfsEvent avroHdfsEvent = (com.ailk.oci.auditor.protocol.event.pipeline.avro.HdfsEvent) avroEvent;
            hdfsEvent.setDst(avroHdfsEvent.getDst());
            hdfsEvent.setGroup(avroHdfsEvent.getGroup());
            hdfsEvent.setOwner(avroHdfsEvent.getOwner());
            hdfsEvent.setPermissions(avroHdfsEvent.getPermissions());
            hdfsEvent.setSrc(avroHdfsEvent.getSrc());
            events.add(hdfsEvent);
        } else if (event instanceof HBaseEvent) {
            HBaseEvent hBaseEvent = (HBaseEvent) event;
            com.ailk.oci.auditor.protocol.event.pipeline.avro.HBaseEvent avroHBaseEvent = (com.ailk.oci.auditor.protocol.event.pipeline.avro.HBaseEvent) avroEvent;
            hBaseEvent.setFamily(avroHBaseEvent.getFamily());
            hBaseEvent.setQualifier(avroHBaseEvent.getQualifier());
            hBaseEvent.setTable(avroHBaseEvent.getTable());
            events.add(hBaseEvent);
        } else if (event instanceof HiveEvent) {
            HiveEvent hiveEvent = (HiveEvent) event;
            com.ailk.oci.auditor.protocol.event.pipeline.avro.HiveEvent avroHiveEvent = (com.ailk.oci.auditor.protocol.event.pipeline.avro.HiveEvent) avroEvent;
            hiveEvent.setDatabase(avroHiveEvent.getDatabase());
            hiveEvent.setLocation(avroHiveEvent.getLocation());
            hiveEvent.setQuery(avroHiveEvent.getQuery());
            hiveEvent.setTable(avroHiveEvent.getTable());
            hiveEvent.setTargetType(avroHiveEvent.getTargetType());
            events.add(hiveEvent);
        }
    }

    @Override
    public void updateConfig(Properties properties) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void close() throws Exception {
        //To change body of implemented methods use File | Settings | File Templates.
    }
}

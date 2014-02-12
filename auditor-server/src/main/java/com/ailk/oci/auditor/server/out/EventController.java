package com.ailk.oci.auditor.server.out;

import com.ailk.oci.auditor.server.domain.HBaseEvent;
import com.ailk.oci.auditor.server.domain.HdfsEvent;
import com.ailk.oci.auditor.server.domain.HiveEvent;
import com.ailk.oci.auditor.server.repository.HBaseEventRepository;
import com.ailk.oci.auditor.server.repository.HdfsEventRepository;
import com.ailk.oci.auditor.server.repository.HiveEventRepository;
import com.ailk.oci.auditor.server.util.PartitioningJpaRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.stereotype.Controller;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: shaoaq
 * Date: 13-11-4
 * Time: 下午5:57
 * To change this template use File | Settings | File Templates.
 */
@Controller
@RequestMapping("/event")
public class EventController {
    @Autowired
    private HdfsEventRepository hdfsEventRepository;

    @Autowired
    private HBaseEventRepository hBaseEventRepository;

    @Autowired
    private HiveEventRepository hiveEventRepository;

    @RequestMapping(method = RequestMethod.GET, value = "/hdfs")
    @Transactional
    public
    @ResponseBody
    HdfsEvent[] hdfsEvents(HdfsEvent hdfsEvent,
                           @RequestParam("start")
                           @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss")
                           long start,
                           @RequestParam("end")
                           @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss")
                           long end,
                           @RequestParam(value = "page", required = false, defaultValue = "0")
                           int page,
                           @RequestParam(value = "size", required = false, defaultValue = "10")
                           int size,
                           @RequestParam(value = "orderBy", required = false, defaultValue = "localTime")
                           String[] orderBy,
                           @RequestParam(value = "direction", required = false, defaultValue = "desc")
                           String[] direction) {

        List<Sort.Order> orders = new ArrayList<Sort.Order>();
        for (int i = 0; i < orderBy.length; i++) {
            orders.add(new Sort.Order(Sort.Direction.fromString(direction[i]), orderBy[i]));
        }
        List<HdfsEvent> eventList = hdfsEventRepository.findAll(
                hdfsEvent,
                new PageRequest(page, size),
                new Sort(orders),
                new PartitioningJpaRepository.Param[]{
                        new PartitioningJpaRepository.Param("localTime", ">", new Date(start)),
                        new PartitioningJpaRepository.Param("localTime", "<", new Date(end))
                }
        );
        return eventList.toArray(new HdfsEvent[eventList.size()]);
    }

    @RequestMapping(method = RequestMethod.GET, value = "/hbase")
    @Transactional
    public
    @ResponseBody
    HBaseEvent[] hbaseEvents(HBaseEvent hBaseEvent,
                             @RequestParam("start")
                             @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss")
                             long start,
                             @RequestParam("end")
                             @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss")
                             long end,
                             @RequestParam(value = "page", required = false, defaultValue = "0")
                             int page,
                             @RequestParam(value = "size", required = false, defaultValue = "10")
                             int size,
                             @RequestParam(value = "orderBy", required = false, defaultValue = "localTime")
                             String[] orderBy,
                             @RequestParam(value = "direction", required = false, defaultValue = "desc")
                             String[] direction) {

        List<Sort.Order> orders = new ArrayList<Sort.Order>();
        for (int i = 0; i < orderBy.length; i++) {
            orders.add(new Sort.Order(Sort.Direction.fromString(direction[i]), orderBy[i]));
        }
        List<HBaseEvent> eventList = hBaseEventRepository.findAll(
                hBaseEvent,
                new PageRequest(page, size),
                new Sort(orders),
                new PartitioningJpaRepository.Param[]{
                        new PartitioningJpaRepository.Param("localTime", ">", new Date(start)),
                        new PartitioningJpaRepository.Param("localTime", "<", new Date(end))
                }
        );
        return eventList.toArray(new HBaseEvent[eventList.size()]);
    }

    @RequestMapping(method = RequestMethod.GET, value = "/hive")
    @Transactional
    public
    @ResponseBody
    HiveEvent[] hbaseEvents(HiveEvent hiveEvent,
                            @RequestParam("start")
                            @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss")
                            long start,
                            @RequestParam("end")
                            @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss")
                            long end,
                            @RequestParam(value = "page", required = false, defaultValue = "0")
                            int page,
                            @RequestParam(value = "size", required = false, defaultValue = "10")
                            int size,
                            @RequestParam(value = "orderBy", required = false, defaultValue = "localTime")
                            String[] orderBy,
                            @RequestParam(value = "direction", required = false, defaultValue = "desc")
                            String[] direction) {

        List<Sort.Order> orders = new ArrayList<Sort.Order>();
        for (int i = 0; i < orderBy.length; i++) {
            orders.add(new Sort.Order(Sort.Direction.fromString(direction[i]), orderBy[i]));
        }
        List<HiveEvent> eventList = hiveEventRepository.findAll(
                hiveEvent,
                new PageRequest(page, size),
                new Sort(orders),
                new PartitioningJpaRepository.Param[]{
                        new PartitioningJpaRepository.Param("localTime", ">", new Date(start)),
                        new PartitioningJpaRepository.Param("localTime", "<", new Date(end))
                }
        );
        return eventList.toArray(new HiveEvent[eventList.size()]);
    }
}

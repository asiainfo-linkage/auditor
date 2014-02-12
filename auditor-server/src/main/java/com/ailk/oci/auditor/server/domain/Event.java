package com.ailk.oci.auditor.server.domain;

import javax.persistence.*;
import java.util.Date;

/**
 * Created with IntelliJ IDEA.
 * User: shaoaq
 * Date: 13-11-6
 * Time: 下午5:25
 * To change this template use File | Settings | File Templates.
 */
@MappedSuperclass
public class Event {
    @Id
    @GeneratedValue(strategy = GenerationType.TABLE)
    @Column(name = "EVENT_ID")
    private Long id;
    @Column(name = "EVENT_SERVICE")
    private String service;
    @Column(name = "EVENT_USER")
    private String user;
    @Column(name = "EVENT_IMPERSONATOR")
    private String impersonator;
    @Column(name = "EVENT_LOCAL_TIME")
    private Date localTime;
    @Column(name = "EVENT_HOST_NAME")
    private String hostName;
    @Column(name = "EVENT_HOST_ADDRESS")
    private String hostAddress;
    @Column(name = "EVENT_OPERATION")
    private String operation;
    @Column(name = "EVENT_ALLOWED")
    private Boolean allowed;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getService() {
        return service;
    }

    public void setService(String service) {
        this.service = service;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getImpersonator() {
        return impersonator;
    }

    public void setImpersonator(String impersonator) {
        this.impersonator = impersonator;
    }

    public Date getLocalTime() {
        return localTime;
    }

    public void setLocalTime(Date localTime) {
        this.localTime = localTime;
    }

    public String getHostName() {
        return hostName;
    }

    public void setHostName(String hostName) {
        this.hostName = hostName;
    }

    public String getHostAddress() {
        return hostAddress;
    }

    public void setHostAddress(String hostAddress) {
        this.hostAddress = hostAddress;
    }

    public String getOperation() {
        return operation;
    }

    public void setOperation(String operation) {
        this.operation = operation;
    }

    public Boolean getAllowed() {
        return allowed;
    }

    public void setAllowed(Boolean allowed) {
        this.allowed = allowed;
    }
}

package com.ailk.oci.auditor.server;

import com.ailk.oci.auditor.protocol.event.pipeline.avro.EventPipelineProtocol;
import com.ailk.oci.auditor.server.in.EventPipelineProtocolServer;
import com.ailk.oci.auditor.server.util.AutoPartitioningProcessor;
import com.ailk.oci.auditor.server.util.PartitioningJpaRepositoryFactoryBean;
import org.apache.avro.ipc.ResponderServlet;
import org.apache.avro.ipc.specific.SpecificResponder;
import org.apache.commons.dbcp.BasicDataSource;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.context.embedded.ServletRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.orm.jpa.vendor.HibernateJpaVendorAdapter;
import org.springframework.scheduling.annotation.ScheduledAnnotationBeanPostProcessor;
import org.springframework.transaction.PlatformTransactionManager;

import javax.persistence.EntityManagerFactory;
import javax.servlet.Servlet;
import javax.sql.DataSource;
import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: shaoaq
 * Date: 13-11-4
 * Time: 下午5:40
 * To change this template use File | Settings | File Templates.
 */
@Configuration
@EnableAutoConfiguration
@ComponentScan
@EnableJpaRepositories(value = "com.ailk.oci.auditor.server.repository", repositoryFactoryBeanClass = PartitioningJpaRepositoryFactoryBean.class)
public class Main {


    @Bean
    public DataSource dataSource(
            @Value("${dataSource.driverClassName}") String driverClassName,
            @Value("${dataSource.url}") String url,
            @Value("${dataSource.user}") String user,
            @Value("${dataSource.password}") String password,
            @Value("${dataSource.maxActive}") int maxActive,
            @Value("${dataSource.maxIdle}") int maxIdle,
            @Value("${dataSource.minIdle}") int minIdle
    ) {
        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setDriverClassName(driverClassName);
        dataSource.setUrl(url);
        dataSource.setUsername(user);
        dataSource.setPassword(password);
        dataSource.setMaxActive(maxActive);
        dataSource.setMaxIdle(maxIdle);
        dataSource.setMinIdle(minIdle);
        return dataSource;
    }

    @Bean
    public EntityManagerFactory entityManagerFactory() {
        HibernateJpaVendorAdapter vendorAdapter = new HibernateJpaVendorAdapter();
        vendorAdapter.setGenerateDdl(true);
        LocalContainerEntityManagerFactoryBean factory = new LocalContainerEntityManagerFactoryBean() {
        };
        factory.setJpaVendorAdapter(vendorAdapter);
        factory.setPackagesToScan("com.ailk.oci.auditor");
        factory.setDataSource(dataSource(null, null, null, null, 0, 0, 0));


        factory.afterPropertiesSet();
        return factory.getObject();
    }

    @Bean
    public PlatformTransactionManager transactionManager() {
        JpaTransactionManager txManager = new JpaTransactionManager();
        txManager.setEntityManagerFactory(entityManagerFactory());
        return txManager;
    }

    @Bean
    public EventPipelineProtocolServer eventPipelineProtocolServer() {
        return new EventPipelineProtocolServer();
    }

    @Bean
    public ServletRegistrationBean servletRegistrationBean() throws IOException {
        SpecificResponder responder = new SpecificResponder(EventPipelineProtocol.class, eventPipelineProtocolServer());
        Servlet servlet = new ResponderServlet(responder);
        ServletRegistrationBean registration = new ServletRegistrationBean(servlet);
        registration.addUrlMappings("/server/*");
        return registration;
    }

    @Bean
    public ScheduledAnnotationBeanPostProcessor scheduledAnnotationBeanPostProcessor() {
        return new ScheduledAnnotationBeanPostProcessor();
    }

    @Bean
    public AutoPartitioningProcessor partitioningManager() {
        return new AutoPartitioningProcessor();
    }

    public static void main(String[] args) throws Exception {
        SpringApplication.run(Main.class, args);
    }

//    public static void main(String[] args) throws IOException {
//        HttpTransceiver client = new HttpTransceiver(new URL("http://localhost:8080/server"));
//        EventPipelineProtocol proxy = SpecificRequestor.getClient(EventPipelineProtocol.class, client);
//        Event event1 = Event.newBuilder().setUser("user1").setLocalTime(1383205964358L).setOperation("ls").setAllowed(true).setEvent(HdfsEvent.newBuilder().setSrc("/tmp").build()).build();
//        ArrayList<Event> events = new ArrayList<Event>();
//        events.add(event1);
//        WriteRequest request = WriteRequest.newBuilder().setEvents(events).build();
//        WriteResponse response = proxy.write(request);
//        System.out.println(response.getSuccess());
//    }
}

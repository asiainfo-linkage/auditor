package com.ailk.oci.auditor.server.util;

import org.apache.commons.lang.StringUtils;
import org.hibernate.Session;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.metadata.ClassMetadata;
import org.hibernate.persister.entity.SingleTableEntityPersister;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.jpa.repository.support.JpaEntityInformation;
import org.springframework.data.jpa.repository.support.SimpleJpaRepository;
import org.springframework.data.repository.NoRepositoryBean;

import javax.persistence.EntityManager;
import javax.persistence.Query;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static java.lang.String.format;

@SuppressWarnings("unchecked")
@NoRepositoryBean
public class PartitioningSimpleJpaRepository<T, ID extends Serializable>
        extends SimpleJpaRepository<T, ID> implements PartitioningJpaRepository<T, ID>, Serializable {
    private final static Logger LOG = LoggerFactory.getLogger(PartitioningSimpleJpaRepository.class);
    private TimeSuffixPartitionRules partitionRules = new TimeSuffixPartitionRules();
    private final EntityManager em;
    
    public PartitioningSimpleJpaRepository(JpaEntityInformation<T, ?> entityInformation, EntityManager entityManager) {
        super(entityInformation, entityManager);
        this.em = entityManager;
    }

    public PartitioningSimpleJpaRepository(Class<T> domainClass, EntityManager em) {
        super(domainClass, em);
        this.em = em;
    }

    @Override
    public <S extends T> void persist(S... entities) {
        if (entities.length > 0) {
            SessionImplementor session = em.unwrap(SessionImplementor.class);
            for (int i = 0; i < entities.length; i++) {
                S entity = entities[i];
                if (entity != null) {
                    persist(entity);
                }
                if (i % 20 == 0) {
                    session.flush();
                    if (session instanceof Session) {
                        Session.class.cast(session).clear();
                    }
                }
            }
            session.flush();
            if(session instanceof Session){
            	Session.class.cast(session).clear();
            }
        }
    }

    private <S extends T> void persist(S entity) {
        SessionImplementor session = em.unwrap(SessionImplementor.class);
        SingleTableEntityPersister persister = (SingleTableEntityPersister) session.getEntityPersister(null, entity);
        String baseTable = persister.getTableName();
        String partitionTableName = partitionRules.getPartitionTable(partitionRules.getPartitionId(entity), baseTable);
        List<String> columns = new ArrayList<String>();
        List<String> valueHolders = new ArrayList<String>();
        List<Object> values = new ArrayList<Object>();
        String keyColumn = persister.getKeyColumnNames()[0];
        Serializable keyValue = persister.getIdentifierGenerator().generate(session, entity);
        columns.add(keyColumn);
        valueHolders.add("?");
        values.add(keyValue);

        for (String name : persister.getPropertyNames()) {
            String column = persister.getPropertyColumnNames(name)[0];
            Object value = persister.getPropertyValue(entity, name);
            columns.add(column);
            valueHolders.add("?");
            values.add(value);
        }
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("INSERT INTO ");
        sqlBuilder.append(partitionTableName);
        sqlBuilder.append(" ");
        sqlBuilder.append("(");
        sqlBuilder.append(StringUtils.join(columns, ","));
        sqlBuilder.append(")");
        sqlBuilder.append(" ");
        sqlBuilder.append("VALUES");
        sqlBuilder.append(" ");
        sqlBuilder.append("(");
        sqlBuilder.append(StringUtils.join(valueHolders, ","));
        sqlBuilder.append(")");

        Query query = em.createNativeQuery(sqlBuilder.toString(), entity.getClass());
        int i = 1;
        for (Iterator<Object> iterator = values.iterator(); iterator.hasNext(); ) {
            Object value = iterator.next();
            query.setParameter(i++, value);
        }
        query.executeUpdate();
        persister.setIdentifier(entity, keyValue, session);
    }


    @Override
    public List<T> findAll(T entity, PageRequest pageRequest, Sort sort, Param[] appendParams) {
        //目前临时采用拼NativeSql的形式，后续可以考虑扩展Specifications实现
        List<String> conditions = new ArrayList<String>();
        List<Object> params = new ArrayList<Object>();
        SessionImplementor session = em.unwrap(SessionImplementor.class);
        SingleTableEntityPersister persister = (SingleTableEntityPersister) session.getEntityPersister(null, entity);
        StringBuilder sql = new StringBuilder("select * from %s where 1=1");
        for (String name : persister.getPropertyNames()) {
            String column = persister.getPropertyColumnNames(name)[0];
            Object value = persister.getPropertyValue(entity, name);
            if (value != null) {
                sql.append(" and ");
                sql.append(column);
                sql.append(" = ?");
                params.add(value);
            }
        }
        if (appendParams != null) {
            for (Param param : appendParams) {
                String column = persister.getPropertyColumnNames(param.getProperty())[0];
                Object value = param.getValue();
                sql.append(" and ");
                sql.append(column);
                sql.append(" ");
                sql.append(param.getOperation());
                sql.append("  ?");
                params.add(value);
            }
        }

        List<String> orderBys = new ArrayList<String>();
        if (sort != null) {
            for (Sort.Order order : sort) {
                orderBys.add(persister.getPropertyColumnNames(order.getProperty())[0] + " " + order.getDirection().name());

            }
        }


        Class<?> entityClass = entity.getClass();


        String[] partitionIds = partitionRules.getPartitionIds(entityClass, appendParams);
        List<String> subQuerys = new ArrayList<String>();
        for (String partitionId : partitionIds) {
            subQuerys.add(format(sql.toString(), partitionRules.getPartitionTable(partitionId, persister.getTableName())));
        }
        StringBuffer queryBuilder = new StringBuffer();
        queryBuilder.append(StringUtils.join(subQuerys, " union "));
        if (orderBys.size() > 0) {
            queryBuilder.append(" order by ");
            queryBuilder.append(StringUtils.join(orderBys, ","));
        }
        Query query = em.createNativeQuery(queryBuilder.toString(), entityClass);
        int i = 1;
        for (String partitionId : partitionIds) {
            for (Iterator<Object> iterator = params.iterator(); iterator.hasNext(); ) {
                query.setParameter(i++, iterator.next());
            }
        }
        query.setFirstResult(pageRequest.getOffset());
        query.setMaxResults(pageRequest.getPageSize());
        return query.getResultList();
    }

    @Override
    public <S extends T> void processPartitions() {
        for (TimeSuffixPartitionRules.Rule partitionRule : partitionRules.getRules()) {
            try {
                SessionImplementor session = em.unwrap(SessionImplementor.class);
                SessionFactoryImplementor factory = session.getFactory();
                ClassMetadata metadata = factory.getClassMetadata(partitionRule.getEntityClass());
                SingleTableEntityPersister persister = (SingleTableEntityPersister) metadata;
                String baseTable = persister.getTableName();
                //check partition to be created
                String[] partitionIds = partitionRules.getAllKeptPartitionIds(partitionRule.getEntityClass());
                for (String partitionId : partitionIds) {
                    String partitionTable = partitionRules.getPartitionTable(partitionId, baseTable);
                    Connection connection = session.connection();
                    if (connection != null) {
                        boolean exists = false;
                        ResultSet tables = connection.getMetaData().getTables(null, null, null, null);
                        while (tables.next()) {
                            String currentTableName = tables.getString("TABLE_NAME");
                            if (currentTableName.equalsIgnoreCase(partitionTable)) {
                                exists = true;
                            }
                        }
                        tables.close();
                        if (!exists) {
                            String sql = format("CREATE TABLE %s AS (SELECT * FROM %s WHERE 1=0)", partitionTable, baseTable);
                            if (factory.getDialect().toString().equals("org.hibernate.dialect.MySQLDialect")) {
                                sql = format("CREATE TABLE %s AS (SELECT * FROM %s WHERE 1=0)", partitionTable, baseTable);
                            } else if (factory.getDialect().toString().equals("org.hibernate.dialect.HSQLDialect")) {
                                sql = format("CREATE TABLE %s AS (SELECT * FROM %s WHERE 1=0) WITH NO DATA", partitionTable, baseTable);
                            }
                            em.createNativeQuery(sql).executeUpdate();
                        }
                    }
                }
                //check partition to be deleted
                String[] expiredPartitionIds = partitionRules.getRecentExpiredPartitionIds(partitionRule.getEntityClass());
                for (String partitionId : expiredPartitionIds) {
                    String partitionTable = partitionRules.getPartitionTable(partitionId, baseTable);
                    Connection connection = session.connection();
                    if (connection != null) {
                        boolean exists = false;
                        ResultSet tables = connection.getMetaData().getTables(null, null, partitionTable, null);
                        while (tables.next()) {
                            String currentTableName = tables.getString("TABLE_NAME");
                            if (currentTableName.equals(partitionTable)) {
                                exists = true;
                            }
                        }
                        tables.close();
                        if (exists) {
                            String sql = format("DROP TABLE %s", partitionTable);
                            em.createNativeQuery(sql).executeUpdate();
                        }
                    }
                }
            } catch (SQLException e) {
                LOG.warn("error process partition for " + partitionRule.getEntityClass(), e);
            }
        }

    }
}
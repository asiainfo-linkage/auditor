package com.ailk.oci.auditor.server.util;

import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.repository.NoRepositoryBean;

import java.io.Serializable;
import java.util.List;

@NoRepositoryBean
public interface PartitioningJpaRepository<T, ID extends Serializable>
        extends JpaRepository<T, ID> {
    <S extends T> void persist(S... entities);

    <S extends T> void processPartitions();

    List<T> findAll(T entity, PageRequest pageRequest, Sort sort,Param[] appendParams);
    public static class Param{
        private final String property;
        private final String operation;
        private final Object value;

        public Param(String property,String operation,Object value){

            this.property = property;
            this.operation = operation;
            this.value = value;
        }

        public String getProperty() {
            return property;
        }

        public String getOperation() {
            return operation;
        }

        public Object getValue() {
            return value;
        }
    }
}
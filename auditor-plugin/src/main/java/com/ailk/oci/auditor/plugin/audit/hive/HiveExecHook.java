package com.ailk.oci.auditor.plugin.audit.hive;

import com.ailk.oci.auditor.pipeline.Pipeline;
import com.ailk.oci.auditor.pipeline.processor.in.filter.DuplicateFilter;
import com.ailk.oci.auditor.pipeline.processor.in.filter.FieldFilter;
import com.ailk.oci.auditor.pipeline.processor.out.EventUploadProcessor;
import com.ailk.oci.auditor.plugin.audit.UserInfo;
import com.ailk.oci.auditor.plugin.audit.hive.util.TargetType;
import com.ailk.oci.auditor.protocol.event.pipeline.avro.Event;
import com.ailk.oci.auditor.protocol.event.pipeline.avro.HiveEvent;

import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.ql.exec.DDLTask;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.hooks.*;
import org.apache.hadoop.hive.ql.plan.DDLWork;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.util.ExitUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.HashSet;
import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: shaoaq
 * Date: 13-11-3
 * Time: 下午5:37
 * To change this template use File | Settings | File Templates.
 */
public class HiveExecHook implements ExecuteWithHookContext {
    private final static Logger LOG = LoggerFactory.getLogger(HiveExecHook.class);
    private Pipeline pipeline;

    @Override
    public void run(HookContext hookContext) throws Exception {
        if (pipeline == null) {
            pipeline = new Pipeline(new File(hookContext.getConf().get("oci.auditor.com.ailk.oci.auditor.pipeline.hive.exec.config", "pipeline.properties")));
            pipeline.setActions(Pipeline.ActionCode.QUEUE_FULL, new QueueFullAction());
            pipeline.setInProcessors(new FieldFilter(), new DuplicateFilter());
            pipeline.setOutProcessors(new EventUploadProcessor());
            pipeline.updateConfig4Processors();
        }
        Set<String> inputSet = input2pipeline(hookContext);
        output2pipeline(hookContext, inputSet);
        if (hookContext.getInputs().isEmpty() && hookContext.getOutputs().isEmpty()) {//处理DDLTask
            for (Task task : hookContext.getQueryPlan().getRootTasks()) {
                if (task instanceof DDLTask && (task.getWork() != null) && task.getWork() instanceof DDLWork) {
                    DDLWork work = (DDLWork) task.getWork();
                    String db = null;
                    String table = null;
                    TargetType targetType = null;
                    if (work.getCreateDatabaseDesc() != null) {
                        db = work.getCreateDatabaseDesc().getName();
                        targetType = TargetType.DATABASE;
                    } else if (work.getDropDatabaseDesc() != null) {
                        db = work.getAlterDatabaseDesc().getDatabaseName();
                        targetType = TargetType.DATABASE;
                    } else if (work.getAlterDatabaseDesc() != null) {
                        db = work.getAlterDatabaseDesc().getDatabaseName();
                        targetType = TargetType.DATABASE;
                    } else if (work.getDropIdxDesc() != null) {
                        table = work.getDropIdxDesc().getTableName();
                        targetType = TargetType.INDEX_TABLE;
                    } else if (work.getMsckDesc() != null) {
                        table = work.getMsckDesc().getTableName() != null ? work.getMsckDesc().getTableName() : "";
                        targetType = TargetType.TABLE;
                    }

                    String operationName = hookContext.getOperationName();
                    UserInfo userInfo = new UserInfo(hookContext.getUgi().toString());
//                    String userName = hookContext.getUserName();
//                    String[] address = parseIpAddress(hookContext.getIpAddress());
                    String userName = hookContext.getUgi().getUserName();//hookContext.getUserName();
                    String[] address = parseIpAddress(ShimLoader.getHadoopShims().getJobLauncherHttpAddress(hookContext.getConf()));//getJobLauncherRpcAddress
                    Event event = Event.newBuilder()
                            .setService(pipeline.getServiceName())
                            .setAllowed(true)
                            .setLocalTime(System.currentTimeMillis())
                            .setUser(userName)
                            .setImpersonator(userInfo.getImpersonator())
                            .setHostName(address[0])
                            .setHostAddress(address[1])
                            .setOperation(operationName)
                            .setEvent(HiveEvent.newBuilder()
                                    .setDatabase(db)
                                    .setTable(table)
                                    .setLocation(null)
                                    .setTargetType(targetType == null ? null : targetType.name())
                                    .setQuery(hookContext.getQueryPlan().getQueryString())
                                    .build()
                            )
                            .build();
                    this.pipeline.in(event);
                }
            }
        }
    }

    private void output2pipeline(HookContext hookContext, Set<String> inputSet) {
        for (WriteEntity output : hookContext.getOutputs()) {
            if (supportted(output.getType())) {
                if (!inputSet.contains(output.toString())) {//input已处理，不再重复
                    in(hookContext, output);
                }
            }
        }
    }

    private Set<String> input2pipeline(HookContext hookContext) {
        Set<String> inputSet = new HashSet<String>();
        for (ReadEntity input : hookContext.getInputs()) {
            inputSet.add(input.toString());
            in(hookContext, input);
        }
        return inputSet;
    }

    private void in(HookContext hookContext, Entity entity) {
        if (supportted(entity.getType())) {
            String operationName = hookContext.getOperationName();
            UserInfo userInfo = new UserInfo(hookContext.getUgi().toString());
            String userName = hookContext.getUgi().getUserName();//hookContext.getUserName();
//            String[] address = parseIpAddress(hookContext.getIpAddress());
            String[] address =  parseIpAddress(ShimLoader.getHadoopShims().getJobLauncherHttpAddress(hookContext.getConf()));
            String db = entity.getTable() != null ? entity.getTable().getDbName() : "";
            String table = entity.getTable() != null ? entity.getTable().getTableName() : "";
            String location = "";
            try {
                location = entity.getLocation() != null ? entity.getLocation().getPath() : "";
            } catch (Exception e) {
                LOG.warn("error get entity location", e);
            }
            TargetType targetType = getTargetType(entity);


            Event event = Event.newBuilder()
                    .setService(pipeline.getServiceName())
                    .setAllowed(true)
                    .setLocalTime(System.currentTimeMillis())
                    .setUser(userName)
                    .setImpersonator(userInfo.getImpersonator())
                    .setHostName(address[0])
                    .setHostAddress(address[1])
                    .setOperation(operationName)
                    .setEvent(HiveEvent.newBuilder()
                            .setDatabase(db)
                            .setTable(table)
                            .setLocation(location)
                            .setTargetType(targetType == null ? null : targetType.name())
                            .setQuery(hookContext.getQueryPlan().getQueryString())
                            .build()
                    )
                    .build();
            this.pipeline.in(event);
        }
    }

    private TargetType getTargetType(Entity entity) {
        TargetType targetType = null;
        switch (entity.getType()) {
            case TABLE:
                targetType = getTargetType(entity.getTable().getTableType());
                break;
            case PARTITION:
                targetType = TargetType.PARTITION;
                break;
            case DUMMYPARTITION:
                targetType = TargetType.DUMMYPARTITION;
                break;
            case DFS_DIR:
                targetType = TargetType.DFS_DIR;
                break;
            case LOCAL_DIR:
                targetType = TargetType.LOCAL_DIR;
                break;
            default:
                targetType = TargetType.UDF;
                break;
        }
        return targetType;
    }

    public static TargetType getTargetType(TableType tableType) {
        TargetType targetType = null;
        switch (tableType) {
            case MANAGED_TABLE:
                targetType = TargetType.MANAGED_TABLE;
                break;
            case EXTERNAL_TABLE:
                targetType = TargetType.EXTERNAL_TABLE;
                break;
            case VIRTUAL_VIEW:
                targetType = TargetType.VIRTUAL_VIEW;
                break;
            case INDEX_TABLE:
                targetType = TargetType.INDEX_TABLE;
                break;
        }
        return targetType;
    }

    private boolean supportted(Entity.Type type) {
        return (type == Entity.Type.TABLE) || (type == Entity.Type.PARTITION) || (type == Entity.Type.DFS_DIR) || (type == Entity.Type.LOCAL_DIR);
    }

    private static class QueueFullAction implements Pipeline.Action {
        @Override
        public void action(Pipeline pipeline, Pipeline.ActionCode code) {
            if (pipeline.getQueueFullPolicy().equals(Pipeline.QUEUE_FULL_POLICY_DROP)) {
                LOG.warn("Hive audit logger [" + code.name() + "], dropped");
            } else if (pipeline.getQueueFullPolicy().equals(Pipeline.QUEUE_FULL_POLICY_SHUTDOWN)) {
                try {
                    pipeline.close();
                } catch (Exception e) {
                    LOG.warn("failed to close com.ailk.oci.auditor.pipeline", e);
                }
                ExitUtil.terminate(2, "Hive audit logger [" + code.name() + "], terminating ...");
            }
        }
    }

    protected String[] parseIpAddress(String raw) {
        if (raw != null) {
            int sep = raw.indexOf('/');
            if ((sep >= 0) && (sep < raw.length() - 1)) {
                return new String[]{raw.substring(0, sep), raw.substring(sep + 1)};
            }
        }
        return new String[2];
    }
}

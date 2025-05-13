package org.apache.flink.table.planner.plan.nodes.physical.stream.conv;

import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.enumerate.FileEnumerator;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;
import org.apache.flink.flaze.plan.protobuf.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import static org.apache.flink.table.planner.plan.utils.CommonFlazeTools.convertDataTypeToArrowType;

public class StreamPhysicalTableSourceScanConv {
    private FileScanExecConf baseConf = null;
    private KafkaScanExecConf kafkaBaseConf = null;
    private String resourceId = "";
    
    public StreamPhysicalTableSourceScanConv(TableSourceTable tableSourceTable, String resourceId) throws IOException {
        ArrayList<Integer> projection = new ArrayList<>();
        ArrayList<String> projectFieldNames = new ArrayList<>();
        tableSourceTable.getRowType().getFieldList().forEach(
                field -> projectFieldNames.add(field.getName())
        );
        ArrayList<Field> fields = new ArrayList<>();
        int idx = 0;
        for (Column column : tableSourceTable.contextResolvedTable().getResolvedSchema().getColumns()) {
            fields.add(Field.newBuilder()
                    .setName(column.getName())
                    .setArrowType(convertDataTypeToArrowType(column.getDataType().toString()))
                    .setNullable(column.getDataType().getLogicalType().isNullable())
                    .build());
            if (projectFieldNames.contains(column.getName()))
                projection.add(idx);
            idx++;
        }
        Schema schema = Schema.newBuilder()
                .addAllColumns(fields)
                .build();
        this.kafkaBaseConf = KafkaScanExecConf.newBuilder()
                .setSchema(schema)
                .addAllProjection(projection)
                .build();
        this.resourceId = resourceId;
    }

    public PhysicalPlanNode convertToParquetScanExecNode() {
        ParquetScanExecNode scan = ParquetScanExecNode.newBuilder()
                .setBaseConf(baseConf)
                .setFsResourceId(resourceId)
                .build();
        return PhysicalPlanNode.newBuilder()
                .setParquetScan(scan)
                .build();
    }

    public PhysicalPlanNode convertToKafkaScanExecNode() {
        KafkaScanExecNode scan = KafkaScanExecNode.newBuilder()
                .setBaseConf(kafkaBaseConf)
                .setKafkaResourceId(resourceId)
                .build();
        return PhysicalPlanNode.newBuilder()
                .setKafkaScan(scan)
                .build();
    }
}

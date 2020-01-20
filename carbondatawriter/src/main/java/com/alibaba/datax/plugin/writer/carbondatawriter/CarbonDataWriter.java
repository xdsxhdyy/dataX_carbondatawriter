package com.alibaba.datax.plugin.writer.carbondatawriter;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.fastjson.JSONObject;
import org.apache.carbondata.common.exceptions.sql.InvalidLoadOptionException;
import org.apache.carbondata.sdk.file.CarbonWriter;
import org.apache.carbondata.sdk.file.CarbonWriterBuilder;
import org.apache.carbondata.sdk.file.Field;
import org.apache.carbondata.sdk.file.Schema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.alibaba.datax.plugin.writer.carbondatawriter.CarbonWriterErrorCode.*;

/**
 * @Author luoyu
 * @create 2020/1/14 11:00
 */
public class CarbonDataWriter extends Writer {
    public static final class Job extends Writer.Job{

        private Configuration originalConfig;

        /**
         * init：Job对象初始化工作，测试可以通过super.getPluginJobConf()获取与本插件相关的配置
         */
        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();

            String path = originalConfig.getString(Key.PATH);
            String cloumn = originalConfig.getString(Key.COLUMN);

            if (null == path){
                throw DataXException.asDataXException(PATH_ERROR,
                        "没有设置参数[path].");
            }
            if (null == cloumn){
                throw DataXException.asDataXException(CLOUMN_ERROR,
                        "没有设置参数[cloumn].");
            }
        }

        /**
         * 工具框架建议的拆分数adviceNumber，拆分Task
         * 返回Task的配置列表
         */
        @Override
        public List<Configuration> split(int adviceNumber) {
            List<Configuration> list = new ArrayList<Configuration>();
            for (int i = 0; i < adviceNumber; i++){
                list.add(originalConfig.clone());
            }
            return list;
        }

        /**
         * destroy：Job对象自身的销毁工作
         */
        @Override
        public void destroy() {

        }
    }

    public static final class Task extends Writer.Task{

        private Configuration sliceConfig;
        private String path;
        private List<JSONObject> columnsList = new ArrayList<JSONObject>();
        private String split;
        private CarbonWriter writer;

        /**
         * Task对象的初始化。此时可以通过super.getPluginJobConf()获取与本Task相关的配置。
         * 这里的配置是Job的split方法返回的配置列表中的其中一个。
         */
        @Override
        public void init() {
            this.sliceConfig = super.getPluginJobConf();
            path = sliceConfig.getString(Key.PATH);
            columnsList = (List<JSONObject>) sliceConfig.get(Key.COLUMN, List.class);
            split = sliceConfig.getString(Key.SPLIT);
        }

        /**
         * 从RecordReceiver中读取数据，写入目标数据源。
         * RecordReceiver中的数据来自Reader和Writer之间的缓存队列。
         */
        @Override
        public void startWrite(RecordReceiver lineReceiver) {
            String writePath = path.replace("\\", "/");
            int clounNum = columnsList.size();
            Field[] fields = new Field[clounNum];
            for (int i = 0; i < clounNum; i++){
                fields[i] = new Field(columnsList.get(i).getString("name"), columnsList.get(i).getString("type"));
            }
            Schema schema = new Schema(fields);
            try {
                CarbonWriterBuilder builder = CarbonWriter
                        .builder()
                        .outputPath(writePath)
                        .uniqueIdentifier(System.currentTimeMillis())
                        .withBlockSize(2)
                        .withCsvInput(schema);
                writer = builder.build();

                Record record = null;
                while ((record = lineReceiver.getFromReader()) != null){
                    if (record.getColumnNumber() != clounNum){
                        throw DataXException.asDataXException(ILLEGAL_VALUES_ERROR, ILLEGAL_VALUES_ERROR.getDescription() + "读出字段个数:" + record.getColumnNumber() + " " + "配置字段个数:" + columnsList.size());
                    }
                    String[] arr = new String[]{};
                    for (int i = 0; i < record.getColumnNumber(); i++){
                       arr[i] = record.getColumn(i).toString();
                    }
                    writer.write(arr);
                }


            } catch (InvalidLoadOptionException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }


        }

        /**
         * destroy：Task对象自身的销毁工作
         */
        @Override
        public void destroy() {
            if (writer != null){
                try {
                    writer.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}

package com.wzs.mapreduce.reducejoin;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

/**
 * @author FateSealed
 * @date 2023/03/31 11:04
 **/
public class TableReducer extends Reducer<Text, TableBean, TableBean, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Reducer<Text, TableBean, TableBean, NullWritable>.Context context) throws IOException, InterruptedException {
        final ArrayList<TableBean> tableBeans = new ArrayList<>();
        final TableBean tableBean = new TableBean();
        for (TableBean value : values) {
            if ("order".equals(value.getFlag())) {
                final TableBean t = new TableBean();
                try {
                    BeanUtils.copyProperties(t, value);
                    tableBeans.add(t);
                } catch (IllegalAccessException | InvocationTargetException e) {
                    throw new RuntimeException(e);
                }
            }else{
                try {
                    BeanUtils.copyProperties(tableBean,value);
                } catch (IllegalAccessException | InvocationTargetException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        for (TableBean bean : tableBeans) {
            bean.setPname(tableBean.getPname());
            context.write(bean,NullWritable.get());
        }
    }
}
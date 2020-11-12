package com.atguigu.app;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.atguigu.constant.GmallConstants;
import com.atguigu.utils.MyKafkaSender;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.kafka.common.internals.Topic;

import java.net.InetSocketAddress;
import java.util.List;

/**
 * @author yymstart
 * @create 2020-11-06 18:47
 */
public class CanalClient {
    public static void main(String[] args) throws InvalidProtocolBufferException {
        //1.获取连接
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress("hadoop102", 11111),
                "example",
                "",
                "");
        //2.抓取数据并解析
        while (true){
            //连接
            canalConnector.connect();
            //指定消费的数据表
            canalConnector.subscribe("gmall2020.*");
            //抓取数据
            Message message = canalConnector.get(100);
            //判空
            if(message.getEntries().size()<=0){
                System.out.println("没有数据休息一会");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }else {
                //获取entries集合
                List<CanalEntry.Entry> entries = message.getEntries();
                //遍历entries集合
                for (CanalEntry.Entry entry : entries) {
                    //获取entries类型
                    CanalEntry.EntryType entryType = entry.getEntryType();
                    //判断,只取rowdata
                    if(CanalEntry.EntryType.ROWDATA.equals(entryType)){
                        //获取表明
                        String tableName = entry.getHeader().getTableName();
                        //获取序列化数据
                        ByteString storeValue = entry.getStoreValue();
                        //反序列化
                        CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(storeValue);
                        //获取事件类型
                        CanalEntry.EventType eventType = rowChange.getEventType();
                        //获取数据集合
                        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
                        //根据表明以及时间类型处理数据
                        handler(tableName,eventType,rowDatasList);
                    }
                }
            }
        }

    }

    private static void handler(String tableName, CanalEntry.EventType eventType, List<CanalEntry.RowData> rowDatasList) {
        //对于订单而言,只需要新增数据
        if("order_info".equals(tableName)&&CanalEntry.EventType.INSERT.equals(eventType)){
            sendToKafka(rowDatasList,GmallConstants.GMALL_ORDER_INFO);
        }else if ("order_detail".equals(tableName)&&CanalEntry.EventType.INSERT.equals(eventType)){
            sendToKafka(rowDatasList,GmallConstants.GMALL_ORDER_DETAIL);

        }else if (("user_info").equals(tableName)&&CanalEntry.EventType.INSERT.equals(eventType)){
            sendToKafka(rowDatasList,GmallConstants.GMALL_USER_INFO);
        }
    }

    private static void sendToKafka(List<CanalEntry.RowData> rowDatasList,String topic) {
        for (CanalEntry.RowData rowData : rowDatasList) {
            //创建json对象,用于存放多个列的数据
            JSONObject jsonObject = new JSONObject();

            for (CanalEntry.Column column : rowData.getAfterColumnsList()) {
                jsonObject.put(column.getName(), column.getValue());
            }
            System.out.println(jsonObject.toString());
            //发送数据至Kafka
//            try {
//                Thread.sleep(new Random().nextInt(5) * 1000);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
            //发送数据至kafka
            MyKafkaSender.send(topic, jsonObject.toString());

        }
    }
}

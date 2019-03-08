package ONLINE.ProtocolBuffer;

import com.google.protobuf.InvalidProtocolBufferException;
import shapeless.HMap;

import java.util.HashMap;
import java.util.Map;

import static ONLINE.ProtocolBuffer.map_uid_counts_Protobuf.*;
import static javax.swing.UIManager.put;
import static org.bouncycastle.asn1.x500.style.RFC4519Style.o;

/**
 * Created by cycy on 2019/3/5.
 */
public class ProtocolBuffer_OP {
    public static byte[] Serialzed_Map_UidCounts(Map<Integer,Long> map){
        //  序列化
        // 创建map_uid_counts的Builder
        map_uid_counts.Builder muc_builder = map_uid_counts.newBuilder();
        // 设置map_uid_counts属性
        muc_builder.putAllUidCounts(map);
        // 创建map_uid_counts
        map_uid_counts instance = muc_builder.build();
        // 序列化，byte[]可以被写到磁盘文件，或者通过网络发送出去。
        byte[] data = instance.toByteArray();
        return data;
    }
    public static Map<Integer,Long> Deserialized_Map_UidCounts(byte[] data){
        try {
            map_uid_counts_Protobuf.map_uid_counts muc= map_uid_counts_Protobuf.map_uid_counts.parseFrom(data);
            System.out.println(muc.getUidCountsMap());
            return muc.getUidCountsMap();
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
            return null;
        }
    }
    public static void main(String[] args) {

        HashMap<Integer,Long> map=new HashMap<>();
        //  序列化
        // 创建map_uid_counts的Builder
        map_uid_counts.Builder muc_builder = map_uid_counts.newBuilder();
        // 设置map_uid_counts属性
        muc_builder.putAllUidCounts(map);
        // 创建map_uid_counts
        map_uid_counts instance = muc_builder.build();
        // 序列化，byte[]可以被写到磁盘文件，或者通过网络发送出去。
        byte[] data = instance.toByteArray();
        System.out.println("serialization end.");


        // 反序列化，byte[]可以读文件或者读取网络数据构建。
        System.out.println("deserialization begin.");
        try {
            map_uid_counts_Protobuf.map_uid_counts muc= map_uid_counts_Protobuf.map_uid_counts.parseFrom(data);
            System.out.println(muc.getUidCountsMap());
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }

    }

    public static void UpdateCounts(Map<Integer,Long> originalmap,Integer key,Long addvalue){
        Long originvalue=originalmap.getOrDefault(key, null);
        if ( originvalue == null) originalmap.put(key, addvalue);
        else originalmap.put(key,originvalue+addvalue);
    }

    public static Integer[] getmapKeys(Map<Integer,Long> map){
        return map.keySet().toArray(new Integer[0]);
    }
}

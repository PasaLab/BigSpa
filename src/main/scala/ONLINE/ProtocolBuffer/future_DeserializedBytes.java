package ONLINE.ProtocolBuffer;

import java.util.Map;
import java.util.concurrent.Callable;

import static ONLINE.ProtocolBuffer.ProtocolBuffer_OP.Deserialized_Map_UidCounts;

/**
 * Created by cycy on 2019/3/31.
 */
public class future_DeserializedBytes implements Callable<Map<Integer,Long>> {
    private byte[] v;

    public future_DeserializedBytes(byte[] _v){
        this.v = _v;
    }

    /**
     * 任务的具体过程，一旦任务传给ExecutorService的submit方法，
     * 则该方法自动在一个线程上执行
     */
    public Map<Integer,Long> call() throws Exception {
//        System.out.println("call()方法被自动调用！！！    " + Thread.currentThread().getName());
        //该返回结果将被Future的get方法得到
//        return "call()方法被自动调用，任务返回的结果是：" + id + "    " + Thread.currentThread().getName();
        return Deserialized_Map_UidCounts(this.v);
    }
}

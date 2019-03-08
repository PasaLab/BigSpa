import ONLINE.utils_ONLINE.Node_Info;

import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import org.json4s.ParserUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;

import static java.lang.System.out;
import static org.apache.hadoop.hbase.Version.user;
import static org.apache.hadoop.hbase.filter.ParseConstants.R;
import static sun.misc.Version.println;
/**
 * Created by cycy on 2018/3/20.
 */
public class test_java {

    public static void main(String[] args) throws IOException {

        int len=(int)1e4;

        int[] array=new int[len];
        HashMap<Integer,Integer> hashMap=new HashMap<Integer, Integer>();
//
        double t0=System.currentTimeMillis();
        for(int i=0;i<len;i++)
            array[i]=i;
        double t1=System.currentTimeMillis();
        out.println("array store use time "+(t1-t0));
        t0=System.currentTimeMillis();
        for(int i=0;i<len;i++)
            hashMap.put(i,1);
        t1=System.currentTimeMillis();
        out.println("hashMap store use time "+(t1-t0));
//
        t0=System.currentTimeMillis();
        ArrayList<int[]> arrayList=new ArrayList<>(hashMap.size());

        int random=new Random().nextInt(len);
        out.println("random: "+random);
        t0=System.nanoTime();
        for(int i=0;i<len;i++)
            if(array[i]==random) break;
        t1=System.nanoTime();
        out.println("array serach use time "+(t1-t0));
//
        t0=System.nanoTime();
        int value=hashMap.get(random);
        t1=System.nanoTime();
        out.println("hashMap serach use time "+(t1-t0));


    }
}

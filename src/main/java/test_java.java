import ONLINE.utils_ONLINE.Node_Info;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import org.json4s.ParserUtil;

import java.io.IOException;
import java.util.*;

import static java.lang.System.out;
import static org.apache.hadoop.hbase.Version.user;
import static org.apache.hadoop.hbase.filter.ParseConstants.R;
import static org.bouncycastle.asn1.x500.style.RFC4519Style.o;
import static sun.misc.Version.println;
/**
 * Created by cycy on 2018/3/20.
 */
public class test_java {

    public static void main(String[] args) throws IOException {

        Scanner scan=new Scanner(System.in);


//        int len=(int)1e8;
//
//        double t0=System.nanoTime();
//        IntOpenHashSet set=new IntOpenHashSet(len);
//        for(int i=0;i<len;i++)
//            set.add(i);
//        double t1=System.nanoTime();
//        out.println("set a  "+len+" int set uses "+(t1-t0)/1e9);
//        Random rand=new Random();
//        int randomint=rand.nextInt(len);
//        out.println("random int : "+randomint);
//        t0=System.nanoTime();
//        boolean isexist=set.contains(randomint);
//        out.println(isexist);
//        t1=System.nanoTime();
//        out.println("find a int uses "+(t1-t0)/1e9);
//
//        scan.next();

    }
}

import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;

import static java.lang.System.out;

/**
 * Created by cycy on 2018/3/20.
 */
public class test_java {

    public static void main(String[] args){
        LongArrayList a=new LongArrayList();
        a.add(1);
        a.add(1);
        a.add(2);
        a.add(0);
        LongOpenHashSet set=new LongOpenHashSet(a);
        long[] b=new long[set.size()];
        set.toArray(b);

        for(int i=0;i<b.length;i++) out.print(b[i]+" ");
    }
}

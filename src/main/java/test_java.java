import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;

import java.util.ArrayList;
import java.util.HashSet;

import static java.lang.System.out;
import static sun.misc.Version.println;

/**
 * Created by cycy on 2018/3/20.
 */
public class test_java {

    public static void main(String[] args) {

        HashSet<Long> set1 = new HashSet<Long>();
        ArrayList<Long> set2 = new ArrayList<Long>();
        set1.add(1L);
        set1.add(2L);
        set2.add(2L);
        set2.add(3L);

        set1.removeAll(set2);
        set2.addAll(set1);
        out.println(set1);
        out.println(set2);

        LongArrayList a = new LongArrayList();
        a.add(1);
        a.add(1);
        a.add(2);
        a.add(0);
        LongOpenHashSet set = new LongOpenHashSet(a);
        long[] b = new long[set.size()];
        set.toArray(b);

        for (int i = 0; i < b.length; i++) out.print(b[i] + " ");
    }
}

import java.util.ArrayList;
import java.util.List;

/**
 * Created by cycy on 2018/3/20.
 */
public class test_java {

    public static void main(String[] args){
        List<int[]> a=new ArrayList<int[]>();
        int len=100;
        double t0=System.nanoTime();
        for(int i=0;i<len;i++){
            int[] ele=new int[3];
            ele[0]=0;
            ele[1]=1;
            ele[2]=2;
            a.add(ele);
        }

    }
}

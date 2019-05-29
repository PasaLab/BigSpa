import ONLINE.ProtocolBuffer.ProtocolBuffer_OP;
import ONLINE.utils_ONLINE.Node_Info;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import org.json4s.ParserUtil;

import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

import static java.lang.System.out;
import static org.apache.avro.TypeEnum.a;
import static org.apache.hadoop.hbase.Version.user;
import static org.apache.hadoop.hbase.filter.ParseConstants.R;
import static org.bouncycastle.asn1.x500.style.RFC4519Style.o;
import static org.bouncycastle.asn1.x500.style.RFC4519Style.st;
import static sun.misc.Version.println;
/**
 * Created by cycy on 2018/3/20.
 */
class information{
    int len,num_bit;
    String path;
    public information(int _len,int _num_bit,String str){
        len=_len;
        num_bit=_num_bit;
        path=str;
    }
}
public class test_java {
    public static int[] randomNumber(int min,int max,int n){

        //判断是否已经达到索要输出随机数的个数
        if(n>(max-min+1) || max <min){
            return null;
        }

        int[] result = new int[n]; //用于存放结果的数组

        int count = 0;
        while(count <n){
            int num = (int)(Math.random()*(max-min))+min;
            boolean flag = true;
            for(int j=0;j<count;j++){
                if(num == result[j]){
                    flag = false;
                    break;
                }
            }
            if(flag){
                result[count] = num;
                count++;
            }
        }
        return result;
    }


    public static void main(String[] args) throws IOException {
        HashMap<String,information> map=new HashMap<>();
        information httpd_pt_info=new information(8186,4,"data/httpd.pt.batch/batches/part-");
        information psql_pt_info=new information(24968,5,"data/psql.pt.batch/batches/part-");
        information linux_pt_info=new information(249497,6,"data/linux.pt.batch/batches/part-");
        information httpd_df_info=new information(10044,5,"data/httpd.df.batch/batches/part-");
        information psql_df_info=new information(34799,5,"data/psql.df.batch/batches/part-");
        information linux_df_info=new information(69408,5,"data/linux.df.batch/batches/part-");
        map.put("httpd.pt",httpd_pt_info);
        map.put("psql.pt",psql_pt_info);
        map.put("linux.pt",linux_pt_info);
        map.put("httpd.df",httpd_df_info);
        map.put("psql.df",psql_df_info);
        map.put("linux.df",linux_df_info);

        for(String style:map.keySet()){
            information info=map.get(style);
            FileWriter writer=new FileWriter("data/"+style);
            int[] select=randomNumber(0,info.len-1,100);
            try{
                for(int i:select)
                    writer.write(info.path+String.format("%0"+info.num_bit+"d",i)+"\n");
                writer.close();
            }catch (Exception e){
                e.printStackTrace();
            }
        }


    }
}

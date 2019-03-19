import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.HashMap;


import static java.lang.System.out;
/**
 * Created by cycy on 2019/3/10.
 */
public class FormAdd_small {
    static HashMap<String,Integer> oldnodes=new HashMap<>();
    static HashMap<String,Double> label_rate=new HashMap<>();// a/d , n/e
    static {
        oldnodes.put("httpd_pt",1721418);
        oldnodes.put("httpd_df",5326664);
        oldnodes.put("psql_pt",5203419);
        oldnodes.put("psql_df",29036053);
        oldnodes.put("linux_pt",52877436);
        oldnodes.put("linux_df",63018022);

    }
    public static void main(String[] args) {
        String input_graph=args[0];
        String patterngraph=args[1];
        int num=Integer.parseInt(args[2]);
        int alreadyadd_f=Integer.parseInt(args[3]);
        String output=input_graph+"_add"+"_"+num;
        HashMap<Integer,Integer> nodemap=new HashMap<>();
        int startId=oldnodes.get(input_graph)+alreadyadd_f*num;
        out.println("num: "+num+" , alreadyadd: "+alreadyadd_f*num+" , startID: "+startId);
        try {
            BufferedReader reader = new BufferedReader(new FileReader(new File(patterngraph)));
            FileWriter writer = new FileWriter(output);
            String temp=null;
            int numnodes=0;
            while((temp=reader.readLine())!=null) {
                String[] tokens = temp.split("\t");
                int v = Integer.parseInt(tokens[0]);
                int u = Integer.parseInt(tokens[1]);
                String label=tokens[2];
                int target_v=-1,target_u=-1;
                if((target_v=nodemap.getOrDefault(v,-1))==-1){
                    target_v=startId+numnodes++;
                    nodemap.put(v,target_v);
                }
                if((target_u=nodemap.getOrDefault(u,-1))==-1){
                    target_u=startId+numnodes++;
                    nodemap.put(u,target_u);
                }
//                out.println(target_v+"\t"+target_u+"\t"+label);
                writer.write(target_v+"\t"+target_u+"\t"+label+"\n");
            }
            writer.close();
            reader.close();
        }catch(Exception e){
            e.printStackTrace();
        }

    }
}

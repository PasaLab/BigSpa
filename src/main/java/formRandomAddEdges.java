import java.io.FileWriter;
import java.util.HashMap;
import java.util.Random;

/**
 * Created by cycy on 2019/3/10.
 */
public class formRandomAddEdges {
    static HashMap<String,String[]> labeltochoose=new HashMap<>();
    static HashMap<String,Integer> oldnodes=new HashMap<>();
    static {
        oldnodes.put("httpd_pt",1721418);
        oldnodes.put("httpd_df",5326664);
        oldnodes.put("psql_pt",5203419);
        oldnodes.put("psql_df",29036053);
        oldnodes.put("linux_pt",52877436);
        oldnodes.put("linux_df",63018022);

        String[] pt_labels={"a","d"};
        String[] df_labels={"n","e"};
        labeltochoose.put("pt",pt_labels);
        labeltochoose.put("df",df_labels);
    }
    public static void main(String[] args) {
        String input_graph=args[0];
        int num=Integer.parseInt(args[1]);
        int alreadyadd=Integer.parseInt(args[2]);
        String output=input_graph+"_"+num+"_add";
        String[] strs=new String[num];
        Random rand=new Random(System.nanoTime());
        int oldnum=oldnodes.get(input_graph);
        int startId=oldnum+alreadyadd;
        if(input_graph.contains("pt")){
            num/=2;
            String[] labels=labeltochoose.get("pt");
            for(int i=0;i<num;i++){
                int v1=rand.nextInt(num)+startId;
                int v2=rand.nextInt(oldnum);
                String label=labels[rand.nextInt(2)];
                strs[2*i]=v1+"\t"+v2+"\t"+label; //a
                strs[2*i+1]=v2+"\t"+v1+"\t-"+label; //-a
            }
        }else{//df
            String[] labels=labeltochoose.get("df");
            for(int i=0;i<num;i++){
                int v1=rand.nextInt(num)+startId;
                int v2=rand.nextInt(oldnum);
                String label=labels[rand.nextInt(2)];
                if(rand.nextInt(2)==0) {
                    strs[i]=v1+"\t"+v2+"\t"+label; //n
                }else{
                    strs[i]=v2+"\t"+v1+"\t"+label; //e
                }
            }
        }
        try {
            FileWriter writer = new FileWriter(output);
            for(int i=0;i<strs.length;i++){
                writer.write(strs[i]+"\n");
            }
            writer.close();
        }catch(Exception e){
            e.printStackTrace();
        }
    }
}

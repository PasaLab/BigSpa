import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.*;

/**
 * Created by cycy on 2019/3/10.
 */
public class FormAdd_large {
    static HashMap<String,String[]> labeltochoose=new HashMap<>();
    static HashMap<String,Integer> oldnodes=new HashMap<>();
    static {
        oldnodes.put("httpd_pt",1721418);
        oldnodes.put("httpd_df",5326664);
        oldnodes.put("psql_pt",5203419);
        oldnodes.put("psql_df",29036053);
        oldnodes.put("linux_pt",52877436);
        oldnodes.put("linux_df",63018022);

        String[] pt_labels={"a","d","-a","-d"};
        String[] df_labels={"n","e"};
        labeltochoose.put("pt",pt_labels);
        labeltochoose.put("df",df_labels);
    }
    public static void main(String[] args) {
        String input_graph=args[0];
        String patterngraph=args[1];
        int num=Integer.parseInt(args[2]);
        int alreadyadd_f=Integer.parseInt(args[3]);
        String output=input_graph+"_add"+"_"+num;
        HashSet<Integer> node_old=new HashSet<>();
        int newnode_num=num;
        if(input_graph.contains("pt")) newnode_num/=5;
        int label_an=0,label_de=0;
        int startId=oldnodes.get(input_graph)+alreadyadd_f*num;
        try {
            BufferedReader reader = new BufferedReader(new FileReader(new File(patterngraph)));
            String temp=null;
            while((temp=reader.readLine())!=null) {
                String[] tokens = temp.split("\t");
                int v = Integer.parseInt(tokens[0]);
                int u = Integer.parseInt(tokens[1]);
                if(tokens[2].contains("a")||tokens[2].contains("n")) label_an++;
                else label_de++;
                node_old.add(v);
                node_old.add(u);
            }
        }catch(Exception e){
            e.printStackTrace();
        }

        try {
            double r_an=(double) label_an/(label_de+label_an);
            double r_de=(double) label_de/(label_an+label_de);
            label_an=(int)(newnode_num*r_an)+1;
            label_de=(int)(newnode_num*r_de)+1;
            FileWriter writer = new FileWriter(output);
            Integer[] old_array=node_old.toArray(new Integer[0]);
            int len_old=old_array.length;
            Random rand=new Random();
            int index=startId;
            if(input_graph.contains("pt")){
                for(int i=0;i<label_an;i++){
                    int newnode=index;
                    int oldnode=old_array[rand.nextInt(len_old)];
                    if(rand.nextInt()%2==0){
                        writer.write(newnode+"\t"+oldnode+"\t"+"-a\n");
                        writer.write(oldnode+"\t"+newnode+"\t"+"a\n");
                    }
                    else{
                        writer.write(newnode+"\t"+oldnode+"\t"+"a\n");
                        writer.write(oldnode+"\t"+newnode+"\t"+"-a\n");
                    }
                    index++;
                }
                for(int i=0;i<label_de;i++){
                    int newnode=index;
                    int oldnode=old_array[rand.nextInt(len_old)];
                    if(rand.nextInt()%2==0){
                        writer.write(newnode+"\t"+oldnode+"\t"+"-d\n");
                        writer.write(oldnode+"\t"+newnode+"\t"+"d\n");
                    }
                    else{
                        writer.write(newnode+"\t"+oldnode+"\t"+"d\n");
                        writer.write(oldnode+"\t"+newnode+"\t"+"-d\n");
                    }
                    index++;
                }
            }else{
                for(int i=0;i<label_an;i++){
                    int newnode=index;
                    int oldnode=old_array[rand.nextInt(len_old)];
                    if(rand.nextInt()%2==0){
                        writer.write(newnode+"\t"+oldnode+"\t"+"n\n");
                    }
                    else{
                        writer.write(oldnode+"\t"+newnode+"\t"+"n\n");
                    }
                    index++;
                }
                for(int i=0;i<label_de;i++){
                    int newnode=index;
                    int oldnode=old_array[rand.nextInt(len_old)];
                    if(rand.nextInt()%2==0){
                        writer.write(newnode+"\t"+oldnode+"\t"+"e\n");
                    }
                    else{
                        writer.write(oldnode+"\t"+newnode+"\t"+"e\n");
                    }
                    index++;
                }
            }
            writer.close();
        }catch(Exception e){
            e.printStackTrace();
        }
    }
}

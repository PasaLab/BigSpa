import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;

import static java.lang.System.out;

/**
 * Created by cycy on 2019/3/4.
 */
public class ProcessOffLineResult {
    static public String transfer(int index,int len){
        if(index==0) {
            StringBuilder sb=new StringBuilder();
            for(int i=0;i<len;i++) sb.append('0');
            return sb.toString();
        }
        if(index>=Math.pow(10,len-1)){
            return ""+index;
        }
        else{
            StringBuilder sb=new StringBuilder();
            while(index<Math.pow(10,len-1)){
                sb.append("0");
                len--;
            }
            sb.append(index);
            return sb.toString();
        }
    }
    public static void main(String[] args) {
        int len=1536;
        String folder_path=args[0];
        int symbol_num=Integer.parseInt(args[1]);
        for(int i=0;i<len;i++){
            String filename=folder_path+"/part-"+transfer(i,5);
//            String filename=folder_path+"/test";
            String filename_output=filename+"_t";
            out.println(filename);
            try {
                FileReader reader = new FileReader(filename);
                FileWriter writer=new FileWriter(filename_output);
                BufferedReader br=new BufferedReader(reader);
                String tempString = null;
                while((tempString=br.readLine())!=null){
//                    out.println(tempString);
                    StringBuilder sb=new StringBuilder();
                    char[] chars=tempString.toCharArray();
                    int index=0;
                    int vid;
                    while(chars[index]!='-')
                        sb.append(chars[index++]);
                    vid=Integer.parseInt(sb.toString());

                    for(int symbol=0;symbol<symbol_num;symbol++){
                        //f
                        while(chars[index]!='[') index++;
                        index++;
                        if (chars[index + 1] != 'n') {//不是 null
                            long vid_label_pos=vid;
                            vid_label_pos=(vid_label_pos<<32)+(symbol<<1)+0;
                            writer.write(vid_label_pos+":");
                            int uid,c1,c2;
                            boolean Iscontinue=true;
                            while(Iscontinue) {
                                while (chars[index] != '(') index++;
                                index++;
                                sb = new StringBuilder();
                                while (chars[index] != ',') sb.append(chars[index++]);
                                uid = Integer.parseInt(sb.toString());
//                                out.print("uid: "+sb.toString());
                                index += 2;
                                sb = new StringBuilder();
                                while (chars[index] != ',') sb.append(chars[index++]);
//                                out.print(" c1: "+sb.toString());
                                c1 = Integer.parseInt(sb.toString());
                                index++;
                                sb = new StringBuilder();
                                while (chars[index] != ',') sb.append(chars[index++]);
//                                out.println(" c2: "+sb.toString());
                                c2 = Integer.parseInt(sb.toString());
//                                writer.write(uid+" "+c1+" "+c2+" ");
                                long counts=c1;
                                counts=(counts<<32)+c2;
                                writer.write(uid+"\t"+counts+"\t");
                                while(chars[index]!='('){
//                                    out.print(chars[index]);
                                    if(chars[index]==']') {
                                        Iscontinue=false;
                                        break;
                                    }
                                    index++;
                                }
//                                out.println();
                            }
                            writer.write("\n");
                        }
                        //b
                        while(chars[index]!='[') index++;
                        index++;
                        if (chars[index + 1] != 'n') {//不是 null
                            long vid_label_pos=vid;
                            vid_label_pos=(vid_label_pos<<32)+(symbol<<1)+1;
                            writer.write(vid_label_pos+":");
                            int uid,c1,c2;
                            boolean Iscontinue=true;
                            while(Iscontinue) {
                                while (chars[index] != '(') index++;
                                index++;
                                sb = new StringBuilder();
                                while (chars[index] != ',') sb.append(chars[index++]);
//                                out.print("uid: "+sb.toString());
                                uid = Integer.parseInt(sb.toString());
                                index += 2;
                                sb = new StringBuilder();
                                while (chars[index] != ',') sb.append(chars[index++]);
//                                out.print(" c1: "+sb.toString());
                                c1 = Integer.parseInt(sb.toString());
                                index++;
                                sb = new StringBuilder();
                                while (chars[index] != ',') sb.append(chars[index++]);
//                                out.println(" c2: "+sb.toString());
                                c2 = Integer.parseInt(sb.toString());
//                                writer.write(uid+" "+c1+" "+c2+" ");
                                long counts=c1;
                                counts=(counts<<32)+c2;
                                writer.write(uid+"\t"+counts+"\t");

//                                out.println("uid: "+uid+" counts: "+counts);
                                while(chars[index]!='('){
//                                    out.print(chars[index]);
                                    if(chars[index]==']'){
                                        Iscontinue=false;
                                        break;
                                    }
                                    index++;
                                }
//                                out.println();
                            }
                            writer.write("\n");
                        }
                    }
                }
                reader.close();
                writer.close();
                FileOperation.deleteFile(filename);
                FileOperation.RenameFile(filename_output,filename);
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }
}

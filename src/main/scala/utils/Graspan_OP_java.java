package utils;

import java.util.*;

import static java.lang.System.exit;
import static java.lang.System.out;
import static org.apache.hadoop.yarn.webapp.hamlet.HamletSpec.Media.print;


/**
 * Created by cycy on 2018/3/4.
 */


public class Graspan_OP_java {
    /**
     * join
     * @return
     */
    public static long getCorrectLength(int[][] grammar,Map<Integer,Integer> directadd, int symbol_num,
                                       int[] old_f_index_list, int[] new_f_index_list, int[] old_b_index_list, int[] new_b_index_list){
        long sum_length=0;
        for(int[] gram:grammar){
            int f=gram[0];
            int b=gram[1];
            int res_label=gram[2];
            /**
             * 判断f和b谁长，决定以谁为主forloop，避免空转
             */
            int new_f_length=-1;
            int new_b_length=-1;
            int old_f_length=-1;
            int old_b_length=-1;
            if(f==0) {
                old_f_length=old_f_index_list[f]+1;
                new_f_length=new_f_index_list[f]-old_f_index_list[f];
            }
            else {
                old_f_length=old_f_index_list[f]-new_f_index_list[f-1];
                new_f_length=new_f_index_list[f]-old_f_index_list[f];
            }
            if(b==0){
                old_b_length=old_b_index_list[b]-new_f_index_list[symbol_num-1];
                new_b_length=new_b_index_list[b]-old_b_index_list[b];
            }
            else {
                old_b_length=old_b_index_list[b]-new_b_index_list[b-1];
                new_b_length=new_b_index_list[b]-old_b_index_list[b];
            }

            if(directadd.containsKey(res_label)){//需添加额外label
                sum_length+=2*( new_f_length*new_b_length + new_f_length*old_b_length + old_f_length*new_b_length);
            }
            else{//仅添加当前label
                sum_length+=( new_f_length*new_b_length + new_f_length*old_b_length + old_f_length*new_b_length);
            }
        }
        return sum_length;
    }
    public static  int[] join(int flag, int[] all_edges, int[]
            old_f_index_list, int[] new_f_index_list, int[] old_b_index_list, int[] new_b_index_list, int[][] grammar, int symbol_num,
                                           int[][] directadd0){

        Map<Integer,Integer> directadd=new HashMap<Integer, Integer>();
        for(int[] i:directadd0) directadd.put(i[0],i[1]);
        long lenofres=getCorrectLength(grammar,directadd,symbol_num,old_f_index_list,new_f_index_list,
                old_b_index_list,new_b_index_list)*3;
//        if(lenofres>0) out.println("lenofres: "+lenofres+" , (int): "+(int)lenofres);
        if(lenofres>Integer.MAX_VALUE){

            out.println("lenofres: (long) - "+lenofres+" , (int) - "+(int)lenofres);

            out.print("new_f_index_list: ");
            out.println(Arrays.toString(new_f_index_list));

            out.print("new_b_index_list: ");
            out.println(Arrays.toString(new_b_index_list));

            out.print("old_f_index_list: ");
            out.println(Arrays.toString(old_f_index_list));

            out.print("old_b_index_list: ");
            out.println(Arrays.toString(old_b_index_list));
        }
        int[] res=new int[(int)lenofres];
        int indexofres=0;
        for(int[] gram:grammar){
            int f=gram[0];
            int b=gram[1];
            int res_label=gram[2];

            /**
             * 判断f和b谁长，决定以谁为主forloop，避免空转
             */
            int new_f_length=-1;
            int new_b_length=-1;
            int old_f_length=-1;
            int old_b_length=-1;
            if(f==0) {
                old_f_length=old_f_index_list[f]+1;
                new_f_length=new_f_index_list[f]-old_f_index_list[f];
            }
            else {
                old_f_length=old_f_index_list[f]-new_f_index_list[f-1];
                new_f_length=new_f_index_list[f]-old_f_index_list[f];
            }
            if(b==0){
                old_b_length=old_b_index_list[b]-new_f_index_list[symbol_num-1];
                new_b_length=new_b_index_list[b]-old_b_index_list[b];
            }
            else {
                old_b_length=old_b_index_list[b]-new_b_index_list[b-1];
                new_b_length=new_b_index_list[b]-old_b_index_list[b];
            }

            if(directadd.containsKey(res_label)){//需添加额外label
                int directadd_symbol=directadd.get(res_label);
                /**
                 * 1、新边之间的两两连接
                 */
                if(new_f_length>0&&new_b_length>0) {
                    int j_end=new_f_index_list[f];
                    int k_end=new_b_index_list[b];
                    for (int j = new_f_index_list[f]-new_f_length+1; j <= j_end; j++) {
                        for (int k = new_b_index_list[b]-new_b_length+1; k <= k_end; k++) {
                            res[indexofres++]=all_edges[j];
                            res[indexofres++]=all_edges[k];
                            res[indexofres++]=res_label;

                            res[indexofres++]=all_edges[j];
                            res[indexofres++]=all_edges[k];
                            res[indexofres++]=directadd_symbol;
                        }
                    }
                }
                /**
                 * 新边与旧边之间的两两连接
                 */
                //新边在前，旧边在后
                if(new_f_length>0&&old_b_length>0){
                    int j_end=new_f_index_list[f];
                    int k_end=old_b_index_list[b];
                    for(int j=new_f_index_list[f]-new_f_length+1;j<=j_end;j++){
                        for(int k=old_b_index_list[b]-old_b_length+1;k<=k_end;k++){
                            res[indexofres++]=all_edges[j];
                            res[indexofres++]=all_edges[k];
                            res[indexofres++]=res_label;

                            res[indexofres++]=all_edges[j];
                            res[indexofres++]=all_edges[k];
                            res[indexofres++]=directadd_symbol;
                        }
                    }
                }
                //旧边在前，新边在后
                if(old_f_length>0&&new_b_length>0){
                    int j_end=old_f_index_list[f];
                    int k_end=new_b_index_list[b];
                    for(int j=old_f_index_list[f]-old_f_length+1;j<=j_end;j++){
                        for(int k=new_b_index_list[b]-new_b_length+1;k<=k_end;k++){
                            res[indexofres++]=all_edges[j];
                            res[indexofres++]=all_edges[k];
                            res[indexofres++]=res_label;

                            res[indexofres++]=all_edges[j];
                            res[indexofres++]=all_edges[k];
                            res[indexofres++]=directadd_symbol;
                        }
                    }
                }
            }
            else{//仅添加当前label
                /**
                 * 1、新边之间的两两连接
                 */
                if(new_f_length>0&&new_b_length>0) {
                    int j_end=new_f_index_list[f];
                    int k_end=new_b_index_list[b];
                    for (int j = new_f_index_list[f]-new_f_length+1; j <= j_end; j++) {
                        for (int k = new_b_index_list[b]-new_b_length+1; k <= k_end; k++) {
                            res[indexofres++]=all_edges[j];
                            res[indexofres++]=all_edges[k];
                            res[indexofres++]=res_label;

                        }
                    }
                }
                /**
                 * 新边与旧边之间的两两连接
                 */
                //新边在前，旧边在后
                if(new_f_length>0&&old_b_length>0){
                    int j_end=new_f_index_list[f];
                    int k_end=old_b_index_list[b];
                    for(int j=new_f_index_list[f]-new_f_length+1;j<=j_end;j++){
                        for(int k=old_b_index_list[b]-old_b_length+1;k<=k_end;k++){
                            res[indexofres++]=all_edges[j];
                            res[indexofres++]=all_edges[k];
                            res[indexofres++]=res_label;
                        }
                    }
                }
                //旧边在前，新边在后
                if(old_f_length>0&&new_b_length>0){
                    int j_end=old_f_index_list[f];
                    int k_end=new_b_index_list[b];
                    for(int j=old_f_index_list[f]-old_f_length+1;j<=j_end;j++){
                        for(int k=new_b_index_list[b]-new_b_length+1;k<=k_end;k++){
                            res[indexofres++]=all_edges[j];
                            res[indexofres++]=all_edges[k];
                            res[indexofres++]=res_label;
                        }
                    }
                }
            }
        }

            return res;
    }

    public static  long[] join_df(int[] n_edges,int[] e_edge_after){
        long[] res_edges=new long[n_edges.length*e_edge_after.length];
        /**
         * n与e之间的两两连接
         */
        int lenofres=0;
        int res_label=0;
        //n在前，e在后
        int i_end=n_edges.length;
        int j_end=e_edge_after.length;
        for(int i=0;i<i_end;i++){
            for(int j=0;j<j_end;j++){
                res_edges[lenofres++]= (((long)e_edge_after[j])<<32)+n_edges[i] ;
            }
        }
        return res_edges;
    }

    public static  long[] join_df_loop(int flag, long[] n_edges, int[] e_edge_after, int n_start,int[]
            n_end){

        int end=n_start;
        long[] res_edges=new long[(n_edges.length-n_start)*e_edge_after.length];
        /**
         * n与e之间的两两连接
         */
        int lenofres=0;
        int res_label=0;
        //n在前，e在后
        int i_end=n_edges.length;
        int j_end=e_edge_after.length;
        int i=n_start;
       while(i<i_end&&( (n_edges[i]>>>32) ==flag)){
            for(int j=0;j<j_end;j++){
                res_edges[lenofres++]= ( ((long)e_edge_after[j]) <<32 )+ (n_edges[i]&0xffffffffL);
            }
            i++;
        }
        n_end[0]=i;
        long[] final_res=new long[lenofres];
        System.arraycopy(res_edges,0,final_res,0,lenofres);
        return final_res;
    }

}

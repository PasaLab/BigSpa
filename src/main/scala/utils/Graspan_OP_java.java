package utils;

import java.util.*;


/**
 * Created by cycy on 2018/3/4.
 */

class IntArrayCompartor_According_Symbol implements Comparator
{
    public int compare(Object o1, Object o2)
    {
        int[] array1= (int[])o1;
        int[] array2= (int[])o2;
        if(array1[0]<array2[0]) return -1;
        else if(array1[0]>array2[0]) return 1;
        else return 0;
    }
}

class IntArrayCompartor_Distinguish_Direction implements Comparator
{
    private int flag=-1;
    public int compare(Object o1, Object o2)
    {
        int[] array1= (int[])o1;
        int[] array2= (int[])o2;
        if(array1[2]<array2[2]) return -1;
        else if(array1[2]>array2[2]) return 1;
        else return 0;
    }

}

public class Graspan_OP_java {
    public static int[][] get_symbol_index_range(int[][] list,int symbol_num){
        int[][] result=new int[symbol_num][2];
        int[] default_res={-1,-1};
        for(int i=0;i<symbol_num;i++){
            result[i]=new int[2];
            result[i][0]=-1;
            result[i][1]=-1;
        }
        int len=list.length;
        if(len==0) return result;

//        System.out.println("in get range index");
//        for(int[] i:array) System.out.println("("+i[0]+","+i[1]+")");
        int currentsymbol=list[0][0];
        result[currentsymbol][0]=0;
        int i=1;
        for(;i<len;i++){
            if(list[i][0]!=currentsymbol){
                result[currentsymbol][1]=i-1;
                currentsymbol=list[i][0];
                result[currentsymbol][0]=i;
            }
        }
        result[currentsymbol][1]=i-1;
        return result;
    }

    /**
     * 缺少directadd，需要在scala代码中补上
     * @param grammar
     * @param symbol_num
     * @return
     */
    public static List<int[]> join_flat(int flag,int[][] old_edge_before,int[][] old_edge_after,
                                        int[][] new_edge_before,int[][] new_edge_after,
                                        int[][] grammar,int symbol_num){
//        String tmp="XXX mid= "+flag+"\n";
//        for(int[] i:new_array){
//            tmp+=" ("+i[0]+","+i[1]+") ";
//        }
        double t0=System.nanoTime();
        List<int[]> res_edges=new ArrayList<int[]>();
        IntArrayCompartor_According_Symbol comparator = new IntArrayCompartor_According_Symbol();
//        tmp+="\nedge_before before sort\n";
//        for(int[] i:new_edge_before){
//            tmp+=" ("+i[0]+","+i[1]+") ";
//        }
//        tmp+="\nedge_after before sort\n";
//        for(int[] i:new_edge_after){
//            tmp+=" ("+i[0]+","+i[1]+") ";
//        }

        Arrays.sort(new_edge_before,comparator);
        Arrays.sort(new_edge_after,comparator);
        Arrays.sort(old_edge_before,comparator);
        Arrays.sort(old_edge_after,comparator);

//        tmp+="\nedge_before after sort\n";
//        for(int[] i:new_edge_before){
//            tmp+=" ("+i[0]+","+i[1]+") ";
//        }
//        tmp+="\nedge_after after sort\n";
//        for(int[] i:new_edge_after){
//            tmp+=" ("+i[0]+","+i[1]+") ";
//        }
        int[][] new_edge_before_symbol_index_range=get_symbol_index_range(new_edge_before,symbol_num);
        int[][] new_edge_after_symbol_index_range=get_symbol_index_range(new_edge_after,symbol_num);
        int[][] old_edge_before_symbol_index_range=get_symbol_index_range(old_edge_before,symbol_num);
        int[][] old_edge_after_symbol_index_range=get_symbol_index_range(old_edge_after,symbol_num);

//        tmp+="\nedge_before symbol index\n";
//        for(int[] i:new_edge_before_symbol_index_range){
//            tmp+=" ("+i[0]+","+i[1]+") ";
//        }
//        tmp+="\nedge_after symbol index\n";
//        for(int[] i:new_edge_after_symbol_index_range){
//            tmp+=" ("+i[0]+","+i[1]+") ";
//        }
//        tmp+="\nold_before symbol index\n";
//        for(int[] i:old_edge_before_symbol_index_range){
//            tmp+=" ("+i[0]+","+i[1]+") ";
//        }
//        tmp+="\nold_after symbol index\n";
//        for(int[] i:old_edge_after_symbol_index_range){
//            tmp+=" ("+i[0]+","+i[1]+") ";
//        }

        for(int[] gram:grammar){
            int f=gram[0];
            int b=gram[1];
            int res_label=gram[2];
            /**
             * 1、新边之间的两两连接
             */
            if(new_edge_before_symbol_index_range[f][0]!=-1&&new_edge_after_symbol_index_range[b][0]!=-1) {
                if((new_edge_before_symbol_index_range[f][1]-new_edge_before_symbol_index_range[f][0
                        ]-new_edge_after_symbol_index_range[b][1]+new_edge_after_symbol_index_range[b][0])<0){
                    for (int j = new_edge_before_symbol_index_range[f][0]; j <= new_edge_before_symbol_index_range[f][1]; j++) {
                        for (int k = new_edge_after_symbol_index_range[b][0]; k <= new_edge_after_symbol_index_range[b][1]; k++) {
                            int[] ele = new int[3];
                            ele[0] = new_edge_before[j][1];
                            ele[1] = new_edge_after[k][1];
                            ele[2] = res_label;
                            res_edges.add(ele);
                        }
                    }
                }
                else{
                    for (int k = new_edge_after_symbol_index_range[b][0]; k <= new_edge_after_symbol_index_range[b][1]; k++) {
                        for (int j = new_edge_before_symbol_index_range[f][0]; j <= new_edge_before_symbol_index_range[f][1]; j++) {
                            int[] ele = new int[3];
                            ele[0] = new_edge_before[j][1];
                            ele[1] = new_edge_after[k][1];
                            ele[2] = res_label;
                            res_edges.add(ele);
                        }
                    }
                }

            }
            /**
             * 新边与旧边之间的两两连接
             */
            //新边在前，旧边在后
            if(new_edge_before_symbol_index_range[f][0]!=-1&&old_edge_after_symbol_index_range[b][0]!=-1){
                if((new_edge_before_symbol_index_range[f][1]-new_edge_before_symbol_index_range[f][0
                        ]-old_edge_after_symbol_index_range[b][1]+old_edge_after_symbol_index_range[b][0])<0) {
                    for(int j=new_edge_before_symbol_index_range[f][0];j<=new_edge_before_symbol_index_range[f][1];j++){
                        for(int k=old_edge_after_symbol_index_range[b][0];k<=old_edge_after_symbol_index_range[b][1];
                            k++){
                            int[] ele=new int[3];
                            ele[0]=new_edge_before[j][1];
                            ele[1]=old_edge_after[k][1];
                            ele[2]=res_label;
                            res_edges.add(ele);
                        }
                    }
                }
                else{
                    for(int k=old_edge_after_symbol_index_range[b][0];k<=old_edge_after_symbol_index_range[b][1];
                        k++){
                        for(int j=new_edge_before_symbol_index_range[f][0];j<=new_edge_before_symbol_index_range[f][1];j++){
                            int[] ele=new int[3];
                            ele[0]=new_edge_before[j][1];
                            ele[1]=old_edge_after[k][1];
                            ele[2]=res_label;
                            res_edges.add(ele);
                        }
                    }
                }

            }
            //旧边在前，新边在后
            if(old_edge_before_symbol_index_range[f][0]!=-1&&new_edge_after_symbol_index_range[b][0]!=-1){
                if((old_edge_before_symbol_index_range[f][1]-old_edge_before_symbol_index_range[f][0
                        ]-new_edge_after_symbol_index_range[b][1]+new_edge_after_symbol_index_range[b][0])<0){
                    for(int j=old_edge_before_symbol_index_range[f][0];j<=old_edge_before_symbol_index_range[f][1];j++){
                        for(int k=new_edge_after_symbol_index_range[b][0];k<=new_edge_after_symbol_index_range[b][1];
                            k++){
                            int [] ele=new int[3];
                            ele[0]=old_edge_before[j][1];
                            ele[1]=new_edge_after[k][1];
                            ele[2]=res_label;
                            res_edges.add(ele);
                        }
                    }
                }
                else{
                    for(int k=new_edge_after_symbol_index_range[b][0];k<=new_edge_after_symbol_index_range[b][1];
                        k++){
                        for(int j=old_edge_before_symbol_index_range[f][0];j<=old_edge_before_symbol_index_range[f][1];j++){
                            int [] ele=new int[3];
                            ele[0]=old_edge_before[j][1];
                            ele[1]=new_edge_after[k][1];
                            ele[2]=res_label;
                            res_edges.add(ele);
                        }
                    }
                }

            }
        }
//        System.out.println(tmp);
        return res_edges;
    }


    public static  List<int[]> join_fully_compressed(int flag,int[] all_edges,int old_f_end,int new_f_end,int
            old_b_end,int[][] grammar,int symbol_num){
        List<int[]> res_edges=new ArrayList<int[]>();
        IntArrayCompartor_According_Symbol comparator = new IntArrayCompartor_According_Symbol();

        int[][] old_edge_before = new int[(old_f_end - 0 + 1) / 2][2];
        int[][] old_edge_after = new int[(old_b_end - new_f_end) / 2][2];
        int[][] new_edge_before = new int[(new_f_end - old_f_end) / 2][2];
        int[][] new_edge_after = new int[(all_edges.length - old_b_end - 1) / 2][2];

        int len = old_edge_before.length;
        for (int i = 0; i < len; i++) {
            old_edge_before[i] = new int[2];
            old_edge_before[i][0] = all_edges[i * 2];
            old_edge_before[i][1] = all_edges[i * 2 + 1];
        }
        len = old_edge_after.length;
        for (int i = 0; i < len; i++) {
            old_edge_after[i] = new int[2];
            old_edge_after[i][0] = all_edges[i * 2 + 1 + new_f_end];
            old_edge_after[i][1] = all_edges[i * 2 + 2 + new_f_end];
        }
        len = new_edge_before.length;
        for (int i = 0; i < len; i++) {
            new_edge_before[i] = new int[2];
            new_edge_before[i][0] = all_edges[i * 2 + 1 + old_f_end];
            new_edge_before[i][1] = all_edges[i * 2 + 2 + old_f_end];
        }
        len = new_edge_after.length;
        for (int i = 0; i < len; i++) {
            new_edge_after[i] = new int[2];
            new_edge_after[i][0] = all_edges[i * 2 + 1 + old_b_end];
            new_edge_after[i][1] = all_edges[i * 2 + 2 + old_b_end];
        }


        Arrays.sort(new_edge_before,comparator);
        Arrays.sort(new_edge_after,comparator);
        Arrays.sort(old_edge_before,comparator);
        Arrays.sort(old_edge_after,comparator);

        int[][] new_edge_before_symbol_index_range=get_symbol_index_range(new_edge_before,symbol_num);
        int[][] new_edge_after_symbol_index_range=get_symbol_index_range(new_edge_after,symbol_num);
        int[][] old_edge_before_symbol_index_range=get_symbol_index_range(old_edge_before,symbol_num);
        int[][] old_edge_after_symbol_index_range=get_symbol_index_range(old_edge_after,symbol_num);

        for(int[] gram:grammar){
            int f=gram[0];
            int b=gram[1];
            int res_label=gram[2];
            /**
             * 1、新边之间的两两连接
             */
            if(new_edge_before_symbol_index_range[f][0]!=-1&&new_edge_after_symbol_index_range[b][0]!=-1) {
                if((new_edge_before_symbol_index_range[f][1]-new_edge_before_symbol_index_range[f][0
                        ]-new_edge_after_symbol_index_range[b][1]+new_edge_after_symbol_index_range[b][0])<0){
                    for (int j = new_edge_before_symbol_index_range[f][0]; j <= new_edge_before_symbol_index_range[f][1]; j++) {
                        for (int k = new_edge_after_symbol_index_range[b][0]; k <= new_edge_after_symbol_index_range[b][1]; k++) {
                            int[] ele = new int[3];
                            ele[0] = new_edge_before[j][1];
                            ele[1] = new_edge_after[k][1];
                            ele[2] = res_label;
                            res_edges.add(ele);
                        }
                    }
                }
                else{
                    for (int k = new_edge_after_symbol_index_range[b][0]; k <= new_edge_after_symbol_index_range[b][1]; k++) {
                        for (int j = new_edge_before_symbol_index_range[f][0]; j <= new_edge_before_symbol_index_range[f][1]; j++) {
                            int[] ele = new int[3];
                            ele[0] = new_edge_before[j][1];
                            ele[1] = new_edge_after[k][1];
                            ele[2] = res_label;
                            res_edges.add(ele);
                        }
                    }
                }

            }
            /**
             * 新边与旧边之间的两两连接
             */
            //新边在前，旧边在后
            if(new_edge_before_symbol_index_range[f][0]!=-1&&old_edge_after_symbol_index_range[b][0]!=-1){
                if((new_edge_before_symbol_index_range[f][1]-new_edge_before_symbol_index_range[f][0
                        ]-old_edge_after_symbol_index_range[b][1]+old_edge_after_symbol_index_range[b][0])<0) {
                    for(int j=new_edge_before_symbol_index_range[f][0];j<=new_edge_before_symbol_index_range[f][1];j++){
                        for(int k=old_edge_after_symbol_index_range[b][0];k<=old_edge_after_symbol_index_range[b][1];
                            k++){
                            int[] ele=new int[3];
                            ele[0]=new_edge_before[j][1];
                            ele[1]=old_edge_after[k][1];
                            ele[2]=res_label;
                            res_edges.add(ele);
                        }
                    }
                }
                else{
                    for(int k=old_edge_after_symbol_index_range[b][0];k<=old_edge_after_symbol_index_range[b][1];
                        k++){
                        for(int j=new_edge_before_symbol_index_range[f][0];j<=new_edge_before_symbol_index_range[f][1];j++){
                            int[] ele=new int[3];
                            ele[0]=new_edge_before[j][1];
                            ele[1]=old_edge_after[k][1];
                            ele[2]=res_label;
                            res_edges.add(ele);
                        }
                    }
                }

            }
            //旧边在前，新边在后
            if(old_edge_before_symbol_index_range[f][0]!=-1&&new_edge_after_symbol_index_range[b][0]!=-1){
                if((old_edge_before_symbol_index_range[f][1]-old_edge_before_symbol_index_range[f][0
                        ]-new_edge_after_symbol_index_range[b][1]+new_edge_after_symbol_index_range[b][0])<0){
                    for(int j=old_edge_before_symbol_index_range[f][0];j<=old_edge_before_symbol_index_range[f][1];j++){
                        for(int k=new_edge_after_symbol_index_range[b][0];k<=new_edge_after_symbol_index_range[b][1];
                            k++){
                            int [] ele=new int[3];
                            ele[0]=old_edge_before[j][1];
                            ele[1]=new_edge_after[k][1];
                            ele[2]=res_label;
                            res_edges.add(ele);
                        }
                    }
                }
                else{
                    for(int k=new_edge_after_symbol_index_range[b][0];k<=new_edge_after_symbol_index_range[b][1];
                        k++){
                        for(int j=old_edge_before_symbol_index_range[f][0];j<=old_edge_before_symbol_index_range[f][1];j++){
                            int [] ele=new int[3];
                            ele[0]=old_edge_before[j][1];
                            ele[1]=new_edge_after[k][1];
                            ele[2]=res_label;
                            res_edges.add(ele);
                        }
                    }
                }

            }
        }
//        System.out.println(tmp);
        return res_edges;
    }

    public static  List<int[]> join_fully_compressed_presort(int flag, int[] all_edges, int[] old_f_index_list, int[]
            new_f_index_list, int[] old_b_index_list, int[] new_b_index_list, int[][] grammar, int symbol_num,
                                                             int[][] directadd0){
//        int all_len=all_edges.length/2;
//        int old_f_len=(old_f_index_list[symbol_num-1]-old_f_index_list[0])/2;
//        int new_f_len=(new_f_index_list[symbol_num-1]-new_f_index_list[0])/2;
//        int old_b_len=(old_b_index_list[symbol_num-1]-old_b_index_list[0])/2;
//        int new_b_len=(new_b_index_list[symbol_num-1]-new_b_index_list[0])/2;
        //new_f_len*new_b_len+old_f_len*new_b_len+new_f_len*old_b_len
        List<int[]> res_edges=new ArrayList<int[]>();
        Map<Integer,Integer> directadd=new HashMap<Integer, Integer>();
        for(int[] i:directadd0) directadd.put(i[0],i[1]);

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
                new_f_length=new_f_index_list[f]-old_f_index_list[symbol_num-1];
            }
            else {
                old_f_length=old_f_index_list[f]-old_f_index_list[f-1];
                new_f_length=new_f_index_list[f]-new_f_index_list[f-1];
            }
            if(b==0){
                old_b_length=old_b_index_list[b]-new_f_index_list[symbol_num-1];
                new_b_length=new_b_index_list[b]-old_b_index_list[symbol_num-1];
            }
            else {
                old_b_length=old_b_index_list[b]-old_b_index_list[b-1];
                new_b_length=new_b_index_list[b]-new_b_index_list[b-1];
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
                            int[] ele = {all_edges[j],all_edges[k],res_label};
                            res_edges.add(ele);

                            int[] ele_add = {all_edges[j],all_edges[k],directadd_symbol};
                            res_edges.add(ele_add);
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
                            int[] ele = {all_edges[j],all_edges[k],res_label};
                            res_edges.add(ele);

                            int[] ele_add = {all_edges[j],all_edges[k],directadd_symbol};
                            res_edges.add(ele_add);
                        }
                    }
                }
                //旧边在前，新边在后
                if(old_f_length>0&&new_b_length>0){
                    int j_end=old_f_index_list[f];
                    int k_end=new_b_index_list[b];
                    for(int j=old_f_index_list[f]-old_f_length+1;j<=j_end;j++){
                        for(int k=new_b_index_list[b]-new_b_length+1;k<=k_end;k++){
                            int[] ele = {all_edges[j],all_edges[k],res_label};
                            res_edges.add(ele);

                            int[] ele_add = {all_edges[j],all_edges[k],directadd_symbol};
                            res_edges.add(ele_add);
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
                            int[] ele = {all_edges[j],all_edges[k],res_label};
                            res_edges.add(ele);
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
                            int[] ele = {all_edges[j],all_edges[k],res_label};
                            res_edges.add(ele);
                        }
                    }
                }
                //旧边在前，新边在后
                if(old_f_length>0&&new_b_length>0){
                    int j_end=old_f_index_list[f];
                    int k_end=new_b_index_list[b];
                    for(int j=old_f_index_list[f]-old_f_length+1;j<=j_end;j++){
                        for(int k=new_b_index_list[b]-new_b_length+1;k<=k_end;k++){
                            int[] ele = {all_edges[j],all_edges[k],res_label};
                            res_edges.add(ele);
                        }
                    }
                }
            }
        }
//        System.out.println(tmp);
        return res_edges;
    }



    public static  List<int[]> join_fully_compressed_df(int flag,int[] n_edges,int[] e_edge_after){
        List<int[]> res_edges=new ArrayList<int[]>();
        /**
         * n与e之间的两两连接
         */
        int res_label=0;
        //n在前，e在后
        int i_end=n_edges.length;
        int j_end=e_edge_after.length;
        for(int i=0;i<i_end;i++){
            for(int j=0;j<j_end;j++){
                int[] ele={n_edges[i],e_edge_after[j],res_label};
                res_edges.add(ele);
            }
        }
//        System.out.println(tmp);
        return res_edges;
    }



    public static  List<int[]> join_fully_compressed_presort_improve(int flag, int[] all_edges, int[] old_f_index_list,
                                                                 int[]
            new_f_index_list, int[] old_b_index_list, int[] new_b_index_list, int[][] grammar, int symbol_num,
                                                             int[][] directadd0){
//        int all_len=all_edges.length/2;
//        int old_f_len=(old_f_index_list[symbol_num-1]-old_f_index_list[0])/2;
//        int new_f_len=(new_f_index_list[symbol_num-1]-new_f_index_list[0])/2;
//        int old_b_len=(old_b_index_list[symbol_num-1]-old_b_index_list[0])/2;
//        int new_b_len=(new_b_index_list[symbol_num-1]-new_b_index_list[0])/2;
        //new_f_len*new_b_len+old_f_len*new_b_len+new_f_len*old_b_len
        List<int[]> res_edges=new ArrayList<int[]>();
        Map<Integer,Integer> directadd=new HashMap<Integer, Integer>();
        for(int[] i:directadd0) directadd.put(i[0],i[1]);

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
                            int[] ele = {all_edges[j],all_edges[k],res_label};
                            res_edges.add(ele);

                            int[] ele_add = {all_edges[j],all_edges[k],directadd_symbol};
                            res_edges.add(ele_add);
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
                            int[] ele = {all_edges[j],all_edges[k],res_label};
                            res_edges.add(ele);

                            int[] ele_add = {all_edges[j],all_edges[k],directadd_symbol};
                            res_edges.add(ele_add);
                        }
                    }
                }
                //旧边在前，新边在后
                if(old_f_length>0&&new_b_length>0){
                    int j_end=old_f_index_list[f];
                    int k_end=new_b_index_list[b];
                    for(int j=old_f_index_list[f]-old_f_length+1;j<=j_end;j++){
                        for(int k=new_b_index_list[b]-new_b_length+1;k<=k_end;k++){
                            int[] ele = {all_edges[j],all_edges[k],res_label};
                            res_edges.add(ele);

                            int[] ele_add = {all_edges[j],all_edges[k],directadd_symbol};
                            res_edges.add(ele_add);
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
                            int[] ele = {all_edges[j],all_edges[k],res_label};
                            res_edges.add(ele);
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
                            int[] ele = {all_edges[j],all_edges[k],res_label};
                            res_edges.add(ele);
                        }
                    }
                }
                //旧边在前，新边在后
                if(old_f_length>0&&new_b_length>0){
                    int j_end=old_f_index_list[f];
                    int k_end=new_b_index_list[b];
                    for(int j=old_f_index_list[f]-old_f_length+1;j<=j_end;j++){
                        for(int k=new_b_index_list[b]-new_b_length+1;k<=k_end;k++){
                            int[] ele = {all_edges[j],all_edges[k],res_label};
                            res_edges.add(ele);
                        }
                    }
                }
            }
        }
//        System.out.println(tmp);
        return res_edges;
    }

    public static  int[] join_fully_compressed_presort_improve_compress(int flag, int[] all_edges, int[]
            old_f_index_list, int[] new_f_index_list, int[] old_b_index_list, int[] new_b_index_list, int[][] grammar, int symbol_num,
                                                                     int[][] directadd0){
//        int all_len=all_edges.length/2;
        int old_f_len_sum=(old_f_index_list[symbol_num-1]-old_f_index_list[0])/2;
        int new_f_len_sum=(new_f_index_list[symbol_num-1]-new_f_index_list[0])/2;
        int old_b_len_sum=(old_b_index_list[symbol_num-1]-old_b_index_list[0])/2;
        int new_b_len_sum=(new_b_index_list[symbol_num-1]-new_b_index_list[0])/2;
        int estimate_len=new_f_len_sum*new_b_len_sum+old_f_len_sum*new_b_len_sum+new_f_len_sum*old_b_len_sum;
        int[] res=new int[estimate_len*6];
        Map<Integer,Integer> directadd=new HashMap<Integer, Integer>();
        for(int[] i:directadd0) directadd.put(i[0],i[1]);

        int lenofres=0;
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
                            res[lenofres++]=all_edges[j];
                            res[lenofres++]=all_edges[k];
                            res[lenofres++]=res_label;

                            res[lenofres++]=all_edges[j];
                            res[lenofres++]=all_edges[k];
                            res[lenofres++]=directadd_symbol;
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
                            res[lenofres++]=all_edges[j];
                            res[lenofres++]=all_edges[k];
                            res[lenofres++]=res_label;

                            res[lenofres++]=all_edges[j];
                            res[lenofres++]=all_edges[k];
                            res[lenofres++]=directadd_symbol;
                        }
                    }
                }
                //旧边在前，新边在后
                if(old_f_length>0&&new_b_length>0){
                    int j_end=old_f_index_list[f];
                    int k_end=new_b_index_list[b];
                    for(int j=old_f_index_list[f]-old_f_length+1;j<=j_end;j++){
                        for(int k=new_b_index_list[b]-new_b_length+1;k<=k_end;k++){
                            res[lenofres++]=all_edges[j];
                            res[lenofres++]=all_edges[k];
                            res[lenofres++]=res_label;

                            res[lenofres++]=all_edges[j];
                            res[lenofres++]=all_edges[k];
                            res[lenofres++]=directadd_symbol;
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
                            res[lenofres++]=all_edges[j];
                            res[lenofres++]=all_edges[k];
                            res[lenofres++]=res_label;

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
                            res[lenofres++]=all_edges[j];
                            res[lenofres++]=all_edges[k];
                            res[lenofres++]=res_label;
                        }
                    }
                }
                //旧边在前，新边在后
                if(old_f_length>0&&new_b_length>0){
                    int j_end=old_f_index_list[f];
                    int k_end=new_b_index_list[b];
                    for(int j=old_f_index_list[f]-old_f_length+1;j<=j_end;j++){
                        for(int k=new_b_index_list[b]-new_b_length+1;k<=k_end;k++){
                            res[lenofres++]=all_edges[j];
                            res[lenofres++]=all_edges[k];
                            res[lenofres++]=res_label;
                        }
                    }
                }
            }
        }
//        System.out.println(tmp);
        int[] final_res=new int[lenofres];
        System.arraycopy(res,0,final_res,0,lenofres);
        return final_res;
    }


    public static  List<int[]> join_fully_compressed_directadd(int flag, int[] all_edges, int[] index_list ,
                                                                       int[][] grammar, int symbol_num,
                                                                     int[][] directadd0) {

        List<int[]> res_edges = new ArrayList<int[]>();
        Map<Integer, Integer> directadd = new HashMap<Integer, Integer>();
        for (int[] i : directadd0) directadd.put(i[0], i[1]);
        int index_len = index_list.length;
        if (index_list[index_len - 1] == -1) return res_edges;
        int old_layer = index_len / (symbol_num * 2) - 1;
        int new_edges_start;
        int new_index_start = index_len - symbol_num * 2;
        if (index_len == symbol_num * 2) new_edges_start = 0;
        else new_edges_start = index_list[new_index_start - 1] + 1;

        for (int[] gram : grammar) {
            int f = gram[0];
            int b = gram[1];
            int res_label = gram[2];

            /**
             * 判断f和b谁长，决定以谁为主forloop，避免空转
             */
            int new_f_length = -1;
            int new_b_length = -1;
            if (f == 0) {
                new_f_length = index_list[new_index_start + f] - new_edges_start + 1;
            } else {
                new_f_length = index_list[new_index_start + f] - index_list[new_index_start + f - 1];
            }
            new_b_length = index_list[new_index_start + b + symbol_num] -
                    index_list[new_index_start + b + symbol_num - 1];

            if (directadd.containsKey(res_label)) {//需添加额外label
                int directadd_symbol = directadd.get(res_label);
                /**
                 * 1、新边之间的两两连接
                 */
                if (new_f_length > 0 && new_b_length > 0) {
                    int j_end = index_list[new_index_start + f];
                    int k_end = index_list[new_index_start + b + symbol_num];
                    for (int j = index_list[new_index_start + f] - new_f_length + 1; j <= j_end; j++) {
                        for (int k = index_list[new_index_start + b + symbol_num] - new_b_length + 1; k <= k_end; k++) {
                            int[] ele = {all_edges[j], all_edges[k], res_label};
                            res_edges.add(ele);

                            int[] ele_add = {all_edges[j], all_edges[k], directadd_symbol};
                            res_edges.add(ele_add);
                        }
                    }
                }
                /**
                 * 新边与旧边之间的两两连接
                 */
                //新边在前，旧边在后
                if (new_f_length > 0) {
                    for (int i = 0; i < old_layer; i++) {
                        int old_index_start = i * symbol_num * 2;
                        int old_b_length = index_list[old_index_start + b + symbol_num]
                                - index_list[old_index_start + b + symbol_num - 1];
                        int j_end = index_list[new_index_start + f];
                        int k_end = index_list[old_index_start + b + symbol_num];
                        for (int j = index_list[new_index_start + f] - new_f_length + 1; j <= j_end; j++) {
                            for (int k = index_list[old_index_start + b + symbol_num] - old_b_length + 1; k <= k_end; k++) {
                                int[] ele = {all_edges[j], all_edges[k], res_label};
                                res_edges.add(ele);

                                int[] ele_add = {all_edges[j], all_edges[k], directadd_symbol};
                                res_edges.add(ele_add);
                            }
                        }
                    }
                }
                //旧边在前，新边在后
                if (new_b_length > 0) {
                    for (int i = 0; i < old_layer; i++) {
                        int old_index_start = i * symbol_num * 2;
                        int old_f_length;
                        if (i == 0 && f == 0) old_f_length = index_list[0] + 1;
                        else old_f_length = index_list[old_index_start + f] - index_list[old_index_start + f - 1];
                        int j_end = index_list[old_index_start + f];
                        int k_end = index_list[new_index_start + b + symbol_num];
                        for (int j = index_list[old_index_start + f] - old_f_length + 1; j <= j_end; j++) {
                            for (int k = index_list[new_index_start + b + symbol_num] - new_b_length + 1; k <= k_end; k++) {
                                int[] ele = {all_edges[j], all_edges[k], res_label};
                                res_edges.add(ele);

                                int[] ele_add = {all_edges[j], all_edges[k], directadd_symbol};
                                res_edges.add(ele_add);
                            }
                        }
                    }
                }
            }
            else {//仅添加当前label
                /**
                 * 1、新边之间的两两连接
                 */
                if (new_f_length > 0 && new_b_length > 0) {
                    int j_end = index_list[new_index_start + f];
                    int k_end = index_list[new_index_start + b + symbol_num];
                    for (int j = index_list[new_index_start + f] - new_f_length + 1; j <= j_end; j++) {
                        for (int k = index_list[new_index_start + b + symbol_num] - new_b_length + 1; k <= k_end; k++) {
                            int[] ele = {all_edges[j], all_edges[k], res_label};
                            res_edges.add(ele);
                        }
                    }
                }
                /**
                 * 新边与旧边之间的两两连接
                 */
                //新边在前，旧边在后
                if (new_f_length > 0) {
                    for (int i = 0; i < old_layer; i++) {
                        int old_index_start = i * symbol_num * 2;
                        int old_b_length = index_list[old_index_start + b + symbol_num]
                                - index_list[old_index_start + b + symbol_num - 1];
                        int j_end = index_list[new_index_start + f];
                        int k_end = index_list[old_index_start + b + symbol_num];
                        for (int j = index_list[new_index_start + f] - new_f_length + 1; j <= j_end; j++) {
                            for (int k = index_list[old_index_start + b + symbol_num] - old_b_length + 1; k <= k_end; k++) {
                                int[] ele = {all_edges[j], all_edges[k], res_label};
                                res_edges.add(ele);
                            }
                        }
                    }
                }
                //旧边在前，新边在后
                if (new_b_length > 0) {
                    for (int i = 0; i < old_layer; i++) {
                        int old_index_start = i * symbol_num * 2;
                        int old_f_length;
                        if (i == 0 && f == 0) old_f_length = index_list[0] + 1;
                        else old_f_length = index_list[old_index_start + f] - index_list[old_index_start + f - 1];
                        int j_end = index_list[old_index_start + f];
                        int k_end = index_list[new_index_start + b + symbol_num];
                        for (int j = index_list[old_index_start + f] - old_f_length + 1; j <= j_end; j++) {
                            for (int k = index_list[new_index_start + b + symbol_num] - new_b_length + 1; k <= k_end; k++) {
                                int[] ele = {all_edges[j], all_edges[k], res_label};
                                res_edges.add(ele);
                            }
                        }
                    }
                }
            }
        }
        return res_edges;
    }
}

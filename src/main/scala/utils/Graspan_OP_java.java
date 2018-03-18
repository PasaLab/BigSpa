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


}

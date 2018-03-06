package utils;

import java.util.*;


/**
 * Created by cycy on 2018/3/4.
 */

class IntArrayCompartor implements Comparator
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

public class Graspan_OP_java {
    public static int[][] get_symbol_index_range(List<int[]> list,int symbol_num){
        int[][] result=new int[symbol_num][2];
        int[] default_res={-1,-1};
        for(int i=0;i<symbol_num;i++){
            result[i]=new int[2];
            result[i][0]=-1;
            result[i][1]=-1;
        }
        if(list.size()==0) return result;

//        System.out.println("in get range index");
//        for(int[] i:array) System.out.println("("+i[0]+","+i[1]+")");
        int currentsymbol=list.get(0)[0];
        result[currentsymbol][0]=0;
        int i=1;
        int size=list.size();
        for(;i<size;i++){
            if(list.get(i)[0]!=currentsymbol){
                result[currentsymbol][1]=i-1;
                currentsymbol=list.get(i)[0];
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
    public static List<int[]> join_flat(int flag,int[][] old_array,int[][] new_array,int[][] grammar,int symbol_num){
        String tmp="XXX mid= "+flag+"\n";
//        for(int[] i:new_array){
//            tmp+=" ("+i[0]+","+i[1]+") ";
//        }
        double t0=System.nanoTime();
        List<int[]> res_edges=new ArrayList<int[]>();
        List<int[]> new_edge_before=new ArrayList<int[]>();
        List<int[]> new_edge_after=new ArrayList<int[]>();
        List<int[]> old_edge_before=new ArrayList<int[]>();
        List<int[]> old_edge_after=new ArrayList<int[]>();
        for(int[] ele :new_array){
            if(ele[1]==flag) {
                int a[]={ele[2],ele[0]};
                new_edge_before.add(a);
            }
            if(ele[0]==flag){
                int a[]={ele[2],ele[1]};
                new_edge_after.add(a);
            }
        }
        for(int[] ele:old_array){
            if(ele[1]==flag){
                int a[]={ele[2],ele[0]};
                old_edge_before.add(a);
            }
            if(ele[0]==flag){
                int a[]={ele[2],ele[1]};
                old_edge_after.add(a);
            }
        }
        IntArrayCompartor intarraycomparator = new IntArrayCompartor();
//        tmp+="\nedge_before before sort\n";
//        for(int[] i:new_edge_before){
//            tmp+=" ("+i[0]+","+i[1]+") ";
//        }
//        tmp+="\nedge_after before sort\n";
//        for(int[] i:new_edge_after){
//            tmp+=" ("+i[0]+","+i[1]+") ";
//        }
        Collections.sort(new_edge_before,intarraycomparator);
        Collections.sort(new_edge_after,intarraycomparator);
        Collections.sort(old_edge_before,intarraycomparator);
        Collections.sort(old_edge_after,intarraycomparator);
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

        tmp+="\nedge_before symbol index\n";
        for(int[] i:new_edge_before_symbol_index_range){
            tmp+=" ("+i[0]+","+i[1]+") ";
        }
        tmp+="\nedge_after symbol index\n";
        for(int[] i:new_edge_after_symbol_index_range){
            tmp+=" ("+i[0]+","+i[1]+") ";
        }
        tmp+="\nold_before symbol index\n";
        for(int[] i:old_edge_before_symbol_index_range){
            tmp+=" ("+i[0]+","+i[1]+") ";
        }
        tmp+="\nold_after symbol index\n";
        for(int[] i:old_edge_after_symbol_index_range){
            tmp+=" ("+i[0]+","+i[1]+") ";
        }

        for(int[] gram:grammar){
            int f=gram[0];
            int b=gram[1];
            int res_label=gram[2];
            /**
             * 1、新边之间的两两连接
             */
            if(new_edge_before_symbol_index_range[f][0]!=-1&&new_edge_after_symbol_index_range[b][0]!=-1) {
                for (int j = new_edge_before_symbol_index_range[f][0]; j <= new_edge_before_symbol_index_range[f][1]; j++) {
                    for (int k = new_edge_after_symbol_index_range[b][0]; k <= new_edge_after_symbol_index_range[b][1]; k++) {
                        int[] ele = new int[3];
                        ele[0] = new_edge_before.get(j)[1];
                        ele[1] = new_edge_after.get(k)[1];
                        ele[2] = res_label;
                        res_edges.add(ele);
                    }
                }
            }
            /**
             * 新边与旧边之间的两两连接
             */
            //新边在前，旧边在后
            if(new_edge_before_symbol_index_range[f][0]!=-1&&old_edge_after_symbol_index_range[b][0]!=-1){
                for(int j=new_edge_before_symbol_index_range[f][0];j<=new_edge_before_symbol_index_range[f][1];j++){
                    for(int k=old_edge_after_symbol_index_range[b][0];k<=old_edge_after_symbol_index_range[b][1];
                        k++){
                        int[] ele=new int[3];
                        ele[0]=new_edge_before.get(j)[1];
                        ele[1]=old_edge_after.get(k)[1];
                        ele[2]=res_label;
                        res_edges.add(ele);
                    }
                }
            }
            //旧边在前，新边在后
            if(old_edge_before_symbol_index_range[f][0]!=-1&&new_edge_after_symbol_index_range[b][0]!=-1){
                for(int j=old_edge_before_symbol_index_range[f][0];j<=old_edge_before_symbol_index_range[f][1];j++){
                    for(int k=new_edge_after_symbol_index_range[b][0];k<=new_edge_after_symbol_index_range[b][1];
                        k++){
                        int [] ele=new int[3];
                        ele[0]=old_edge_before.get(j)[1];
                        ele[1]=new_edge_after.get(k)[1];
                        ele[2]=res_label;
                        res_edges.add(ele);
                    }
                }
            }
        }
        System.out.println(tmp);
        return res_edges;
    }

}

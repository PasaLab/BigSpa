package ONLINE.utils_ONLINE;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static java.lang.System.out;
import static org.apache.avro.TypeEnum.a;
import static org.apache.hadoop.yarn.webapp.hamlet.HamletSpec.LinkType.index;
import static sun.misc.MessageUtils.out;

/**
 * Created by cycy on 2019/2/19.
 */
public class Node_Info {
    private int middle_node=-1;
    private HashMap<Integer,int[]> neighbours[]=null;
    public Node_Info(){
        middle_node=-1;
        neighbours=new HashMap[2];
    }
    public Node_Info(int mid_id,int symbol_num){
        middle_node=mid_id;
        neighbours=new HashMap[symbol_num*2];
    }

    public int getMiddle_node(){ return middle_node;}
    public HashMap<Integer,int[]>[] getNeighbours(){return neighbours;}
    /**
     *
     * @param vid
     * @param symbol
     * @param direction =0，f；=1，b
     * @param count_direction =0,直接计数；=1，三角计数
     * @param count
     * @return
     */
    public boolean add_count(int vid,int symbol,int direction,int count_direction,int count){
        try {
            int symbol_index=symbol * 2 + direction;
            if(neighbours[symbol_index]==null){
                neighbours[symbol_index]=new HashMap<>();
            }
            int[] count_array = neighbours[symbol_index].getOrDefault(vid,null);
            if(count_array==null) {
                count_array = new int[2];
            }
            count_array[count_direction]+=count;
            neighbours[symbol_index].put(vid, count_array);
            return true;
        }catch(Exception e){
            out.println("add_neighbour failed "+e);
            return false;
        }
    }


    /**
     *
     * @param vid
     * @param symbol
     * @param direction
     * @param sub_dir_count
     * @param sub_tri_count
     * @param remove_worklist
     */
    public void remove_count(int vid,int symbol,int direction,int sub_dir_count,int sub_tri_count,ArrayList<int[]>
            remove_worklist){
        int symbol_index=symbol * 2 + direction;
        int[] count_array=neighbours[symbol_index].get(vid);
        count_array[0]-=sub_dir_count;
        count_array[1]-=sub_tri_count;
        if(count_array[0]<=0&&count_array[1]<=0){
            neighbours[symbol_index].remove(vid);
            if(direction==0){//
                int[] edge_array={vid,middle_node,symbol};
                remove_worklist.add(edge_array);
            }
            else{
                int[] edge_array={middle_node,vid,symbol};
                remove_worklist.add(edge_array);
            }
        }
    }

    public String print(){
        StringBuilder sb=new StringBuilder();
        for(int i=0;i<neighbours.length/2;i++) {
            sb.append(i + " : ");
            sb.append("f[ ");
            if(neighbours[i * 2]!=null) {
                for (Map.Entry<Integer, int[]> entry : neighbours[i * 2].entrySet())
                    sb.append("(" + entry.getKey() + "," + intarray2string(entry.getValue()) + ")|");
            }
            else sb.append("null|");
            sb.append(" ] , b[ ");
            if(neighbours[i*2+1]!=null) {
                for (Map.Entry<Integer, int[]> entry : neighbours[i * 2 + 1].entrySet())
                    sb.append("(" + entry.getKey() + "," + intarray2string(entry.getValue()) + ")");
            }
            else sb.append("null|");
            sb.append(" ]");
        }
        return sb.toString();
    }

    public String intarray2string(int[] array){
        StringBuilder sb=new StringBuilder();
        sb.append("{");
        for(int a :array) sb.append(a+",");
        sb.append("}");
        return sb.toString();
    }

    public int calNeighboursTotalNum(){
        int ans=0;
        for(HashMap<Integer,int[]> m: neighbours){
            if(m!=null) ans+=m.size();
        }
        return ans;
    }
    public int calLabelEdgesMaximum(){
        int max=0;
        for(HashMap<Integer,int[]> m:neighbours){
            if(m!=null&&m.size()>max) max=m.size();
        }
        return max;
    }
}

import java.util.Arrays;

import static java.lang.System.out;

/**
 * Created by cycy on 2018/2/28.
 */

class TreeNode {
    int val;
    TreeNode left;
    TreeNode right;
    TreeNode(int x) { val = x; }
}

public class Solution {
    public static int find_index(int[] nums,int target,int f,int b){
        for(int i=f;i<=b;i++){
            if(nums[i]==target) return i;
        }
        return -1;
    }
    public static int recursion(TreeNode root,int root_index_pre,int b_pre,int[] preorder,int[] inorder,int f_ino,int
            b_ino){
//        out.println("root_index_pre: "+root_index_pre+", root= "+root.val);
        int root_index_ino=find_index(inorder,root.val,f_ino,b_ino);
        TreeNode l=null;
        TreeNode r=null;
        int left_lastindex_pre=root_index_pre;
        int left_last_val=root.val;
        if(root_index_ino!=-1&&f_ino<root_index_ino)//说明有左子树
        {
//            out.println(root.val+" 有左子树");
//            out.println("root_index_pre: "+root_index_pre+", root= "+root.val);
            int left_pre=root_index_pre+1;
            int left_val=preorder[left_pre];
            l=new TreeNode(left_val);
            left_last_val=recursion(l,left_pre,b_pre,preorder,inorder,f_ino,root_index_ino-1);
            left_lastindex_pre=find_index(preorder,left_last_val,left_pre,b_pre);
//            out.println("左子树最后的节点值为 "+left_last_val+", 在preorder中最后的位置为 "+left_lastindex_pre);
        }
        if(root_index_ino!=-1&&root_index_ino<b_ino)//说明有右子树
        {
//            out.println(root.val+" 有右子树");
            int right_pre=left_lastindex_pre+1;
            int right_val=preorder[right_pre];
            r=new TreeNode(right_val);
            left_last_val=recursion(r,right_pre,b_pre,preorder,inorder,root_index_ino+1,b_ino);
        }

        root.left=l;
        root.right=r;
//        out.print("root: "+root.val);
//        if(root.left!=null) out.print("\tleft: "+root.left.val);
//        else out.print("\tleft: null");
//        if(root.right!=null) out.print("\tright: "+root.right.val);
//        else out.print("\tright: null");
//        out.println();
        return left_last_val;
    }
    public static TreeNode buildTree(int[] preorder, int[] inorder){
        if(preorder.length==0) return null;
        TreeNode root=new TreeNode(preorder[0]);
        recursion(root,0,preorder.length-1,preorder,inorder,0,inorder.length-1);
        return root;
    }
    public static void main(String[] args){
        int len=100000;
        int[] array=new int[len];
        Arrays.fill(array,1);
        int tmp=0;
        double t0=System.nanoTime();

        t0=System.nanoTime();
        for(int i=0;i<len;i++){
            tmp+=array[i];
        }
        out.println("for array use "+(System.nanoTime()-t0)/1000000.0+" msecs");
        tmp=0;
        t0=System.nanoTime();
        int i=0;
        while(i<len){
            tmp+=array[i++];
        }
        out.println("while-loop array use "+(System.nanoTime()-t0)/1000000.0+" msecs");
    }
}

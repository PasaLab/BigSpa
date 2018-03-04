/**
 * Created by cycy on 2018/2/28.
 */

import static java.lang.System.out;

public class Solution {
    public static boolean search(int[] nums, int target) {
        if(nums.length==0) return false;
        int head=nums[0],tail=nums[nums.length-1];
        if(head==target||tail==target) return true;
        if(target>tail&&target<head) return false;

        boolean inadvance=false;
        if(target>head) inadvance=true;

        int f=0,b=nums.length;
        if(head==tail){
            while(f<nums.length&&nums[f]==head) f++;
            if(f==nums.length) return false;
            b--;
            while(b>=0&&nums[b]==tail) b--;
            b++;
        }
//        out.println("f: "+f+" , b: "+b);
        while(b>f+1){
            int mid=(b+f)/2;
            if(nums[mid]==target) return true;
            if(nums[mid]<target){
                if(inadvance) {
                    if(nums[mid]>=head) f=mid;
                    else if(nums[mid]<=tail) b=mid;
                }
                else f=mid;
            }
            else {
                if(inadvance) b=mid;
                else{
                    if(nums[mid]<=tail) b=mid;
                    else if(nums[mid]>=head) f=mid;
                }
            }
//            out.println("f: "+f+" , b: "+b);
        }
        if(nums[f]==target) return true;
        return false;
    }
    public static void main(String[] args){
        int[] nums={3,1};
        int target=0;
        out.println(search(nums,target));
    }
}

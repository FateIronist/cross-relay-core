package top.fateironist.cross_relay_core;


import java.util.concurrent.PriorityBlockingQueue;

public class Test {
     public class ListNode {
         int val;
         ListNode next;
         ListNode() {}
         ListNode(int val) { this.val = val; }
        ListNode(int val, ListNode next) { this.val = val; this.next = next; }
     }

      public class TreeNode {
          int val;
          TreeNode left;
         TreeNode right;
         TreeNode() {}
         TreeNode(int val) { this.val = val; }
         TreeNode(int val, TreeNode left, TreeNode right) {
         this.val = val;
             this.left = left;
                this.right = right;
         }
     }

    class Solution {
        public int[] sortArray(int[] nums) {
            int[] res = new int[nums.length];

            for (int i = nums.length / 2 - 1; i >= 0; i--) {
                balance(nums, i, nums.length);
            }

            int len = nums.length;
            for (int i = 0; i < res.length; i++) {
                res[i] = nums[0];
                swap(nums, 0, --len);
                balance(nums, 0, len);
            }

            return res;
        }

        public void balance(int[] nums, int idx, int len) {
            if (idx >= len) return;


            int leftIdx = idx * 2 + 1;
            int rightIdx = idx * 2 + 2;
            int leftValue = leftIdx >= len ? nums[idx] : nums[leftIdx];
            int rightValue = rightIdx >= len ? nums[idx] : nums[rightIdx];

            int min = Math.min(nums[idx], Math.min(leftValue, rightValue));

            if (min == nums[idx]) return;

            if (min == leftValue) {
                swap(nums, idx, leftIdx);
                balance(nums, leftIdx, len);
            } else if (min == rightValue) {
                swap(nums, idx, rightIdx);
                balance(nums, rightIdx, len);
            }
        }


        public void swap(int[] nums, int l, int r) {
            int temp = nums[l];
            nums[l] = nums[r];
            nums[r] = temp;
        }


    }
}

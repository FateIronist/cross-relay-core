package top.fateironist.cross_relay_core;


import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.Promise;
import io.netty.util.concurrent.Future;

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
        public boolean isSubStructure(TreeNode A, TreeNode B) {
            if (A == null || B == null) {
                return false;
            }

            if (A.val == B.val && backTrack(A, B)) {
                return true;
            }

            return isSubStructure(A.left, B) || isSubStructure(A.right, B);
        }


        public boolean backTrack(TreeNode A, TreeNode B) {
            if (B == null) {
                return true;
            }

            if (A == null || A.val != B.val) {
                return false;
            }

            return backTrack(A.left, B.left) && backTrack(A.right, B.right);
        }
    }
}

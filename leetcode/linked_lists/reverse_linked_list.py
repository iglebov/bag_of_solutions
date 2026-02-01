from typing import Optional


class ListNode:
    def __init__(self, val=0, next=None):
        self.val = val
        self.next = next


class Solution:
    def reverseList(self, head: Optional[ListNode]) -> Optional[ListNode]:
        if not head:
            return None

        previous = None
        current = head
        while current:
            temp_node = current.next
            current.next = previous
            previous = current
            current = temp_node
        return previous

from collections import defaultdict
from typing import List


class Solution:
    def findAnagrams(self, s: str, p: str) -> List[int]:
        len_s, len_p = len(s), len(p)
        if len_p > len_s:
            return []

        result = []
        chars_difference = defaultdict(int)
        for char in p:
            chars_difference[char] -= 1

        for i in range(len_p - 1):
            if s[i] in chars_difference:
                chars_difference[s[i]] += 1

        for i in range(-1, len_s - len_p + 1):
            if i != -1 and s[i] in chars_difference:
                chars_difference[s[i]] -= 1
            if (i + len_p) < len_s and s[i + len_p] in chars_difference:
                chars_difference[s[i + len_p]] += 1
            if not any(chars_difference.values()):
                result.append(i + 1)
        return result

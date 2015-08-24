## Top N Algorithm Discussion

Recently when I learning recommendation system, the top-N algorithm maybe is the most frequent item I meet in all kinds of articles. Of course the algorithm used in recommendation system for top-N selection is more related to Maxtrix Factorization, while how to efficiently implement a top-n algorithm for normal case still a problem which we should notice.

### Problem Set up
 
Input: An array or a list depending on which language, with comparable N items filling in.
       Integer K which indicates how much elements your want to pick.
       
Output: The top K element.

### Discussion

Simply sort the array or list then select n elements will be the first solution coming into most people's mind. However, before solving this problem, there are several questions should be asked: How large for the original datasets? How many elements will you fetch? How kind of data you will process in this problem, batch or streaming? All these factor can dramatically influence the final decison on algorithm you should use. The naive solution will be sorting the list which will use O(NlogN) time. So can we do better than that?

### Random Selection Solution

Because the top n algorithm essencially is to find the kth element with anyone smaller or bigger than it. We could use some algorithm related to [kth order statistic](https://en.wikipedia.org/wiki/Order_statistic) like Selection Algoritthm. Random Selection algorithm provide the exatly the same thing we want. Linear time to locate element, using partition under the hood which automatically separate the elements smaller then kth element with others. If we want the return elements sorted, just sort those N elements and return. It is not hard to get the Running time for Random selection based algorithm: O(N), if required on sorted, O(N+KlogK). My implementaion in Java for this solution is [here](./RandomSelectionTopN.java). There is one case that can affect this algorithm's speed: K is large and required elements returned are sorted. In worst case, when k = N/2(if bigger than N/2, we can just flip the result), the running time will be O(NlogN) but with some small constant compared to other sorting algorithm.

### HeapSort Algorithm Solution

There are still some issues for above problem. One major one is the it assume the dataset is provided in batch. What if they comes in streamming which would be more possible case in production environment? HeapSort algorithm will fix this issue nicely. With maintaining a max-heap with just K elements, the top N will be directly solved by return a copy of the all elements in max-heap. If the dataset is in streamming style, the running time will be O(NlogK) in the worst case. If in a batch style, we can just use extra O(K) space complexity, then simulate the streamming data again. Actually the worst case can be narrowed down if we think carefully: the initial phase will be a heap building phase with a linear time complexity. Now the O(NlogK) is actually O((N-K)log(K)+K). Why's this useful? If the K is very close to N, then O((N-K)log(K)+K) is very close to O(N) which is a linear time solution. It will solve two problems that random selection algorithm can not: Dealing with streamming data and handle the case if K is close to N and need sorted. My Solution in this case is [here](./HeapSortTopN.java)

### Hadoop MapReduce Solution

What if the dataset is extremely big so main memory cannot fit? MapReduce will be used in this case: In map phase, we use above heapsort solution to find the top n for each Mapper because the recorder in each mapper class will emit the data in streamming. Then use one reducer to reduce those N-topers to extract the true N-top elements. My implementation for this solution is [here](./MRTopNDemo.java).Use _hadoop jar jarname full-qualified-classname -D N=4 input output_ to run the experiment.


### Conclusion

Same problem can have different opitmal solutions based on their assumption. As a programmer, wisely analysis on the problem case by case is very important in real senario. 
























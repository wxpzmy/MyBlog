package com.wxp.topn.demo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.PriorityQueue;

// Use max-heap to implement the top N algorithm for streaming data.
public class HeapTopN<T extends Comparable<T>>{
    
    private int n;
    PriorityQueue<T> maxheap;
    public HeapSortTopN(int n){
        this.n = n;
        this.maxheap = new PriorityQueue<T>(n, Collections.reverseOrder());
    }

    public void acceptNew(T input){
        if(maxheap.size() >= n){
            T top = maxheap.peek();
            if (top.compareTo(input) > 0){
                maxheap.remove();
                maxheap.add(input);
            }
        }else{
            maxheap.add(input);
        }
    }
    
    public List<T> fetchTopN(){
        List<T> ret = new ArrayList<>();
        while(!maxheap.isEmpty()){
            ret.add(maxheap.remove());
        }
        return ret;
    }
}

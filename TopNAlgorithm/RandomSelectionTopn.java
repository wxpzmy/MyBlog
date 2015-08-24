package com.wxp.topn.demo;
import java.util.Arrays;
import java.util.List;


// Generic top N algorithm selection implementation based on Random Selection
// Running time O(n)
public class RandomSelectionTopn{
        
    public <T extends Comparable<T>> List<T> topN(T[] arr, int N){
        randomSelection(arr, N, 0, arr.length-1);
        T[] ret = Arrays.copyOf(arr, N);
        return Arrays.asList(ret);
        
    }
    
    private <T extends Comparable<T>> int randomSelection(T[] arr, int nth, int start, int end){
        // random select a pivot
        int pivot = start + ((int) (Math.random()*(end-start+1)));
        int partition = partition(arr, start, end, pivot);
        int nthfound = partition - start + 1;
        if( nthfound == nth){
            // return if found 
            return partition;
        }else if( nthfound < nth){
            // if smaller then search the right side with some offset on original target
            return randomSelection(arr, nth-nthfound, partition + 1, end);
        }else{
            // if greater keep searching on the left part
            return randomSelection(arr, nth, start, partition - 1);
        }       
    }
    
    // basic same with quicksort    
    private <T extends Comparable<T>> int partition(T[] input, int start, int end, int pivot) {
        int left = start + 1;
        int right = end+1;
        swap(input, pivot, start);
        T cmp = input[start];
        while (left < right){
            if (input[left].compareTo(cmp) <= 0){
                left++;
            }else{
                right--;
                swap(input, left, right);
            }
        }
        swap(input, start, left-1);
        return left-1;
    }
    
    private <T extends Comparable<T>> void swap(T[] arr, int i, int j){
        T temp = arr[i];
        arr[i] = arr[j];
        arr[j] = temp;
    }

}

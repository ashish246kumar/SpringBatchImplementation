package com.lean.springBatch.springBatch.partion;

import java.util.Map;

import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;
import java.util.*;
public class ColumnRangePartitioner implements Partitioner{
	

	@Override
	public Map<String, ExecutionContext> partition(int gridSize) {
		
		int min=1;
		int max=1000;
		int targetSize=(max-min)/gridSize+1;
		Map<String,ExecutionContext>result=new HashMap<>();
		int number=0;
		int start=min;
		int end=start+targetSize-1;
		while(start<=max) {
			ExecutionContext value=new ExecutionContext();
			result.put("partition"+number, value);
			value.putInt("minValue",start);
			value.putInt("maxValue", end);
			
			start+=targetSize;
			end+=targetSize;
			number++;
			
		}
		System.out.println("partition result"+result.toString());	
		return result;
	}

}

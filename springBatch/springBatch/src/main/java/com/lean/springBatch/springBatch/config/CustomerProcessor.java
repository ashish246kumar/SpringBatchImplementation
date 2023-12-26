package com.lean.springBatch.springBatch.config;

import org.springframework.batch.item.ItemProcessor;

import com.lean.springBatch.springBatch.entity.Customer;

public class CustomerProcessor implements ItemProcessor<Customer,Customer> {

	@Override
	public Customer process(Customer customer) throws Exception {
		return customer;
	}
}

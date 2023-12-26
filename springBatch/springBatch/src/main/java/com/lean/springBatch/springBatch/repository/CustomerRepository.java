package com.lean.springBatch.springBatch.repository;

import org.springframework.data.jpa.repository.JpaRepository;

import com.lean.springBatch.springBatch.entity.Customer;

public interface CustomerRepository extends JpaRepository<Customer,Integer> {

}

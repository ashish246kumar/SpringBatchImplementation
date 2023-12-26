package com.lean.springBatch.springBatch.entity;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Entity
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Customer {

	@Id
	private int id;
	private String firstName;
	private String lastName;
	private String email;
	private String gender;
	private String contactNo;
	private String country;
	private String dob;
}

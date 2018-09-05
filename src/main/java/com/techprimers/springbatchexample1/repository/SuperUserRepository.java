package com.techprimers.springbatchexample1.repository;

import org.springframework.data.jpa.repository.JpaRepository;

import com.techprimers.springbatchexample1.model.SuperUser;

public interface SuperUserRepository extends JpaRepository<SuperUser, Integer> {
}

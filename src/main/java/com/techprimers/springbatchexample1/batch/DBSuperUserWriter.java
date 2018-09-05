package com.techprimers.springbatchexample1.batch;

import java.util.List;

import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.techprimers.springbatchexample1.model.SuperUser;
import com.techprimers.springbatchexample1.repository.SuperUserRepository;

@Component
public class DBSuperUserWriter implements ItemWriter<SuperUser> {

    @Autowired
    private SuperUserRepository repository;

    @Override
    public void write(List<? extends SuperUser> superUsers) throws Exception {

	System.out.println("Data Saved for SuperUsers: " + superUsers.size());
	repository.save(superUsers);
    }
}

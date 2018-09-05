package com.techprimers.springbatchexample1.batch;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

import com.techprimers.springbatchexample1.model.SuperUser;
import com.techprimers.springbatchexample1.model.User;

@Component
public class SuperProcessor implements ItemProcessor<User, SuperUser> {

    private static final Map<String, String> DEPT_NAMES = new HashMap<>();

    public SuperProcessor() {
	DEPT_NAMES.put("Technology", "Senior Technology");
	DEPT_NAMES.put("Operations", "Senior Operations");
	DEPT_NAMES.put("Accounts", "Senior Accounts");
    }

    @Override
    public SuperUser process(User user) throws Exception {
	System.out.println("itemProcessor called:" + user.getId());
	SuperUser superUser = new SuperUser();
	superUser.setId(user.getId() * 10);
	superUser.setName(user.getName());
	superUser.setSalary(user.getSalary());
	String dept = DEPT_NAMES.get(user.getDept());
	superUser.setDept(dept);
	superUser.setTime(new Date());
	return superUser;
    }
}

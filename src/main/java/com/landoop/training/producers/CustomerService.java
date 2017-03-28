package com.landoop.training.producers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CustomerService {

    // Pairs of username and id
    private Map<String, Integer> usersMap;

    public CustomerService() {
        usersMap = new HashMap<String, Integer>();
        usersMap.put("Tom", 0);
        usersMap.put("Mary", 1);
        usersMap.put("Alice", 2);
    }

    public Integer findUserId(String userName) {
        return usersMap.get(userName);
    }

    public List<String> findAllUsers() {
        return new ArrayList<String>(usersMap.keySet());
    }

}
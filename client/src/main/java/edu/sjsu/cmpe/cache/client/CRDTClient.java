package edu.sjsu.cmpe.cache.client;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;

public class CRDTClient {
	HashMap<String,DistributedCacheService> cache = new HashMap<String,DistributedCacheService>();
	private ArrayList<String> successServers;
	private static CountDownLatch latch;
	private ConcurrentHashMap<String, ArrayList<String>> dictResults;
	
	public CRDTClient(){
		System.out.println("Starting Distributed Cache Servers");
		DistributedCacheService C0 = new DistributedCacheService("http://localhost:3000",this);
		DistributedCacheService C1 = new DistributedCacheService("http://localhost:3001",this);
		DistributedCacheService C2 = new DistributedCacheService("http://localhost:3002",this);
		cache.put("http://localhost:3000",C0);
		cache.put("http://localhost:3001",C1);
		cache.put("http://localhost:3002",C2);
	}
	
	  public String get(long key) throws InterruptedException{
	    	dictResults = new ConcurrentHashMap<String, ArrayList<String>>();
	    	latch = new CountDownLatch(cache.size());
	    	System.out.println("Get Request for Key "+key);
	    	for(DistributedCacheService dcs : cache.values()){
	    		dcs.get(key);
	    	}
	    	latch.await();
	    	System.out.println("After Await");
	    	String rightValue = dictResults.keys().nextElement();
	    	if(dictResults.keySet().size() > 1 || dictResults.get(rightValue).size() != cache.size()){
	    		//All three servers did not return the same 
	    		//get the value with most servers.
	    		String mostFrequent;
	    		for(Map.Entry<String, ArrayList<String>> e : dictResults.entrySet()){
	    			int tempSize = 0;
	    			if(e.getValue().size() > tempSize){
	    				mostFrequent = e.getKey();
	    				tempSize = e.getValue().size();
	    				rightValue = mostFrequent;
	    			}
	    		}
	    		for(String repair : cache.keySet()){
	                System.out.println("repairing: " + repair + " value: " + rightValue);
	                DistributedCacheService server = cache.get(repair);
	                server.put(key, rightValue);
	    		}
	    	}
	    	return rightValue;
	    }
	  
    public void getFailed(Exception e) {
        System.out.println("The request has failed");
        latch.countDown();
    }
    
    
    public void getCompleted(HttpResponse<JsonNode> response, String serverUrl) {
    	System.out.println("Get Completed url "+serverUrl);
        String value = null;
        if (response != null && response.getStatus() == 200) {
            value = response.getBody().getObject().getString("value");
            System.out.println("value from server " + serverUrl + "is " + value);
            ArrayList valueServers = dictResults.get(value);
            if (valueServers == null) {
                valueServers = new ArrayList(3);
            }
            valueServers.add(serverUrl);
            dictResults.put(value, valueServers);
        }

        latch.countDown();
    }
  
	
	public boolean put(long key, String value) throws InterruptedException {
		System.out.println("PUT Request From Client for KEY = "+key+" Value = "+value);
		boolean isSuccess = false;
	    successServers = new ArrayList<String>(cache.size());
	      latch = new CountDownLatch(cache.size());
	
	      for (DistributedCacheService c : cache.values()) {
	          c.put(key, value);
	      }
	
	      latch.await();
	      if(successServers.size() >= (cache.size()-1)){
	        isSuccess = true;
	      }
	
	      if (! isSuccess) {
	          // Send delete for the same key
	          delete(key, value);
	      }
	    
        return isSuccess;
	}	  
	
    public void putCompleted(HttpResponse<JsonNode> response, String serverUrl) {
        int code = response.getStatus();
        successServers.add(serverUrl);
        latch.countDown();
    }
    
    public void putFailed(Exception e) {
        System.out.println("The request has failed");
        latch.countDown();
    }
    
	public void delete(long key, String value) {
		for (final String serverUrl : successServers) {
	        DistributedCacheService server = cache.get(serverUrl);
	        server.delete(key);
	    }
	}
    
}

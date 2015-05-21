package com.disrupt.deal;

import java.util.ArrayList;

import com.disrupt.deal.businesslogic.MatchingEngine;

public class DealEvent
{
    private String dealJSON;
    private String dealMsg;
    private DealEntity dealEntity;
    private ArrayList<DealEntity> dealMatched;
    private int matched = MatchingEngine.MatchType.NOTMATCHED.getValue();
    public ArrayList<String> handlersVisited = new ArrayList<String>();

    public void resetEvent()
    {
        this.dealEntity = null;
        this.matched = MatchingEngine.MatchType.NOTMATCHED.getValue();
        this.dealMatched = null;
        this.handlersVisited = new ArrayList<String>();
        
    }

    public void setDealJSON(String JSON)
    {
        this.dealJSON = JSON;
    }
    
    public String getDealJSON() {
    	return this.dealJSON;
    }
    
    public void setMatched(int matched)
    {
        this.matched = matched;
    }
    
    public int getMatched() {
    	return this.matched;
    }
    
    public void setDealMsg(String msg)
    {
        this.dealMsg = msg;
    }
    
    public String getDealMsg() {
    	return this.dealMsg;
    }

    public void setDealEntity(DealEntity dealEntity) {
    	this.dealEntity = dealEntity;
    }
    public DealEntity getDealEntity() {
    	return this.dealEntity;
    }
    
    public void setDealMatched(ArrayList<DealEntity> dealMatched) {
    	this.dealMatched = dealMatched;
    }
    public ArrayList<DealEntity> getDealMatched() {
    	return this.dealMatched;
    }}
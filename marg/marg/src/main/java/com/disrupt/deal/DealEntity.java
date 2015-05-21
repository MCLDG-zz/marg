package com.disrupt.deal;

public class DealEntity {

	private String dealJSON = null;

	private long dealID;
	private long sequence;
	private String transactionType = null;
	private String contractName = null;
	private float lots;
	private String CCY1 = null;
	private double CCY1Amount;
	private String CCY2 = null;
	private double CCY2Amount;
	private boolean processed;

	public DealEntity(long sequence, long dealID, String transactionType, String contractName,
			float lots, String CCY1, double CCY1Amount, String CCY2,
			double CCY2Amount) {
		this.dealID = dealID;
		this.sequence = sequence;
		this.transactionType = transactionType;
		this.contractName = contractName;
		this.lots = lots;
		this.CCY1 = CCY1;
		this.CCY2 = CCY2;
		this.CCY1Amount = CCY1Amount;
		this.CCY2Amount = CCY2Amount;
	}

	String getDealJSON() {
		return dealJSON;
	}

	void setDealJSON(String dealJSON) {
		this.dealJSON = dealJSON;
	}

	public long getDealID() {
		return dealID;
	}

	void setDealID(long dealID) {
		this.dealID = dealID;
	}

	public long getSequence() {
		return sequence;
	}

	void setSequence(long sequence) {
		this.sequence = sequence;
	}

	public String getTransactionType() {
		return transactionType;
	}

	void setTransactionType(String transactionType) {
		this.transactionType = transactionType;
	}

	public String getContractName() {
		return contractName;
	}

	void setContractName(String contractName) {
		this.contractName = contractName;
	}

	public float getLots() {
		return lots;
	}
	/*
	 * Public because the number of lots may be updated during a partial match
	 */
	public void setLots(float lots) {
		this.lots = lots;
	}

	public String getCCY1() {
		return CCY1;
	}

	void setCCY1(String CCY1) {
		this.CCY1 = CCY1;
	}

	public double getCCY1Amount() {
		return CCY1Amount;
	}

	void setCCY1Amount(double CCY1Amount) {
		this.CCY1Amount = CCY1Amount;
	}

	public String getCCY2() {
		return CCY2;
	}

	void setCCY2(String CCY2) {
		this.CCY2 = CCY2;
	}

	public double getCCY2Amount() {
		return CCY2Amount;
	}

	void setCCY2Amount(double CCY2Amount) {
		this.CCY2Amount = CCY2Amount;
	}
	public boolean getProcessed() {
		return processed;
	}

	void setProcessed(boolean processed) {
		this.processed = processed;
	}
}

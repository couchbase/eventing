package main

type creditScoreBlob struct {
	CreditCardCount  int    `json:"credit_card_count"`
	CreditLimitUsed  int    `json:"credit_limit_used"`
	CreditScore      int    `json:"credit_score"`
	MissedEMIs       int    `json:"missed_emi_payments"`
	SSN              int    `json:"ssn"`
	TotalCreditLimit int    `json:"total_credit_limit"`
	Type             string `json:"type"`
}

type travelSampleBlob struct {
	Type        string `json:"type"`
	ID          int    `json:"doc_id"`
	Source      string `json:"src"`
	Destination string `json:"dst"`
}

type cpuOpBlob struct {
	Type string `json:"type"`
	ID   int    `json:"doc_id"`
}

type docTimerBlob struct {
	Type string `json:"type"`
	ID   int    `json:"doc_id"`
}

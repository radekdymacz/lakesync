// ---------------------------------------------------------------------------
// Salesforce Connector — Type Definitions
// ---------------------------------------------------------------------------

/** Connection configuration for a Salesforce CRM source. */
export interface SalesforceConnectorConfig {
	/** Salesforce instance URL (e.g. "https://mycompany.salesforce.com"). */
	instanceUrl: string;
	/** Connected App consumer key. */
	clientId: string;
	/** Connected App consumer secret. */
	clientSecret: string;
	/** Salesforce username. */
	username: string;
	/** Salesforce password + security token concatenated. */
	password: string;
	/** REST API version (default "v62.0"). */
	apiVersion?: string;
	/** Use test.salesforce.com for auth (default false). */
	isSandbox?: boolean;
	/** Optional WHERE clause fragment appended to all SOQL queries. */
	soqlFilter?: string;
	/** Whether to include Account objects (default true). */
	includeAccounts?: boolean;
	/** Whether to include Contact objects (default true). */
	includeContacts?: boolean;
	/** Whether to include Opportunity objects (default true). */
	includeOpportunities?: boolean;
	/** Whether to include Lead objects (default true). */
	includeLeads?: boolean;
}

/** Ingest configuration for the Salesforce poller. */
export interface SalesforceIngestConfig {
	/** Poll interval in milliseconds (default 30 000). */
	intervalMs?: number;
}

// ---------------------------------------------------------------------------
// Salesforce REST API — Response Types
// ---------------------------------------------------------------------------

/** OAuth 2.0 token response from the Salesforce token endpoint. */
export interface SalesforceAuthResponse {
	access_token: string;
	instance_url: string;
	token_type: string;
	issued_at: string;
	signature: string;
}

/** SOQL query response envelope. */
export interface SalesforceQueryResponse<T> {
	totalSize: number;
	done: boolean;
	nextRecordsUrl?: string;
	records: T[];
}

/** Salesforce Account sObject (fields used by the connector). */
export interface SfAccount {
	Id: string;
	Name: string | null;
	Type: string | null;
	Industry: string | null;
	Website: string | null;
	Phone: string | null;
	BillingCity: string | null;
	BillingState: string | null;
	BillingCountry: string | null;
	AnnualRevenue: number | null;
	NumberOfEmployees: number | null;
	Owner: { Name: string } | null;
	CreatedDate: string | null;
	LastModifiedDate: string | null;
}

/** Salesforce Contact sObject (fields used by the connector). */
export interface SfContact {
	Id: string;
	FirstName: string | null;
	LastName: string | null;
	Email: string | null;
	Phone: string | null;
	Title: string | null;
	AccountId: string | null;
	Account: { Name: string } | null;
	MailingCity: string | null;
	MailingState: string | null;
	MailingCountry: string | null;
	Owner: { Name: string } | null;
	CreatedDate: string | null;
	LastModifiedDate: string | null;
}

/** Salesforce Opportunity sObject (fields used by the connector). */
export interface SfOpportunity {
	Id: string;
	Name: string | null;
	StageName: string | null;
	Amount: number | null;
	CloseDate: string | null;
	Probability: number | null;
	AccountId: string | null;
	Account: { Name: string } | null;
	Type: string | null;
	LeadSource: string | null;
	IsClosed: boolean | null;
	IsWon: boolean | null;
	Owner: { Name: string } | null;
	CreatedDate: string | null;
	LastModifiedDate: string | null;
}

/** Salesforce Lead sObject (fields used by the connector). */
export interface SfLead {
	Id: string;
	FirstName: string | null;
	LastName: string | null;
	Company: string | null;
	Email: string | null;
	Phone: string | null;
	Title: string | null;
	Status: string | null;
	LeadSource: string | null;
	IsConverted: boolean | null;
	ConvertedAccountId: string | null;
	ConvertedContactId: string | null;
	ConvertedOpportunityId: string | null;
	Owner: { Name: string } | null;
	CreatedDate: string | null;
	LastModifiedDate: string | null;
}

package equi_genea_broker_client

type AccountCreationEvent struct {
	AccountID string
	Password  string
}

type AccountActivityEvent struct {
	AccountID string
}

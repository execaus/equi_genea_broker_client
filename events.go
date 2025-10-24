package equi_genea_broker_client

type AccountCreationEvent struct {
	Email    string
	Password string
}

type AccountActivityEvent struct {
	AccountID string
}

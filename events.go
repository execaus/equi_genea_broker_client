package equi_genea_broker_producer

type AccountCreationEvent struct {
	AccountID string
	Password  string
}

type AccountActivityEvent struct {
	AccountID string
}

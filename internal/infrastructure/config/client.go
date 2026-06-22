package config

type ClientCFG struct {
	ClientID    string
	Addres      string
	Interactive bool
}

func Client() (ClientCFG, error) {
	cfg, err := Load()
	if err != nil {
		return ClientCFG{}, err
	}
	return cfg.Client, nil
}

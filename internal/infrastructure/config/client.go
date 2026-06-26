package config

type ClientCFG struct {
	ClientID    string
	Address     string
	Interactive bool
	Ssh         SSH
}

type SSH struct {
	User                  string
	Host                  string
	Port                  string
	Password              string
	KeyFile               string
	InsecureIgnoreHostKey bool
	RemoteAddress         string
}

func Client() (ClientCFG, error) {
	cfg, err := Load()
	if err != nil {
		return ClientCFG{}, err
	}
	return cfg.Client, nil
}

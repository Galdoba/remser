package infrastructure

import "github.com/Galdoba/appcontext/xdg"

const (
	AppName       = "remser"
	ClientAppName = AppName + "c"
	ServerAppName = AppName + "d"
	Version       = "v 0.0.0"
)

func ClientConfigPath() string {
	return xdg.Location(xdg.ForConfig(), xdg.WithProgramName(AppName), xdg.WithFileName(ClientConfigPath()+".config"))
}

func ServerConfigPath() string {
	return xdg.Location(xdg.ForConfig(), xdg.WithProgramName(AppName), xdg.WithFileName(ServerAppName+".config"))
}

package appconfig

import (
	"os"
	"strconv"

	"github.com/joho/godotenv"
)

// AppConfig contains config settings
type AppConfig struct {
	URI         string
	DBName      string
	Debug       bool
	Connections int
	GoRoutines  int
}

// ConfigData contains application configuration settings read from a JSON formatted file.
var ConfigData AppConfig

func LoadAppConfig() {

	//The following will try to load environment variables from a .env file in the application root folder
	//if they have not been defined in the system
	godotenv.Load()

	ConfigData.URI = os.Getenv("MONGODB_URI")
	ConfigData.DBName = os.Getenv("MONGODB_DB_NAME")
	ConfigData.Debug, _ = strconv.ParseBool(os.Getenv("MONGODB_DEBUG"))
	ConfigData.Connections, _ = strconv.Atoi(os.Getenv("MONGODB_CONNECTIONS"))
	ConfigData.GoRoutines, _ = strconv.Atoi(os.Getenv("MONGODB_GOROUTINES"))
}

package main

import (
	"github.com/mongodb-developer/dotLocalPopQuiz/appconfig"
	"github.com/mongodb-developer/dotLocalPopQuiz/documentsize"
	"github.com/mongodb-developer/dotLocalPopQuiz/hierarchy"
)

func main() {

	//The following will load environment variables from a .env file in the application root folder if one exists.
	appconfig.LoadAppConfig()
	documentsize.LoadQ1Data()
	documentsize.LoadQ2Data()
	hierarchy.LoadQ3Data()
}

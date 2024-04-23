# dotLocalPopQuiz
Code to generate the sample data and metrics used in the "Pop Quiz: Optimizing Data Designs for High Performance" presentation at .Local NYC 2024

The code reads environemnt variables with the target MongoDB database name (MONGODB_URI) and connection URI (MONGODB_DB_NAME). These can optionally be defined in a .env file in the application root directory.

The code was executed on a Mac Powerbook M1 Pro against a locally installed MongoDB Instance, and on a AWS EC2 T2.xLarge Ubuntu instance targetting a 3-Node MongoDB Atlas M50 cluster with 250GB storage. All metrics used in the presentation were derived from testing on the Atlas M50 cluster.

If running in different environments, if any problems are encountered, try reducing the values of "connectionCount" and "routineCount" in Q1DtataSetup.go, Q2DataLoad.go and Q3HierarchDataLoad.go to reduce the number of concurrent threads loading data.

Prebuilt executables for Linux (arm64 and amd64), Mac (arm64 and amd64) and Windows (amd64) are provided.
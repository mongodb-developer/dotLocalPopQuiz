package documentsize

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/mongodb-developer/dotLocalPopQuiz/appconfig"
	"github.com/mongodb-developer/dotLocalPopQuiz/utils"

	"go.mongodb.org/mongo-driver/bson"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
)

func LoadQ2Data() {

	mongoDB := utils.GetMongoDatabase(appconfig.ConfigData.URI, appconfig.ConfigData.DBName)
	defer func() {
		if err := mongoDB.Client().Disconnect(context.TODO()); err != nil {
			log.Fatal(err)
		}
	}()

	//Make sure profiling is dasabled after we are finished
	defer utils.DisableProfiling(mongoDB)

	//Run the data loads

	//########################################################
	//The individual doc inserts are fast enough we can run them using a single connection / single thread
	utils.ResetProfiling(mongoDB, "MDBPerformancePopQuiz.Q1Individual", "insert")
	runQ2IndividualDataLoads(mongoDB)

	//########################################################

	//For the bucket loads, we'll use multiple connections / threads for performance

	mongoDBURI := os.Getenv("MONGODB_URI")
	mongoDBName := os.Getenv("MONGODB_DB_NAME")

	connectionCount := 5
	routineCount := 5
	sensorCount := 1000

	//Create the connections for the bucket loads
	var connections []*mongo.Database
	for i := 0; i < connectionCount; i++ {
		db := utils.GetMongoDatabase(mongoDBURI, mongoDBName)
		connections = append(connections, db)
	}
	defer func() {
		for i := 0; i < connectionCount; i++ {
			if err := connections[i].Client().Disconnect(context.TODO()); err != nil {
				log.Fatal(err)
			}
		}
	}()

	//Work out the number of sensors readings to be handled by each connection.
	sensorsCount := sensorCount / connectionCount

	//Initialize a channel to receive consolidated performance data from each connection
	pDataChan := make(chan int)
	var pData int

	//############################################################
	//Do the minute bucketloads
	//Reset profiling
	utils.ResetProfiling(mongoDB, "MDBPerformancePopQuiz.Q1MinBucket", "update")
	//Run a search of the collection to load as much data as possible into cache
	_, err := mongoDB.Collection("Q1MinBucket").Find(context.TODO(), bson.D{{}})
	if err != nil {
		log.Fatal(err)
	}
	//Start a new Go Routine for each MDB connection
	startTime := time.Now()
	startID := 1
	for i := 0; i < connectionCount; i++ {
		go runQ2BucketDataLoads(connections[i], pDataChan, routineCount, startID, sensorsCount, false)
		startID += sensorsCount
	}
	for i := 0; i < connectionCount; i++ {
		conPData := <-pDataChan
		pData += conPData
	}
	endTime := time.Now()
	executionDuration := time.Since(startTime)
	mdbDuration := utils.GetProfilingData(mongoDB, "MDBPerformancePopQuiz.Q1MinBucket", "update", startTime)

	//Save the execution duration back to MongoDB
	var result utils.TestResult
	result.TestName = "Q2 Minute Buckets Data Load"
	result.StartTime = startTime
	result.EndTime = endTime
	//Duration is the end-to-end time observed by this client application
	//including network latency and command set-up etc
	result.Duration = int(executionDuration / time.Millisecond)
	//MongoDuration is the execution time reported by the server and what
	//we are mostly interested in.
	result.MongoDuration = int(mdbDuration)
	resultsColl := mongoDB.Collection("Q2Results")
	_, err = resultsColl.InsertOne(context.TODO(), result)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Q2 Minute Bucket Data Loads Server duration was %d ms", mdbDuration)
	log.Printf("Q2 Minute Bucket Data Loads Total duration was %d ms", int(executionDuration/time.Millisecond))

	//###########################################################
	//Do the hour bucket loads
	//Reset profiling
	utils.ResetProfiling(mongoDB, "MDBPerformancePopQuiz.Q1HourBucket", "update")
	//Run a search of the collection to load as much data as possible into cache
	_, err = mongoDB.Collection("Q1HourBucket").Find(context.TODO(), bson.D{{}})
	if err != nil {
		log.Fatal(err)
	}
	//Start a new Go Routine for each MDB connection
	pData = 0
	startTime = time.Now()
	startID = 1
	for i := 0; i < connectionCount; i++ {
		go runQ2BucketDataLoads(connections[i], pDataChan, appconfig.ConfigData.GoRoutines, startID, sensorsCount, true)
		startID += sensorsCount
	}
	for i := 0; i < connectionCount; i++ {
		conPData := <-pDataChan
		pData += conPData
	}
	endTime = time.Now()
	executionDuration = time.Since(startTime)
	mdbDuration = utils.GetProfilingData(mongoDB, "MDBPerformancePopQuiz.Q1HourBucket", "update", startTime)

	//Save the execution duration back to MongoDB
	result.TestName = "Q2 Hour Buckets Data Load"
	result.StartTime = startTime
	result.EndTime = endTime
	//Duration is the end-to-end time observed by this client application
	//including network latency and command set-up etc
	result.Duration = int(executionDuration / time.Millisecond)
	//MongoDuration is the execution time reported by the server and what
	//we are mostly interested in.
	result.MongoDuration = int(mdbDuration)
	_, err = resultsColl.InsertOne(context.TODO(), result)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Q2 Hour Bucket Data Loads Server duration was %d ms", mdbDuration)
	log.Printf("Q2 Hour Bucket Data Loads Total duration was %d ms", int(executionDuration/time.Millisecond))

	log.Print("Q2 Data Load Completed")

}

func runQ2BucketDataLoads(mongoDB *mongo.Database, masterChan chan (int), goRoutines, startID, sensorCount int, hourBuckets bool) {

	//Work out the number of sensors to be loaded by each goRoutine.
	routineSensorCount := sensorCount / goRoutines

	//Initialize a channel to receive performance data from each goroutine
	pDataChan := make(chan int)
	var pData int

	for i := 0; i < goRoutines; i++ {
		go insertQ2BucketData(mongoDB, pDataChan, startID, startID+routineSensorCount, hourBuckets)
		startID += routineSensorCount
	}
	for i := 0; i < goRoutines; i++ {
		conPData := <-pDataChan
		pData += conPData
	}
	masterChan <- pData

}

func insertQ2BucketData(mdb *mongo.Database, pDataChan chan (int), startID, endID int, hourBuckets bool) {

	//Use Write Concern 1 for faster performance. Not recommended for a production system
	wc := writeconcern.W1()
	var collOpts options.CollectionOptions
	collOpts.WriteConcern = wc
	var coll *mongo.Collection
	if hourBuckets {
		coll = mdb.Collection("Q1HourBucket", &collOpts)
	} else {
		coll = mdb.Collection("Q1MinBucket", &collOpts)
	}

	//Set the data start time to midnight on the current date
	currTime := time.Now()
	//readingtime will be incremented by a second on each iteration of our main loop
	readingTime := time.Date(currTime.Year(), currTime.Month(), currTime.Day(), 0, 0, 0, 0, time.UTC)
	//minuteTime will be incremented as we roll over each minute and used to set the bucket
	//date fields
	minuteTime := readingTime

	//Execution Duration will track the end to end duration from the application side (including
	//network latency etc)
	var executionDuration time.Duration

	//We're generating 1 hour of data for each sensor, with one reading per second.
	//That's 60 * 60 or 3600 readings per sensor
	for i := 0; i < 3600; i++ {

		sec := readingTime.Second()
		min := readingTime.Minute()
		if !hourBuckets && sec == 0 {
			minuteTime = readingTime
		}
		log.Printf("Updating data for minute %d, second %d", min, sec)

		for x := startID; x < endID; x++ {
			//One iteration for each sensor

			reading := utils.RandomDecimal(150.00, 175.00)

			//Minute Docs - use an upsert to add the new reading to the appropriate bucket document,
			//creating the bucket document if it does not exist
			filter := bson.D{
				{"sensorID", x},
				{"date", minuteTime},
			}
			readingDoc := bson.D{
				{"temperature", reading},
				{"sec", sec},
			}
			push := bson.D{
				{"readings", readingDoc},
			}
			updateCmd := bson.D{
				{"$push", push},
				{"$setOnInsert", filter},
			}
			var opts options.UpdateOptions
			opts.SetUpsert(true)
			executionStart := time.Now()
			_, err := coll.UpdateOne(context.TODO(), filter, updateCmd, &opts)
			if err != nil {
				log.Fatal(err)
			}
			executionDuration += time.Since(executionStart)
		}

		//Increment time tracking
		readingTime = readingTime.Add(time.Second * 1)
	}
	//Return the total executon time in milliseconds
	pDataChan <- int(executionDuration / time.Millisecond)

}

func runQ2IndividualDataLoads(mongoDB *mongo.Database) {

	//Run the data load
	startTime := time.Now()
	executionTime := insertQ2IndividualData(mongoDB)
	endTime := time.Now()

	mdbDuration := utils.GetProfilingData(mongoDB, "MDBPerformancePopQuiz.Q1Individual", "insert", startTime)

	//Save the execution duration back to MongoDB
	var result utils.TestResult
	result.TestName = "Q2 Individual Data Load"
	result.StartTime = startTime
	result.EndTime = endTime
	//Duration is the end-to-end time observed by this client application
	//including network latency and command set-up etc
	result.Duration = executionTime
	//MongoDuration is the execution time reported by the server and what
	//we are mostly interested in.
	result.MongoDuration = int(mdbDuration)
	resultsColl := mongoDB.Collection("Q2Results")
	_, err := resultsColl.InsertOne(context.TODO(), result)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Q2 Inidividual Data Loads Server duration was %d ms", mdbDuration)
	log.Printf("Q2 Inidividual Data Loads Total duration was %d ms", executionTime)

}

func insertQ2IndividualData(mdb *mongo.Database) int {

	//Use Write Concern 1 for faster performance. Not recommended for a production system
	wc := writeconcern.W1()
	var collOpts options.CollectionOptions
	collOpts.WriteConcern = wc
	individualColl := mdb.Collection("Q1Individual", &collOpts)

	var insertOrderedOp options.InsertManyOptions

	//Set the data start time to midnight on the current date
	currTime := time.Now()
	batchTime := time.Date(currTime.Year(), currTime.Month(), currTime.Day(), 0, 0, 0, 0, time.UTC)
	var executionDuration time.Duration

	//We're generating 1 hour of data for each sensor, with one reading per second.
	//That's 60 * 60 or 3600 readings per sensor
	for i := 0; i < 3600; i++ {

		var individualDocs []interface{}

		for x := 1; x <= 1000; x++ {

			reading := utils.RandomDecimal(150.00, 175.00)

			//Individual Docs - for this exercise, we'll write a batch for all sensors each second (1000 docs per batch).
			var individualDoc bson.D
			individualDoc = append(individualDoc, bson.E{Key: "sensorID", Value: x})
			individualDoc = append(individualDoc, bson.E{Key: "temperature", Value: reading})
			individualDoc = append(individualDoc, bson.E{Key: "date", Value: currTime})
			individualDocs = append(individualDocs, individualDoc)
		}

		executionStart := time.Now()
		_, err := individualColl.InsertMany(context.TODO(), individualDocs, insertOrderedOp.SetOrdered(false))
		if err != nil {
			log.Fatal(err)
		}
		executionDuration += time.Since(executionStart)
		log.Printf("Individual document batch %d of 3600 written", i)

		//Don't do more than one batch per second.(Note - enabling this code means the load will take an actual hour)
		//for t := time.Now(); t.Before(currTime.Add(time.Second * 1)); t = time.Now() {
		//}

		//Increment time tracking
		currTime = time.Now()
		batchTime = batchTime.Add(time.Second * 1)
	}
	//Return the total executon time in milliseconds
	return int(executionDuration / time.Millisecond)

}

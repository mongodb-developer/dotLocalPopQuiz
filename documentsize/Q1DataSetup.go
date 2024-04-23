package documentsize

import (
	"context"

	"log"

	"sync"
	"time"

	"github.com/mongodb-developer/dotLocalPopQuiz/appconfig"
	"github.com/mongodb-developer/dotLocalPopQuiz/utils"
	"go.mongodb.org/mongo-driver/bson"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
)

func LoadQ1Data() {

	//This function loads the sample data for Q1 using multiple connections to MongoDB, each of which
	//runs multiple parallel goRoutines. The total number of threads loading data in parallel is
	//connectionCount * routineCount. "sensors" should be exactly divisible by the total number of
	//threads.

	//I ran this successfully using 5 connections, each running 5 threads on an Apple Macbook M1 Pro, and on
	//an AWS EC2 T2.xlarge Ubuntu instance loading into a MongoDB Atlas M50 3-node cluster with 250GB storage.

	mongoDBURI := appconfig.ConfigData.URI
	mongoDBName := appconfig.ConfigData.DBName

	connectionCount := 5
	routineCount := 5
	sensors := 1000

	//Work out the number of sensor readings to be loaded by each connection.
	sensorsCount := sensors / connectionCount

	//Create the necessary number of Mongo Client / Database connections
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

	//Initialize the master wait group
	utils.MasterWG.Add(connectionCount)

	//Create an array of sub-wait groups - one for each MDB connection
	var wgs []*sync.WaitGroup
	for i := 0; i < connectionCount; i++ {
		var wg sync.WaitGroup
		wgs = append(wgs, &wg)
	}

	//Drop the existing collections
	coll := connections[0].Collection("Q1Individual")
	coll.Drop(context.TODO())
	coll = connections[0].Collection("Q1HourBucket")
	coll.Drop(context.TODO())
	coll = connections[0].Collection("Q1MinBucket")
	coll.Drop(context.TODO())

	//Start a new Go Routine for each MDB connection
	startTime := time.Now()
	startID := 1
	for i := 0; i < connectionCount; i++ {
		go runDataLoads(connections[i], wgs[i], routineCount, startID, sensorsCount, startTime)
		startID += sensorsCount
	}
	utils.MasterWG.Wait()
	endTime := time.Now()

	//Save the execution duration back to MongoDB
	var result utils.TestResult
	result.TestName = "Q1 Data Load"
	result.StartTime = startTime
	result.EndTime = endTime
	result.Duration = int(endTime.UnixMilli() - startTime.UnixMilli())
	resultsColl := connections[0].Collection("Q1DataBuildResults")
	_, err := resultsColl.InsertOne(context.TODO(), result)
	if err != nil {
		log.Fatal(err)
	}
	log.Print("Data Load Completed - creating Indexes")
	utils.MasterWG.Add(3)
	indexModel := mongo.IndexModel{
		Keys: bson.D{
			{"sensorID", 1},
			{"date", 1},
		},
	}
	go utils.CreateIndex(connections[0].Collection("Q1Individual"), indexModel, &utils.MasterWG) //Might need to adjust context for this - looks like the call timed out.
	go utils.CreateIndex(connections[0].Collection("Q1MinBucket"), indexModel, &utils.MasterWG)
	go utils.CreateIndex(connections[0].Collection("Q1HourBucket"), indexModel, &utils.MasterWG)
	utils.MasterWG.Wait()

}

func insertData(mdb *mongo.Database, wg *sync.WaitGroup, startSensor, endSensor int, startTime time.Time) {

	defer wg.Done()

	//Use Write Concern 1 for faster performance. Not recommended for a production system
	wc := writeconcern.W1()
	var collOpts options.CollectionOptions
	collOpts.WriteConcern = wc
	individualColl := mdb.Collection("Q1Individual", &collOpts)
	hourColl := mdb.Collection("Q1HourBucket", &collOpts)
	minColl := mdb.Collection("Q1MinBucket", &collOpts)

	var insertOrderedOp options.InsertManyOptions

	//Set the data start time to 7 days prior
	midnight := time.Date(startTime.Year(), startTime.Month(), startTime.Day(), 0, 0, 0, 0, time.UTC) // Set time to midnight
	currTime := midnight.AddDate(0, 0, -7)

	var individualDocs []interface{}
	var hourDocs []interface{}
	var minDocs []interface{}

	//We're generating 7 days of data for each sensor, with one reading per second.
	//That's 7 * 24 * 60 * 60 or 604,800 readings per sensor

	priorDay := currTime.Day()
	priorHour := currTime.Hour()
	priorMin := currTime.Minute()

	for i := 0; i <= 604800; i++ {

		day := currTime.Day()
		hour := currTime.Hour()
		min := currTime.Minute()
		sec := currTime.Second()

		//If we clicked over a minute, write the prior minute documents
		if min != priorMin {

			for x := startSensor; x < endSensor; x++ {
				var minDoc bson.D
				minDoc = append(minDoc, bson.E{Key: "sensorID", Value: x})
				minDoc = append(minDoc, bson.E{Key: "date", Value: currTime.Add(time.Minute * -1)})
				minDoc = append(minDoc, bson.E{Key: "readings", Value: minDocs[x-startSensor].(bson.A)})
				minDocs[x-startSensor] = minDoc
			}
			_, err := minColl.InsertMany(context.TODO(), minDocs, insertOrderedOp.SetOrdered(false))
			if err != nil {
				log.Fatal(err)
			}
			minDocs = nil
			//log.Printf("Minute document batch written for day %d, hour %d, minute %d", priorDay, priorHour, priorMin)
		}

		//If we clicked over an hour, write the prior hour documents
		if hour != priorHour {

			for x := startSensor; x < endSensor; x++ {
				var hourDoc bson.D
				hourDoc = append(hourDoc, bson.E{Key: "sensorID", Value: x})
				hourDoc = append(hourDoc, bson.E{Key: "date", Value: currTime.Add(time.Hour * -1)})
				hourDoc = append(hourDoc, bson.E{Key: "readings", Value: hourDocs[x-startSensor].(bson.A)})
				hourDocs[x-startSensor] = hourDoc
			}
			_, err := hourColl.InsertMany(context.TODO(), hourDocs, insertOrderedOp.SetOrdered(false))
			if err != nil {
				log.Fatal(err)
			}
			hourDocs = nil
			log.Printf("Hour document batch written for day %d, hour %d", priorDay, priorHour)
		}

		if i < 604800 {

			for x := startSensor; x < endSensor; x++ {

				reading := utils.RandomDecimal(150.00, 175.00)

				//Handle Individual Docs - we'll write these in batches of 10,000
				var individualDoc bson.D
				individualDoc = append(individualDoc, bson.E{Key: "sensorID", Value: x})
				individualDoc = append(individualDoc, bson.E{Key: "temperature", Value: reading})
				individualDoc = append(individualDoc, bson.E{Key: "date", Value: currTime})
				individualDocs = append(individualDocs, individualDoc)
				if len(individualDocs) == 10000 || (i == 604799 && x == endSensor-1) {
					_, err := individualColl.InsertMany(context.TODO(), individualDocs, insertOrderedOp.SetOrdered(false))
					if err != nil {
						log.Fatal(err)
					}
					individualDocs = nil
				}

				//Handle Minute Docs
				var minDoc bson.D
				minDoc = append(minDoc, bson.E{Key: "temperature", Value: reading})
				minDoc = append(minDoc, bson.E{Key: "sec", Value: sec})
				var minReadings bson.A
				if len(minDocs) < ((x - startSensor) + 1) {
					//This is the first reading for this sensor this day
					minReadings = append(minReadings, minDoc)
					minDocs = append(minDocs, minReadings)
				} else {
					//We already have 1 or more reading for this sensor for this day
					minReadings = minDocs[x-startSensor].(bson.A)
					minReadings = append(minReadings, minDoc)
					minDocs[x-startSensor] = minReadings
				}

				//Handle Hour Docs
				var hourDoc bson.D
				hourDoc = append(hourDoc, bson.E{Key: "temperature", Value: reading})
				hourDoc = append(hourDoc, bson.E{Key: "min", Value: min})
				hourDoc = append(hourDoc, bson.E{Key: "sec", Value: sec})
				var hourReadings bson.A
				if len(hourDocs) < ((x - startSensor) + 1) {
					//This is the first reading for this sensor this hour
					hourReadings = append(hourReadings, hourDoc)
					hourDocs = append(hourDocs, hourReadings)
				} else {
					//We already have 1 or more reading for this sensor in this hour
					hourReadings = hourDocs[x-startSensor].(bson.A)
					hourReadings = append(hourReadings, hourDoc)
					hourDocs[x-startSensor] = hourReadings
				}

			}
		}
		//Increment time tracking
		priorDay = day
		priorHour = hour
		priorMin = min
		currTime = currTime.Add(time.Second * 1)
	}
}

func runDataLoads(mdb *mongo.Database, wg *sync.WaitGroup, goRoutines, startID, sensorCount int, startTime time.Time) {

	defer utils.MasterWG.Done()

	//Work out the number of sensors to be loaded by each goRoutine.
	routineSensorCount := sensorCount / goRoutines

	//Initialize the wait group
	wg.Add(goRoutines)

	for i := 0; i < goRoutines; i++ {
		go insertData(mdb, wg, startID, startID+routineSensorCount, startTime)
		startID += routineSensorCount
	}
	wg.Wait()

}

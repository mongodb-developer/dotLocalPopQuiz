package utils

import (
	"context"
	"time"

	"log"
	"math/rand"

	"fmt"
	"os"
	"strconv"
	"sync"
	"syscall"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var MasterWG sync.WaitGroup

type TestResult struct {
	TestName      string    `bson:"TestName"`
	StartTime     time.Time `bson:"StartTime"`
	EndTime       time.Time `bson:"EndTime"`
	Duration      int       `bson:"Duration"`
	MongoDuration int       `bson:"MongoDuration"`
}

func CheckPIDFile(pidFile string) error {
	// Read in the pid file as a slice of bytes.
	if piddata, err := os.ReadFile(pidFile); err == nil {
		// Convert the file contents to an integer.
		if pid, err := strconv.Atoi(string(piddata)); err == nil {
			// Look for the pid in the process list.
			if process, err := os.FindProcess(pid); err == nil {
				// Send the process a signal zero kill.
				if err := process.Signal(syscall.Signal(0)); err == nil {
					// We only get an error if the pid isn't running, or it's not ours.
					return fmt.Errorf("pid already running: %d", pid)
				}
			}
		}
	}
	// If we get here, then the pidfile didn't exist,
	// or the pid in it doesn't belong to the user running this app.
	return os.WriteFile(pidFile, []byte(fmt.Sprintf("%d", os.Getpid())), 0664)
}

func CreateIndex(coll *mongo.Collection, indexModel mongo.IndexModel, wg *sync.WaitGroup) {

	defer wg.Done()
	name, err := coll.Indexes().CreateOne(context.TODO(), indexModel)
	if err != nil {
		log.Fatal(err)
	}
	log.Print("Name of Index Created: " + name)

}

func GetMongoDatabase(mongoDBURI, mongoDBName string) *mongo.Database {

	client, err := mongo.Connect(context.TODO(), options.Client().ApplyURI(mongoDBURI).SetAppName("DotLocalPerformanceTest"))
	if err != nil {
		log.Fatal(err)
	}
	// Send a ping to confirm a successful connection
	client.Ping(context.TODO(), nil)
	if err := client.Ping(context.TODO(), nil); err != nil {
		log.Fatal(err)
	}

	return client.Database(mongoDBName)

}

func RandomInt(min, max int) int {
	return rand.Intn(max-min+1) + min
}

func RandomDecimal(min, max float64) float64 {
	value := min + rand.Float64()*(max-min)
	return roundToTwoDecimalPlaces(value)
}

func roundToTwoDecimalPlaces(value float64) float64 {
	return float64(int(value*100+0.5)) / 100
}

func GetProfilingData(mongoDB *mongo.Database, collName, opname string, startTime time.Time) int {

	//Retreive events from system.profile
	coll := mongoDB.Collection("system.profile")
	filter := bson.D{
		{"op", opname},
		{"ns", collName},
		{"ts", bson.D{{"$gte", startTime}}},
		{"appName", "DotLocalPerformanceTest"},
	}
	project := bson.D{{"millis", 1}, {"_id", -1}}
	opts := options.Find().SetProjection(project)
	cursor, err := coll.Find(context.TODO(), filter, opts)
	if err != nil {
		log.Fatal(err)
	}
	defer cursor.Close(context.TODO())
	var mdbDuration int64
	for cursor.Next(context.TODO()) {
		res := cursor.Current
		mdbDuration += res.Lookup("millis").AsInt64()
	}
	return int(mdbDuration)

}

func ResetProfiling(mongoDB *mongo.Database, collectionName, operation string) {

	//We can't use explain plans with insert operations so we'll enable profiling and get execution times from the system.profile collection
	//We need to increase the size of system.profile as the default 1MB cap is too small for our purposes, so we'll set it to 10GB (profiling
	//Q2 data loads was maxing out at ~7.25GB)

	//Start by disabling profiling if it is currently active
	command := bson.D{
		{"profile", 0},
	}
	mongoDB.RunCommand(context.TODO(), command)

	//Next, drop the system.profile
	coll := mongoDB.Collection("system.profile")
	coll.Drop(context.TODO())

	//Now recreate it with the larger capacity
	createOpts := options.CreateCollection().SetCapped(true).SetSizeInBytes(10000000000)
	mongoDB.CreateCollection(context.TODO(), "system.profile", createOpts)

	//Now re-enable profiling with a filter to only capture the events related to this data load
	command = bson.D{
		{"profile", 1},
		{"filter", bson.D{
			{"op", operation},
			{"ns", collectionName},
			{"appName", "DotLocalPerformanceTest"},
		}},
	}
	mongoDB.RunCommand(context.TODO(), command)

}

func DisableProfiling(mongoDB *mongo.Database) {

	//Turn off profiling
	command := bson.D{
		{"profile", 0},
	}
	mongoDB.RunCommand(context.TODO(), command)
	//Reset the size of system.profile to 1MB
	command = bson.D{
		{"profile", 0},
	}
	mongoDB.RunCommand(context.TODO(), command)
	coll := mongoDB.Collection("system.profile")
	coll.Drop(context.TODO())
	createOpts := options.CreateCollection().SetCapped(true).SetSizeInBytes(1048576)
	mongoDB.CreateCollection(context.TODO(), "system.profile", createOpts)

}

// randomDate generates a random date between startDate and endDate.
func RandomDate(startDate, endDate time.Time) time.Time {
	if endDate.Before(startDate) {
		startDate, endDate = endDate, startDate
	}

	diff := endDate.Sub(startDate)
	randomDuration := time.Duration(rand.Int63n(int64(diff)))

	newDate := startDate.Add(randomDuration)

	return time.Date(newDate.Year(), newDate.Month(), newDate.Day(), 0, 0, 0, 0, time.UTC)
}

// randomString selects a random string from an array of strings.
func RandomString(strings []string) string {
	if len(strings) == 0 {
		return ""
	}
	return strings[rand.Intn(len(strings))]
}

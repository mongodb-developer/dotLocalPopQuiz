package hierarchy

import (
	"context"
	"os"
	"strconv"

	"log"

	"sync"
	"time"

	"github.com/mongodb-developer/dotLocalPopQuiz/utils"
	"go.mongodb.org/mongo-driver/bson"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
)

func LoadQ3Data() {

	mongoDBURI := os.Getenv("MONGODB_URI")
	mongoDBName := os.Getenv("MONGODB_DB_NAME")
	connectionCount := 2
	routineCount := 2
	customers := 10000000

	//Work out the number of customer docs to be loaded by each connection.
	customersCount := customers / connectionCount

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
	coll := connections[0].Collection("Q3CustomersFlat")
	coll.Drop(context.TODO())
	coll = connections[0].Collection("Q3CustomersHierarchical")
	coll.Drop(context.TODO())

	//Start a new Go Routine for each MDB connection
	startTime := time.Now()
	startID := 1
	for i := 0; i < connectionCount; i++ {
		go runQ3DataLoads(connections[i], wgs[i], routineCount, startID, customersCount, startTime)
		startID += customersCount
	}
	utils.MasterWG.Wait()
	endTime := time.Now()

	//Save the execution duration back to MongoDB
	var result utils.TestResult
	result.TestName = "Q3 Data Load"
	result.StartTime = startTime
	result.EndTime = endTime
	result.Duration = int(endTime.UnixMilli() - startTime.UnixMilli())
	resultsColl := connections[0].Collection("Q3DataLoadResults")
	_, err := resultsColl.InsertOne(context.TODO(), result)
	if err != nil {
		log.Fatal(err)
	}
	log.Print("Q3 Data Load Completed")

}

func insertQ3Data(mdb *mongo.Database, wg *sync.WaitGroup, startCustomer, endCustomer int, startTime time.Time) {

	defer wg.Done()

	//Use Write Concern 1 for faster performance. Not recommended for a production system
	wc := writeconcern.W1()
	var collOpts options.CollectionOptions
	collOpts.WriteConcern = wc
	fColl := mdb.Collection("Q3CustomersFlat", &collOpts)
	hColl := mdb.Collection("Q3CustomersHierarchical", &collOpts)

	var insertOrderedOp options.InsertManyOptions

	firstNames := []string{"Graeme", "John", "Andrew", "Rick", "Karen", "Sarah", "Steffan", "Daniel", "Ginny", "Kerry"}
	lastNames := []string{"Robinson", "Page", "Brandino", "Blackhurst", "Barry", "Asay", "McClelland", "Mejia", "Suhaimi", "Gregory"}
	middleNames := []string{"Douglas", "Sarah", "Anne", "Campbell", "Cameron", "Nicholas", "Elise", "Elizabeth", "Gary", "Jennifer"}
	tiers := []string{"Blue", "Silver", "Gold", "Platinum", "Diamond"}
	status := []string{"closed", "suspended", "active"}

	var fDocs []interface{}
	var hDocs []interface{}

	for i := startCustomer; i < endCustomer; i++ {

		fName := utils.RandomString(firstNames)
		mName := utils.RandomString(middleNames)
		lName := utils.RandomString(lastNames)
		dob := utils.RandomDate(time.Date(1960, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2000, 12, 31, 0, 0, 0, 0, time.UTC))
		acDate := utils.RandomDate(time.Date(2015, 1, 1, 0, 0, 0, 0, time.UTC), time.Now())
		lpDate := utils.RandomDate(acDate, time.Now())
		spend := utils.RandomDecimal(10.00, 100000.00)
		tier := utils.RandomString(tiers)
		status := utils.RandomString(status)

		fDoc := bson.D{
			{"_id", i},
			{"firstName", fName},
			{"middleName", mName},
			{"lastName", lName},
			{"dob", dob},
		}

		hDoc := bson.D{
			{"_id", i},
			{"customerDetails", bson.D{
				{"firstName", fName},
				{"middleName", mName},
				{"lastName", lName},
				{"dob", dob},
			}},
		}

		//Pad out fields using random numeric values - we need another 290 fields
		//split over 3 sectionsin the hierarchical document, so build in batches of
		//100, 100, and 90
		var tmpBSON bson.D
		for x := 6; x < 106; x++ {
			fieldVal := utils.RandomDecimal(1.00, 1000000.00)
			fDoc = append(fDoc, bson.E{"field" + strconv.FormatInt(int64(x), 10), fieldVal})
			tmpBSON = append(tmpBSON, bson.E{"field" + strconv.FormatInt(int64(x), 10), fieldVal})
		}
		hDoc = append(hDoc, bson.E{"customerDemographics", tmpBSON})

		tmpBSON = bson.D{}
		for x := 106; x < 206; x++ {
			fieldVal := utils.RandomDecimal(1.00, 1000000.00)
			fDoc = append(fDoc, bson.E{"field" + strconv.FormatInt(int64(x), 10), fieldVal})
			tmpBSON = append(tmpBSON, bson.E{"field" + strconv.FormatInt(int64(x), 10), fieldVal})
		}
		hDoc = append(hDoc, bson.E{"customerPreferences", tmpBSON})

		tmpBSON = bson.D{}
		for x := 206; x < 296; x++ {
			fieldVal := utils.RandomDecimal(1.00, 1000000.00)
			fDoc = append(fDoc, bson.E{"field" + strconv.FormatInt(int64(x), 10), fieldVal})
			tmpBSON = append(tmpBSON, bson.E{"field" + strconv.FormatInt(int64(x), 10), fieldVal})
		}
		hDoc = append(hDoc, bson.E{"accountMetaData", tmpBSON})

		tmpBSON = bson.D{}
		fDoc = append(fDoc, bson.E{"accountCreated", acDate})
		tmpBSON = append(tmpBSON, bson.E{"accountCreated", acDate})
		fDoc = append(fDoc, bson.E{"lastPurchase", lpDate})
		tmpBSON = append(tmpBSON, bson.E{"lastPurchase", lpDate})
		fDoc = append(fDoc, bson.E{"lifetimeSpend", spend})
		tmpBSON = append(tmpBSON, bson.E{"lifetimeSpend", spend})
		fDoc = append(fDoc, bson.E{"accountTier", tier})
		tmpBSON = append(tmpBSON, bson.E{"accountTier", tier})
		fDoc = append(fDoc, bson.E{"accountStatus", status})
		tmpBSON = append(tmpBSON, bson.E{"accountStatus", status})
		hDoc = append(hDoc, bson.E{"rewardsMetaData", tmpBSON})

		fDocs = append(fDocs, fDoc)
		hDocs = append(hDocs, hDoc)

		if len(fDocs) == 10000 || i == endCustomer-1 {
			_, err := fColl.InsertMany(context.TODO(), fDocs, insertOrderedOp.SetOrdered(false))
			if err != nil {
				log.Fatal(err)
			}
			fDocs = nil
			_, err = hColl.InsertMany(context.TODO(), hDocs, insertOrderedOp.SetOrdered(false))
			if err != nil {
				log.Fatal(err)
			}
			hDocs = nil
			log.Printf("Customer document batch written")
		}
	}
}

func runQ3DataLoads(mdb *mongo.Database, wg *sync.WaitGroup, goRoutines, startID, customerCount int, startTime time.Time) {

	defer utils.MasterWG.Done()

	//Work out the number of customers to be loaded by each goRoutine.
	routineCustomerCount := customerCount / goRoutines

	//Initialize the wait group
	wg.Add(goRoutines)

	for i := 0; i < goRoutines; i++ {
		go insertQ3Data(mdb, wg, startID, startID+routineCustomerCount, startTime)
		startID += routineCustomerCount
	}
	wg.Wait()

}

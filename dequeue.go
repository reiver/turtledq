package main



import (
	"fmt"
//	"github.com/streadway/amqp"
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
	"log/syslog"
	"math/rand"
	"time"
)



func dequeue(syslogLog *syslog.Writer, mongoHref string, mongoDatabaseName string, mongoCollectionName string) {

	//DEBUG
	syslogLog.Notice("[dequeue] BEGIN")
	syslogLog.Notice(  fmt.Sprintf("    [dequeue] mongoHref = [%v]", mongoHref)  )

	// Deal with parameters.
		if "" == mongoHref {
			errMsg := fmt.Sprintf("    [dequeue] Bad mongoHref. Received: [%v].", mongoHref)
			syslogLog.Err(errMsg)
			panic(errMsg)
/////////////////////// RETURN
			return
		}

		if "" == mongoDatabaseName {
			errMsg := fmt.Sprintf("    [dequeue] Bad mongoDatabaseName. Received: [%v].", mongoDatabaseName)
			syslogLog.Err(errMsg)
			panic(errMsg)
/////////////////////// RETURN
			return
		}

		if "" == mongoCollectionName {
			errMsg := fmt.Sprintf("    [dequeue] Bad mongoCollectionName. Received: [%v].", mongoCollectionName)
			syslogLog.Err(errMsg)
			panic(errMsg)
/////////////////////// RETURN
			return
		}


	// Connect to MongoDB
		session, err := mgo.Dial(mongoHref)
		if err != nil {
			syslogLog.Err( fmt.Sprintf("    [dequeue] Could NOT connect to MongoDB at [%v], received err = [%v]", mongoHref, err) )
			panic(err)
/////////////////////// RETURN
			return
		}
		defer session.Close()

		// Optional. Switch the session to a monotonic behavior.
		session.SetMode(mgo.Monotonic, true)



		mongoCollection := session.DB(mongoDatabaseName).C(mongoCollectionName)


	// Forever
		for {

			//DEBUG
			syslogLog.Notice("    [dequeue] =-<>-=-<>-=-<>-=-<>-=-<>-=-<>-=-<>-=-<>-=-<>-=-<>-=")
			syslogLog.Notice("    [dequeue] loop")




			// Check MongoDB for items that are ready.
				mongoCriteria := bson.M{  "when":bson.M{"$lte": bson.Now()}  }

				mongoQuery := mongoCollection.Find(mongoCriteria)

				//DEBUG
				syslogLog.Notice( fmt.Sprintf("    [dequeue] mongoQuery = [%v]", mongoQuery) )
				
				if nil != err {
					syslogLog.Err( fmt.Sprintf("    [dequeue] Error querying MongoDB with mongoCriteria = [%v] received err = [%v]", mongoCriteria, err) )
				} else {

					var x struct{
						when   time.Time
						target string
					}

					items := mongoQuery.Iter()
					for items.Next(&x) {

						//DEBUG
						syslogLog.Err( fmt.Sprintf("    [dequeue] Received row: [%v]", x) )


					} // for

				}



			// Sleep for a while before checking again.
				sleepTime := time.Duration(5 + rand.Intn(7)) * time.Second

				//DEBUG
				syslogLog.Notice( fmt.Sprintf("    [dequeue] sleep for %v", sleepTime) )

				time.Sleep(sleepTime)
		} // for


	//DEBUG
	syslogLog.Notice("[dequeue] END")

}

package main



import (
	"fmt"
	"github.com/streadway/amqp"
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
	"log/syslog"
	"math/rand"
	"time"
)



func dequeue(syslogLog *syslog.Writer, mongoHref string, mongoDatabaseName string, mongoCollectionName string, amqpHref string) {

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

		if "" == amqpHref {
			errMsg := fmt.Sprintf("    [dequeue] Bad amqpHref. Received: [%v].", amqpHref)
			syslogLog.Err(errMsg)
			panic(errMsg)
/////////////////////// RETURN
			return
		}


	// Connect to MongoDB
		mongoSession, err := mgo.Dial(mongoHref)
		if err != nil {
			syslogLog.Err(  fmt.Sprintf("    [dequeue] Could NOT connect to MongoDB at [%v], received err = [%v]", mongoHref, err)  )
			panic(err)
/////////////////////// RETURN
			return
		}
		defer mongoSession.Close()

		// Optional. Switch the session to a monotonic behavior.
		mongoSession.SetMode(mgo.Monotonic, true)

		mongoCollection := mongoSession.DB(mongoDatabaseName).C(mongoCollectionName)

		//DEBUG
		syslogLog.Notice(  fmt.Sprintf("    [dequeue] Connect to MongoDB with %v.%v", mongoDatabaseName, mongoCollectionName)  )


	// Connect to AMQP.
		amqpConnection, err := amqp.Dial(amqpHref)
		if err != nil {
			syslogLog.Err(  fmt.Sprintf("    [dequeue] Could NOT connect to AMQP server (RabbitMQ?) at [%v], received err = [%v]", amqpHref, err)  )
			panic(err)
/////////////////////// RETURN
			return
		}
		defer amqpConnection.Close()


	// Forever
		for {

			//DEBUG
			syslogLog.Notice("    [dequeue] =-<>-=-<>-=-<>-=-<>-=-<>-=-<>-=-<>-=-<>-=-<>-=-<>-= LOOP")




			// Check MongoDB for items that are ready.
				mongoCriteria := bson.M{  "when":bson.M{"$lte": bson.Now()}  }

				mongoQuery := mongoCollection.Find(mongoCriteria)

				//DEBUG
				syslogLog.Notice( fmt.Sprintf("    [dequeue] mongoQuery = [%v]", mongoQuery) )
				
				if nil != err {
					syslogLog.Err( fmt.Sprintf("    [dequeue] Error querying MongoDB with mongoCriteria = [%v] received err = [%v]", mongoCriteria, err) )
				} else {

					syslogLog.Err( fmt.Sprintf("    [dequeue] Success querying MongoDB with mongoCriteria = [%v]", mongoCriteria, err) )

					var x struct{
						_Id    bson.ObjectId
						When   time.Time
						Target string
					}

					items := mongoQuery.Iter()
					for items.Next(&x) {

						//DEBUG
						syslogLog.Err( fmt.Sprintf("        [dequeue] Received row: [%v]", x) )


					} // for

					syslogLog.Err("    [dequeue] Done iterating through result of query.")
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

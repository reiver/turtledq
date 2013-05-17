package main



import (
	"fmt"
	"github.com/streadway/amqp"
	"encoding/json"
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
	"log/syslog"
	"math/rand"
	"time"
)



func dequeue(syslogLog *syslog.Writer, mongoHref string, mongoDatabaseName string, mongoCollectionName string, amqpHref string, amqpExchange string, amqpExchangeType string) {

	//DEBUG
	syslogLog.Notice("[dequeue] BEGIN")
	syslogLog.Notice(  fmt.Sprintf("    [dequeue]           mongoHref = [%v]", mongoHref)  )
	syslogLog.Notice(  fmt.Sprintf("    [dequeue]   mongoDatabaseName = [%v]", mongoDatabaseName)  )
	syslogLog.Notice(  fmt.Sprintf("    [dequeue] mongoCollectionName = [%v]", mongoCollectionName)  )
	syslogLog.Notice(  fmt.Sprintf("    [dequeue]            amqpHref = [%v]", amqpHref)  )
	syslogLog.Notice(  fmt.Sprintf("    [dequeue]        amqpExchange = [%v]", amqpExchange)  )
	syslogLog.Notice(  fmt.Sprintf("    [dequeue]    amqpExchangeType = [%v]", amqpExchangeType)  )

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
		syslogLog.Notice(  fmt.Sprintf("    [dequeue] Connected to MongoDB with %v.%v", mongoDatabaseName, mongoCollectionName)  )


	// Connect to AMQP.
		amqpConnection, err := amqp.Dial(amqpHref)
		if err != nil {
			syslogLog.Err(  fmt.Sprintf("    [dequeue] Could NOT connect to AMQP server (RabbitMQ?) at [%v], received err = [%v]", amqpHref, err)  )
			panic(err)
/////////////////////// RETURN
			return
		}
		defer amqpConnection.Close()

		amqpChannel, err := amqpConnection.Channel()
		if err != nil {
			syslogLog.Err("    [dequeue] Could NOT get AMQP channel")
			panic(err)
/////////////////////// RETURN
			return
		}

		//DEBUG
		syslogLog.Notice(  fmt.Sprintf("    [dequeue] amqpChannel = [%v]", amqpChannel)  )


		//DEBUG
		syslogLog.Notice(  fmt.Sprintf("    [dequeue] Connected to AMQP server (RabbitMQ?)", mongoDatabaseName, mongoCollectionName)  )



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

					syslogLog.Notice( fmt.Sprintf("    [dequeue] Success querying MongoDB with mongoCriteria = [%v]", mongoCriteria, err) )

//					var x struct{
//						_Id    bson.ObjectId
//						When   time.Time
//						Target string
//						Message map[string]interface{}
//					}

					x := map[string]interface{}{}

					items := mongoQuery.Iter()
					for items.Next(&x) {

						//DEBUG
						syslogLog.Notice( fmt.Sprintf("        [dequeue] Received row: [%v]", x) )


//						routingKey := x.Target
						routingKey := x["target"].(string)

						//DEBUG
						syslogLog.Notice(  fmt.Sprintf("        [dequeue] Routing Key : [%v]", routingKey) )

//						body, err := json.Marshal(x.Message)
						body, err := json.Marshal(x["message"])
						if nil != err {
							//DEBUG
							syslogLog.Err(  fmt.Sprintf("        [dequeue] Error serializing body into JSON, received err = [%v]", err)  )
//@TODO #############################################################################

						}

						//DEBUG
						syslogLog.Notice(  fmt.Sprintf("        [dequeue] Body : [%v]", body) )


						//DEBUG
						syslogLog.Notice(  fmt.Sprintf("        [dequeue] Routing key: [%v]", routingKey) )
						syslogLog.Notice(  fmt.Sprintf("        [dequeue] declared Exchange, publishing %dB body (%s)", len(body), body)  )



						if err = amqpChannel.Publish(
							amqpExchange,   // publish to an exchange
							routingKey, // routing to 0 or more queues
							false,      // mandatory
							false,      // immediate
							amqp.Publishing{
								Headers:         amqp.Table{},
								ContentType:     "text/plain",
								ContentEncoding: "",
								Body:            body,
								DeliveryMode:    amqp.Transient, // 1=non-persistent, 2=persistent
								Priority:        0,              // 0-9
								// a bunch of application/implementation-specific fields
							},
						); err != nil {

							//DEBUG
							syslogLog.Err( fmt.Sprintf("        [dequeue] Error: Exchange Publish: [%v]", err) )
						} else {

							//DEBUG
							syslogLog.Err("        [dequeue] Published")
						}



						// Delete from MongoDB.
//							err = mongoCollection.RemoveId(x._Id)
							err = mongoCollection.RemoveId(x["_id"])
							if nil != err {
								//DEBUG
//								syslogLog.Err(  fmt.Sprintf("        [dequeue] Could NOT remove item from Mongo with _id [%v], received err = [%v]", x._Id, err)  )
								syslogLog.Err(  fmt.Sprintf("        [dequeue] Could NOT remove item from Mongo with _id [%v], received err = [%v]", x["_id"], err)  )
							} else {
								//DEBUG
//								syslogLog.Notice(  fmt.Sprintf("        [dequeue] Removed item from Mongo with _id [%v]", x._Id)  )
								syslogLog.Notice(  fmt.Sprintf("        [dequeue] Removed item from Mongo with _id [%v]", x["_id"])  )
							}


					} // for

					syslogLog.Notice("    [dequeue] Done iterating through result of query.")
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

package main



import (
	"fmt"
	"github.com/bitly/go-simplejson"
	"github.com/streadway/amqp"
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
	"log/syslog"
//	"math/rand"
	"time"
)



func enqueue(syslogLog *syslog.Writer, mongoHref string, mongoDatabaseName string, mongoCollectionName string, amqpHref string, amqpExchange string, amqpExchangeType string, amqpQueue string) {

	//DEBUG
	syslogLog.Notice("[enqueue] BEGIN")
	syslogLog.Notice(  fmt.Sprintf("    [enqueue]           mongoHref = [%v]", mongoHref            )  )
	syslogLog.Notice(  fmt.Sprintf("    [enqueue]   mongoDatabaseName = [%v]", mongoDatabaseName    )  )
	syslogLog.Notice(  fmt.Sprintf("    [enqueue] mongoCollectionName = [%v]", mongoCollectionName  )  )
	syslogLog.Notice(  fmt.Sprintf("    [enqueue]            amqpHref = [%v]", amqpHref             )  )
	syslogLog.Notice(  fmt.Sprintf("    [enqueue]        amqpExchange = [%v]", amqpExchange         )  )
	syslogLog.Notice(  fmt.Sprintf("    [enqueue]    amqpExchangeType = [%v]", amqpExchangeType     )  )
	syslogLog.Notice(  fmt.Sprintf("    [enqueue]           amqpQueue = [%v]", amqpQueue            )  )

	// Deal with parameters.
		if "" == mongoHref {
		errMsg := fmt.Sprintf("    [enqueue] Bad mongoHref. Received: [%v].", mongoHref)
			syslogLog.Err(errMsg)
			panic(errMsg)
/////////////////////// RETURN
			return
		}

		if "" == mongoDatabaseName {
		errMsg := fmt.Sprintf("    [enqueue] Bad mongoHref. Received: [%v].", mongoDatabaseName)
			syslogLog.Err(errMsg)
			panic(errMsg)
/////////////////////// RETURN
			return
		}

		if "" == mongoCollectionName {
		errMsg := fmt.Sprintf("    [enqueue] Bad mongoHref. Received: [%v].", mongoCollectionName)
			syslogLog.Err(errMsg)
			panic(errMsg)
/////////////////////// RETURN
			return
		}

		if "" == amqpHref {
		errMsg := fmt.Sprintf("    [enqueue] Bad amqpHref. Received: [%v].", amqpHref)
			syslogLog.Err(errMsg)
			panic(errMsg)
/////////////////////// RETURN
			return
		}

		if "" == amqpExchange {
		errMsg := fmt.Sprintf("    [enqueue] Bad amqpExchange. Received: [%v].", amqpExchange)
			syslogLog.Err(errMsg)
			panic(errMsg)
/////////////////////// RETURN
			return
		}

		if "" == amqpExchangeType {
		errMsg := fmt.Sprintf("    [enqueue] Bad amqpExchangeType. Received: [%v].", amqpExchangeType)
			syslogLog.Err(errMsg)
			panic(errMsg)
/////////////////////// RETURN
			return
		}

		if "" == amqpQueue {
		errMsg := fmt.Sprintf("    [enqueue] Bad amqpQueue. Received: [%v].", amqpQueue)
			syslogLog.Err(errMsg)
			panic(errMsg)
/////////////////////// RETURN
			return
		}


	// Connect to AMQP.
		amqpConnection, err := amqp.Dial(amqpHref)
		if err != nil {
			syslogLog.Err(  fmt.Sprintf("    [enqueue] Could NOT connect to AMQP server (RabbitMQ?) at [%v], received err = [%v]", amqpHref, err)  )
			panic(err)
/////////////////////// RETURN
			return
		}
		defer amqpConnection.Close()

		amqpChannel, err := amqpConnection.Channel()
		if err != nil {
			syslogLog.Err("    [enqueue] Could NOT get AMQP channel")
			panic(err)
/////////////////////// RETURN
			return
		}

		//DEBUG
		syslogLog.Notice(  fmt.Sprintf("    [enqueue] amqpChannel = [%v]", amqpChannel)  )



	// Connect to MongoDB
		mongoSession, err := mgo.Dial(mongoHref)
		if err != nil {
			syslogLog.Err(  fmt.Sprintf("    [enqueue] Could NOT connect to MongoDB at [%v], received err = [%v]", mongoHref, err)  )
			panic(err)
/////////////////////// RETURN
			return
		}
		defer mongoSession.Close()

		// Optional. Switch the session to a monotonic behavior.
		mongoSession.SetMode(mgo.Monotonic, true)

		mongoCollection := mongoSession.DB(mongoDatabaseName).C(mongoCollectionName)

		//DEBUG
		syslogLog.Notice(  fmt.Sprintf("    [enqueue] Connected to MongoDB with %v.%v", mongoDatabaseName, mongoCollectionName)  )



	// AMQP queue bind.
		amqpKey := amqpQueue

		if err = amqpChannel.QueueBind(
			amqpQueue,    // name of the queue
			amqpKey,      // bindingKey
			amqpExchange, // sourceExchange
			false,        // noWait
			nil,          // arguments
		); err != nil {
			syslogLog.Err(  fmt.Sprintf("    [enqueue] Could NOT bind to queue [%v] on AMQP server (RabbitMQ?) at [%v], received err = [%v]", amqpQueue, amqpHref, err)  )
			panic(err)
/////////////////////// RETURN
			return
		}

		//DEBUG
		syslogLog.Notice("    [enqueue] Queue bound.")


	// Set up AMQP consumer.
		amqpDeliveries, err := amqpChannel.Consume(
			amqpQueue,  // name
			"turtledq", // consumerTag
			false,      // noAck
			false,      // exclusive
			false,      // noLocal
			false,      // noWait
			nil,        // arguments
		)
		if err != nil {
			syslogLog.Err(  fmt.Sprintf("    [enqueue] Could NOT get deliverables from queue [%v] on AMQP server (RabbitMQ?) at [%v], received err = [%v]", amqpQueue, amqpHref, err)  )
			panic(err)
/////////////////////// RETURN
			return
		}






	// Forever
		for d := range amqpDeliveries {

			//DEBUG
			syslogLog.Notice("        [enqueue] =-<>-=-<>-=-<>-=-<>-=-<>-=-<>-=-<>-=-<>-=-<>-=-<>-= LOOP")
			syslogLog.Notice( fmt.Sprintf("        [enqueue] got %dB message: [%v]", len(d.Body), d.DeliveryTag) )
			syslogLog.Notice( fmt.Sprintf("        [enqueue] THING: [%v]", d) )



                        sjson, err := simplejson.NewJson(d.Body)
                        if nil != err {
                                //DEBUG
                                syslogLog.Err( fmt.Sprintf("        [enqueue] Could NOT parse raw JSON: [%v]", d.Body) )
//@TODO ##################################################################################################################
                        }

                        //DEBUG
                        syslogLog.Notice( fmt.Sprintf("        [enqueue] json = [%#v]", sjson) )



			haveTarget := true
			messageTarget, err := sjson.Get("target").String()
			if nil != err {
				haveTarget = false
			}

			//DEBUG
			if haveTarget {
				syslogLog.Notice(  fmt.Sprintf("        [enqueue] target = [%v]", messageTarget)  )
			} else {
				syslogLog.Notice("        [enqueue] either do NOT have messageTarget (or did not receive a \"good\" one)")
			}


			haveWhen := true
			messageWhen, err := sjson.Get("when").Int64()
			if nil != err {
				haveWhen = false
			}

			//DEBUG
			if haveWhen {
				syslogLog.Notice(  fmt.Sprintf("        [enqueue] when = [%v]", messageWhen)  )
			} else {
				syslogLog.Notice("        [enqueue] either do NOT have messageTarget (or did not receive a \"good\" one)")
			}

			haveMessage := true
			messageMessage, err := sjson.Get("message").Map()
			if nil != err {
				haveMessage = false
			}

			//DEBUG
			if haveMessage {
				syslogLog.Notice(  fmt.Sprintf("        [enqueue] message = [%v]", messageMessage)  )
			} else {
				syslogLog.Notice("        [enqueue] either do NOT have messageMessage (or did not receive a \"good\" one)")
			}




			// Deal with corrupted items.
				if !haveTarget || !haveWhen || !haveMessage {

				//DEBUG
				syslogLog.Notice("        [enqueue] Empty item from AMQP queue. Will ACK early and continue.")

				// Ack.
					err = d.Ack(false)
					if nil != err {
						syslogLog.Err(  fmt.Sprintf("        [enqueue] ERROR EARLY ACKing: [%v]", err)  )
					}

					//DEBUG
					syslogLog.Notice("        [enqueue] EARLY ACKed")


		/////////////////////// CONTINUE
					continue
				}


			// Enqueue the item
				mongoDoc := bson.M{
						"target":  messageTarget,
						"when":    time.Unix(messageWhen, 0),
						"message": messageMessage}

				err = mongoCollection.Insert(mongoDoc)
				if nil != err {
					syslogLog.Err(  fmt.Sprintf("        [enqueue] ERROR Inserting into Mongo: [%v]", err)  )
//@TODO ###############################################################################################
				}

				syslogLog.Notice(  fmt.Sprintf("        [enqueue] Inserted into Mongo: [%v]", mongoDoc)  )




			// Ack.
				err = d.Ack(false)
				if nil != err {
					syslogLog.Err(  fmt.Sprintf("        [enqueue] ERROR ACKing: [%v]", err)  )
				}

				//DEBUG
				syslogLog.Notice("        [enqueue] ACKed")



//			sleepTime := time.Duration(5 + rand.Intn(7)) * time.Second
//
//			//DEBUG
//			syslogLog.Notice( fmt.Sprintf("    [enqueue] sleep for %v", sleepTime) )
//
//			time.Sleep(sleepTime)
		} // for


	//DEBUG
	syslogLog.Notice("[enqueue] END")

}

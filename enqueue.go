package main



import (
	"fmt"
	"github.com/bitly/go-simplejson"
	"github.com/streadway/amqp"
	"log/syslog"
	"math/rand"
	"time"
)



func enqueue(syslogLog *syslog.Writer, amqpHref string, amqpExchange string, amqpExchangeType string, amqpQueue string) {

	//DEBUG
	syslogLog.Notice("[enqueue] BEGIN")
	syslogLog.Notice(  fmt.Sprintf("    [enqueue]            amqpHref = [%v]", amqpHref)  )
	syslogLog.Notice(  fmt.Sprintf("    [enqueue]        amqpExchange = [%v]", amqpExchange)  )
	syslogLog.Notice(  fmt.Sprintf("    [enqueue]    amqpExchangeType = [%v]", amqpExchangeType)  )
	syslogLog.Notice(  fmt.Sprintf("    [enqueue]           amqpQueue = [%v]", amqpQueue)  )

	// Deal with parameters.
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
		amqpDeliveries, err := c.channel.Consume(
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
		for {


			//DEBUG
			syslogLog.Notice("    [enqueue] =-<>-=-<>-=-<>-=-<>-=-<>-=-<>-=-<>-=-<>-=-<>-=-<>-= LOOP")


			for d := range deliveries {



                        //DEGUG
                        syslogLog.Notice("    [HANDLE] =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-= BEGIN")
                        syslogLog.Notice( fmt.Sprintf("    [HANDLE] got %dB message: [%v]", len(d.Body), d.DeliveryTag) )
                        syslogLog.Notice( fmt.Sprintf("    [HANDLE] THING: [%v]", d) )


                        sjson, err := simplejson.NewJson(d.Body)
                        if nil != err {
                                //DEBUG
                                syslogLog.Err( fmt.Sprintf("[HANDLE] Could NOT parse raw JSON: %v", d.Body) )
//@TODO: ###########################################################################################
                        }

                        //DEBUG
                        syslogLog.Notice( fmt.Sprintf("[HANDLE] json = [%#v]", sjson) )




			sleepTime := time.Duration(5 + rand.Intn(7)) * time.Second

			//DEBUG
			syslogLog.Notice( fmt.Sprintf("    [enqueue] sleep for %v", sleepTime) )

			time.Sleep(sleepTime)
		} // for


	//DEBUG
	syslogLog.Notice("[enqueue] END")

}

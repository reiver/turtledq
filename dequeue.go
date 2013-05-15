package main



import (
	"fmt"
//	"github.com/streadway/amqp"
	"labix.org/v2/mgo"
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


	// Forever
		for {

			//DEBUG
			syslogLog.Notice("    [dequeue] =-<>-=-<>-=-<>-=-<>-=-<>-=-<>-=-<>-=-<>-=-<>-=-<>-=")
			syslogLog.Notice("    [dequeue] loop")




			// Check MongoDB for items that are ready.



			// Sleep for a while before checking again.
				sleepTime := time.Duration(5 + rand.Intn(7)) * time.Second

				//DEBUG
				syslogLog.Notice( fmt.Sprintf("    [dequeue] sleep for %v", sleepTime) )

				time.Sleep(sleepTime)
		} // for


	//DEBUG
	syslogLog.Notice("[dequeue] END")

}

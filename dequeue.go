package main



import (
	"fmt"
	"github.com/streadway/amqp"
	"log/syslog"
	"math/rand"
	"time"
)



func dequeue(syslogLog *syslog.Writer) {

	//DEBUG
	syslogLog.Notice("[dequeue] BEGIN")


	// Forever
		for {

			//DEBUG
			syslogLog.Notice("    [dequeue] =-<>-=-<>-=-<>-=-<>-=-<>-=-<>-=-<>-=-<>-=-<>-=-<>-=")
			syslogLog.Notice("    [dequeue] loop")



			sleepTime := time.Duration(5 + rand.Intn(7)) * time.Second

			//DEBUG
			syslogLog.Notice( fmt.Sprintf("    [dequeue] sleep for %v", sleepTime) )

			time.Sleep(sleepTime)
		} // for


	//DEBUG
	syslogLog.Notice("[dequeue] END")

}

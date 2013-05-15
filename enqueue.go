package main



import (
	"fmt"
	"github.com/streadway/amqp"
	"log/syslog"
	"math/rand"
	"time"
)



func enqueue(syslogLog *syslog.Writer) {

	//DEBUG
	syslogLog.Notice("[enqueue] BEGIN")


	// Forever
		for {

			//DEBUG
			syslogLog.Notice("    [enqueue] =-<>-=-<>-=-<>-=-<>-=-<>-=-<>-=-<>-=-<>-=-<>-=-<>-=")
			syslogLog.Notice("    [enqueue] loop")



			sleepTime := time.Duration(5 + rand.Intn(7)) * time.Second

			//DEBUG
			syslogLog.Notice( fmt.Sprintf("    [enqueue] sleep for %v", sleepTime) )

			time.Sleep(sleepTime)
		} // for


	//DEBUG
	syslogLog.Notice("[enqueue] END")

}

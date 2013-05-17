package main



import (
	"fmt"
//	"github.com/streadway/amqp"
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

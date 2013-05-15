package main



import (
	"fmt"
	"github.com/vaughan0/go-zmq"
	"log"
	"log/syslog"
)



func main() {

	// Syslog
		syslogLog, err :=syslog.New(syslog.LOG_INFO, "turtledq/client")
		if nil != err {
			log.Fatal("Crap on a stick, could not open up syslog!")
/////////// RETURN
			return
		}


	//DEBUG
	syslogLog.Notice("=-<>-=-<>-=-<>-=-<>-=-<>-=-<>-=-<>-=-<>-=-<>-=-<>-=-<>-=-<>-=-<>-=-<>-=-<>-=")
	syslogLog.Notice("TurtleDQ Client BEGIN.")


	// Create ZeroMQ context.
		ctx, err := zmq.NewContext()
		if err != nil {
			panic(err)
		}
		defer ctx.Close()


	// Create ZeroMQ socket (from ZeroMQ context).
		sock, err := ctx.Socket(zmq.Dealer)
		if err != nil {
			panic(err)
		}
		defer sock.Close()

		if err = sock.Connect("tcp://localhost:5555"); nil != err {
			panic(err)
		}


	// Send message over ZeroMQ (via channels).
		chans := sock.Channels()
		defer chans.Close()


		msg := "banana"

		//DEBUG
		syslogLog.Notice( fmt.Sprintf("msg = [%v] [%v]", msg, []byte(msg)) )


		chans.Out() <- [][]byte{  []byte(msg)  }


	//DEBUG
	syslogLog.Notice("TurtleDQ Client END.")
}

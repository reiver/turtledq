package main



import (
	"fmt"
	"github.com/vaughan0/go-zmq"
	"log"
	"log/syslog"
	"math/rand"
	"time"
)



func main() {

	// Syslog
		syslogLog, err :=syslog.New(syslog.LOG_INFO, "turtledq/server")
		if nil != err {
			log.Fatal("Crap on a stick, could not open up syslog!")
/////////// RETURN
			return
		}


	//DEBUG
	syslogLog.Notice("=-<>-=-<>-=-<>-=-<>-=-<>-=-<>-=-<>-=-<>-=-<>-=-<>-=-<>-=-<>-=-<>-=-<>-=-<>-=")
	syslogLog.Notice("TurtleDQ Server BEGIN.")


	// Go!
		go dequeue(syslogLog)


	// Create ZeroMQ context.
		ctx, err := zmq.NewContext()
		if err != nil {
			panic(err)
		}
		defer ctx.Close()


	// Create ZeroMQ socket (from ZeroMQ context).
		sock, err := ctx.Socket(zmq.Router)
		if err != nil {
			panic(err)
		}
		defer sock.Close()

		if err = sock.Bind("tcp://*:5555"); err != nil {
			panic(err)
		}


	// Handle input from ZeroMQ (via channels).
		chans := sock.Channels()
		defer chans.Close()



		for {
			select {

				case msg := <-chans.In():
					go func() {
						resp := handleRequest(syslogLog, msg)
						chans.Out() <- resp
					}()

				case err := <-chans.Errors():
					panic(err)

			} // select
		}


	//DEBUG
	syslogLog.Notice("TurtleDQ Server END.")
}


func handleRequest(syslogLog *syslog.Writer, msg [][]byte) [][]byte {

//@TODO
	syslogLog.Notice( fmt.Sprintf("[handleRequest] msg = [%v]", msg) )


	// Return
//@TODO
		return [][]byte{
			[]byte("apple banana cherry"),
		}
}


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

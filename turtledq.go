package main



import (
	"fmt"
	"github.com/msbranco/goconfig"
	"github.com/vaughan0/go-zmq"
	"log"
	"log/syslog"
	"os"
	"strings"
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


	// Config
		//DEBUG
		syslogLog.Notice("    Settings BEGIN.")

		cmdPath := os.Args[0]
		cmdPathLastSlash := strings.LastIndexFunc(cmdPath, func(c rune) bool {
			return '/' == c
		})

		cmdDirPath := cmdPath[0:cmdPathLastSlash]

		//DEBUG
		syslogLog.Notice( fmt.Sprintf("    command path: [%v]", cmdPath)  )
		syslogLog.Notice( fmt.Sprintf("    command path last slash: [%v]", cmdPathLastSlash) )
		syslogLog.Notice( fmt.Sprintf("    command dir path: [%v]", cmdDirPath) )

		confRelativePath := "turtledq.ini"
		confPath := cmdDirPath + "/" + confRelativePath

		//DEBUG
		syslogLog.Notice( fmt.Sprintf("    settings file relative path: [%v]", confRelativePath) )
		syslogLog.Notice( fmt.Sprintf("    settings file absolute path: [%v]", confPath) )

		//c, err := configfile.ReadConfigFile(confPath);
		c, err := goconfig.ReadConfigFile(confPath);
		if nil != err {
			errMsg := fmt.Sprintf("Error when trying to read config file: err = [%v]", err)
			syslogLog.Err(errMsg)
			log.Fatal(errMsg)
/////////// RETURN
			return
		}

		//DEBUG
		syslogLog.Notice( fmt.Sprintf("config file: [%v]", c) )

		//DEBUG
		syslogLog.Notice("    Settings END.")



	// Go!
		go dequeue(syslogLog)
		go enqueue(syslogLog)


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

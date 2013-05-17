package main



import (
	"fmt"
	"github.com/msbranco/goconfig"
	"log"
	"log/syslog"
	"os"
	"strings"
)



type TurtleConfig struct {
	syslogLog                  *syslog.Writer
	DequeueMongoHref            string
	DequeueMongoDatabaseName    string
	DequeueMongoCollectionName  string
	DequeueAmqpHref             string
	DequeueAmqpExchange         string
	DequeueAmqpExchangeType     string
	EnqueueAmqpHref             string
	EnqueueAmqpExchange         string
	EnqueueAmqpExchangeType     string
	EnqueueAmqpQueue            string
}



func NewTurtleConfig(syslogLog *syslog.Writer) (*TurtleConfig, error) {

	//DEBUG
	syslogLog.Notice("    [CONFIG] BEGIN.")



	cmdPath := os.Args[0]
	cmdPathLastSlash := strings.LastIndexFunc(cmdPath, func(c rune) bool {
		return '/' == c
	})

	cmdDirPath := cmdPath[0:cmdPathLastSlash]

	//DEBUG
	syslogLog.Notice( fmt.Sprintf("    [CONFIG] command path: [%v]", cmdPath)  )
	syslogLog.Notice( fmt.Sprintf("    [CONFIG] command path last slash: [%v]", cmdPathLastSlash) )
	syslogLog.Notice( fmt.Sprintf("    [CONFIG] command dir path: [%v]", cmdDirPath) )



	confRelativePath := "turtledq.ini"
	confPath := cmdDirPath + "/" + confRelativePath

	//DEBUG
	syslogLog.Notice( fmt.Sprintf("    [CONFIG] settings file relative path: [%v]", confRelativePath) )
	syslogLog.Notice( fmt.Sprintf("    [CONFIG] settings file absolute path: [%v]", confPath) )


	return NewTurtleConfigFromFile(syslogLog, confPath)
}

func NewTurtleConfigFromFile(syslogLog *syslog.Writer, configPath string) (*TurtleConfig, error) {

	me := TurtleConfig{syslogLog: syslogLog}

	//c, err := configfile.ReadConfigFile(configPath);
	c, err := goconfig.ReadConfigFile(configPath);
	if nil != err {
		errMsg := fmt.Sprintf("Error when trying to read config file: err = [%v]", err)
		me.syslogLog.Err(errMsg)
		log.Fatal(errMsg)
/////////////// RETURN
		return nil, err
	}

	//DEBUG
	me.syslogLog.Notice( fmt.Sprintf("    [CONFIG] config file: [%v]", c) )



	// Check for use_instead
		useInstead, err := c.GetString("default", "use_instead")

		if nil == err && "" != useInstead {
			//DEBUG
			me.syslogLog.Notice(  fmt.Sprintf("    [CONFIG] Have use_instead = [%v]", useInstead)  )

/////////////////////// RETURN
			return NewTurtleConfigFromFile(syslogLog, useInstead,)
		}

		//DEBUG
		me.syslogLog.Notice(  fmt.Sprintf("    [CONFIG] use_instead = [%v]", useInstead)  )



	// Get banal configs.
		me.DequeueMongoHref, err = c.GetString("dequeue", "mongo_href")
		if nil != err {
			me.syslogLog.Notice("    [CONFIG] NO [dequeue].mongo_href")
		} else {
			me.syslogLog.Notice(  fmt.Sprintf("    [CONFIG] [dequeue].mongo_href = [%v]", me.DequeueMongoHref)  )
		}

		me.DequeueMongoDatabaseName, err = c.GetString("dequeue", "mongo_database_name")
		if nil != err {
			me.syslogLog.Notice("    [CONFIG] NO [dequeue].mongo_database_name")
		} else {
			me.syslogLog.Notice(  fmt.Sprintf("    [CONFIG] [dequeue].mongo_database_name = [%v]", me.DequeueMongoDatabaseName)  )
		}

		me.DequeueMongoCollectionName, err = c.GetString("dequeue", "mongo_collection_name")
		if nil != err {
			me.syslogLog.Notice("    [CONFIG] NO [dequeue].mongo_collection_name")
		} else {
			me.syslogLog.Notice(  fmt.Sprintf("    [CONFIG] [dequeue].mongo_collection_name = [%v]", me.DequeueMongoCollectionName)  )
		}

		me.DequeueAmqpHref, err = c.GetString("dequeue", "amqp_href")
		if nil != err {
			me.syslogLog.Notice("    [CONFIG] NO [dequeue].amqp_href")
		} else {
			me.syslogLog.Notice(  fmt.Sprintf("    [CONFIG] [dequeue].amqp_href = [%v]", me.DequeueAmqpHref)  )
		}

		me.DequeueAmqpExchange, err = c.GetString("dequeue", "amqp_exchange")
		if nil != err {
			me.syslogLog.Notice("    [CONFIG] NO [dequeue].amqp_exchange")
		} else {
			me.syslogLog.Notice(  fmt.Sprintf("    [CONFIG] [dequeue].amqp_exchange = [%v]", me.DequeueAmqpExchange)  )
		}

		me.DequeueAmqpExchangeType, err = c.GetString("dequeue", "amqp_exchange_type")
		if nil != err {
			me.syslogLog.Notice("    [CONFIG] NO [dequeue].amqp_exchange_type")
		} else {
			me.syslogLog.Notice(  fmt.Sprintf("    [CONFIG] [dequeue].amqp_exchange_type = [%v]", me.DequeueAmqpExchangeType)  )
		}


		me.EnqueueAmqpHref, err = c.GetString("enqueue", "amqp_href")
		if nil != err {
			me.syslogLog.Notice("    [CONFIG] NO [enqueue].amqp_href")
		} else {
			me.syslogLog.Notice(  fmt.Sprintf("    [CONFIG] [enqueue].amqp_href = [%v]", me.EnqueueAmqpHref)  )
		}

		me.EnqueueAmqpExchange, err = c.GetString("enqueue", "amqp_exchange")
		if nil != err {
			me.syslogLog.Notice("    [CONFIG] NO [enqueue].amqp_exchange")
		} else {
			me.syslogLog.Notice(  fmt.Sprintf("    [CONFIG] [enqueue].amqp_exchange = [%v]", me.EnqueueAmqpExchange)  )
		}

		me.EnqueueAmqpExchangeType, err = c.GetString("enqueue", "amqp_exchange_type")
		if nil != err {
			me.syslogLog.Notice("    [CONFIG] NO [enqueue].amqp_exchange_type")
		} else {
			me.syslogLog.Notice(  fmt.Sprintf("    [CONFIG] [enqueue].amqp_exchange_type = [%v]", me.EnqueueAmqpExchangeType)  )
		}

		me.EnqueueAmqpQueue, err = c.GetString("enqueue", "amqp_queue")
		if nil != err {
			me.syslogLog.Notice("    [CONFIG] NO [enqueue].amqp_queue")
		} else {
			me.syslogLog.Notice(  fmt.Sprintf("    [CONFIG] [enqueue].amqp_queue = [%v]", me.EnqueueAmqpQueue)  )
		}



	//DEBUG
	me.syslogLog.Notice("    [CONFIG] END.")



	// Return.
		return &me, nil
}

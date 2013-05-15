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
			me.syslogLog.Notice("    [CONFIG] NO [deueue].mongo_href")
		} else {
			me.syslogLog.Notice(  fmt.Sprintf("    [CONFIG] [deueue].mongo_href = [%v]", me.DequeueMongoHref)  )
		}

		me.DequeueMongoDatabaseName, err = c.GetString("dequeue", "mongo_database_name")
		if nil != err {
			me.syslogLog.Notice("    [CONFIG] NO [deueue].mongo_database_name")
		} else {
			me.syslogLog.Notice(  fmt.Sprintf("    [CONFIG] [deueue].mongo_database_name = [%v]", me.DequeueMongoDatabaseName)  )
		}

		me.DequeueMongoCollectionName, err = c.GetString("dequeue", "mongo_collection_name")
		if nil != err {
			me.syslogLog.Notice("    [CONFIG] NO [deueue].mongo_collection_name")
		} else {
			me.syslogLog.Notice(  fmt.Sprintf("    [CONFIG] [deueue].mongo_collection_name = [%v]", me.DequeueMongoCollectionName)  )
		}



	//DEBUG
	me.syslogLog.Notice("    [CONFIG] END.")



	// Return.
		return &me, nil
}

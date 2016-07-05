package main

import (
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	flags "github.com/jessevdk/go-flags"
	mgo "gopkg.in/mgo.v2"
)

type CommandArgs struct {
	BindHost        string `long:"host" description:"Server Host" default:""`
	BindPort        int    `long:"port" description:"Server Port" default:"8080"`
	MongoUrl        string `long:"mongo-url" description:"MongoDB Remote Url" default:"http://localhost:27017"`
	MongoDatabase   string `long:"mongo-database" description:"MongoDB Database" required:"true"`
	MongoCollection string `long:"mongo-collection" description:"MongoDB Collection" required:"true"`
}

type ServerContext struct {
	Args            CommandArgs
	MongoSession    *mgo.Session
	MongoDatabase   *mgo.Database
	MongoCollection *mgo.Collection
}

func (context *ServerContext) ParseCommandArgs() (remains []string, err error) {
	remains, err = flags.Parse(&context.Args)
	return
}

func (context *ServerContext) ConnectMongoDB() (err error) {
	context.MongoSession, err = mgo.Dial(context.Args.MongoUrl)
	if err == nil {
		context.MongoSession.SetMode(mgo.Eventual, true)
		context.MongoDatabase = context.MongoSession.DB(context.Args.MongoDatabase)
		context.MongoCollection = context.MongoDatabase.C(context.Args.MongoCollection)
	}
	return
}

func (context *ServerContext) SetupRoutes() (err error) {
	router := gin.Default()

	router.GET("/", context.Heartbeat)
	router.POST("/records", context.CreateRecord)
	router.GET("/records", context.FindRecord)
	err = router.Run(context.Args.BindHost + ":" + strconv.Itoa(context.Args.BindPort))
	return
}

func main() {
	var (
		err     error
		context ServerContext
	)
	_, err = context.ParseCommandArgs()
	if err != nil {
		return
	}
	err = context.ConnectMongoDB()
	if err != nil {
		panic(err)
	}
	defer context.MongoSession.Close()

	err = context.SetupRoutes()
	if err != nil {
		panic(err)
	}
}

func (context *ServerContext) Heartbeat(c *gin.Context) {
	c.Status(200)
}

func (context *ServerContext) CreateRecord(c *gin.Context) {
	record := RecordGenerate()
	err := record.Create(context.MongoCollection)
	if err != nil {
		panic(err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
	} else {
		c.JSON(http.StatusOK, record)
	}
}

func (context *ServerContext) FindRecord(c *gin.Context) {
	query := make(map[string]string)
	for key, values := range c.Request.URL.Query() {
		if len(values) > 0 {
			query[key] = values[0]
		}
	}
	record, err := RecordFind(context.MongoCollection, query)
	if err != nil {
		panic(err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
	} else if record != nil {
		c.JSON(http.StatusOK, record)
	} else {
		c.Status(http.StatusNotFound)
	}
}

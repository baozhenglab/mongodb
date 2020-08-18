package sdkmongo

import (
	"context"
	"flag"
	"fmt"
	"math"
	"sync"
	"time"

	goservice "github.com/baozhenglab/go-sdk"
	"github.com/baozhenglab/go-sdk/logger"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

const (
	retryCount = 10
	KeyService = "mongodb"
)

type mongoOpt struct {
	Uri      string
	Database string
}

type mongoDB struct {
	logger       logger.Logger
	client       *mongo.Client
	isRunning    bool
	once         *sync.Once
	PingInterval int
	*mongoOpt
}

func NewMongoDB() goservice.PrefixRunnable {
	return &mongoDB{
		once:     new(sync.Once),
		mongoOpt: &mongoOpt{},
	}
}

func (*mongoDB) Name() string {
	return KeyService
}

func (*mongoDB) GetPrefix() string {
	return KeyService
}

func (mdb *mongoDB) Get() interface{} {
	mdb.once.Do(func() {
		if !mdb.isRunning && !mdb.isDisabled() {
			if err := mdb.getConnWithRetry(math.MaxInt32); err == nil {
				mdb.isRunning = true
				//gdb.db.SetLogger(gdb.logger)
			} else {
				mdb.logger.Fatalf("%s connection cannot reconnect\n", mdb.Name(), err)
			}
		}
	})

	if mdb.client == nil {
		return nil
	}
	service := &mongodbService{mdb.client, mdb.client.Database(mdb.Database)}
	return service
}

func (mdb *mongoDB) InitFlags() {
	prefix := fmt.Sprintf("%s-", mdb.Name())
	flag.StringVar(&mdb.Uri, prefix+"uri", "", "uri connect mongodb")
	flag.IntVar(&mdb.PingInterval, prefix+"ping-interval", 5, "mongodb database ping check interval")
	flag.StringVar(&mdb.Database, prefix+"database", "", "database name connect mongodb")
}

func (mdb *mongoDB) isDisabled() bool {
	return mdb.Uri == ""
}

func (mdb *mongoDB) Configure() error {
	if mdb.isDisabled() || mdb.isRunning {
		return nil
	}
	client, err := mongo.NewClient(options.Client().ApplyURI(mdb.Uri))
	if err != nil {
		return err
	}
	mdb.client = client
	mdb.logger = logger.GetCurrent().GetLogger(mdb.Name())
	return nil
}

func (mdb *mongoDB) Run() error {
	if mdb.isRunning && !mdb.isDisabled() {
		return nil
	}
	if err := mdb.Configure(); err != nil {
		return err
	}
	mdb.logger.Info("Connect to mongo DB at ", mdb.Uri, " ...")
	err := mdb.getConnWithRetry(retryCount)
	if err != nil {
		mdb.logger.Error("Error connect to mongodb database at ", mdb.Uri, ". ", err.Error())
		return err
	}
	mdb.isRunning = true
	return nil
}

func (mdb *mongoDB) Stop() <-chan bool {
	if mdb.client != nil {
		ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
		_ = mdb.client.Disconnect(ctx)
	}
	mdb.isRunning = false

	c := make(chan bool)
	go func() { c <- true }()
	return c
}

func (mdb *mongoDB) getConnWithRetry(retryCount int) (err error) {
	err = mdb.connect()

	if err != nil {
		for i := 1; i <= retryCount; i++ {
			time.Sleep(time.Second * 2)
			mdb.logger.Errorf("Retry to connect %s (%d).\n", mdb.Name(), i)
			err = mdb.connect()
			mdb.logger.Errorf("Retry to connect %s (%d). with err %s\n", mdb.Name(), i, err.Error())
			if err == nil {
				return err
			}
		}
	} else {
		// auto reconnect
		go mdb.reconnectIfNeeded()
	}
	return err
}

func (mdb *mongoDB) connect() (err error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	if err = mdb.client.Connect(ctx); err != nil {
		go cancel()
		return err
	}
	return mdb.client.Ping(ctx, readpref.Primary())
}

func (mdb *mongoDB) reconnectIfNeeded() {
	client := mdb.client
	for {
		ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
		if err := client.Ping(ctx, readpref.Primary()); err != nil {
			client.Disconnect(ctx)
			mdb.logger.Errorf("%s connection is gone, try to reconnect\n", mdb.Name())
			mdb.isRunning = false
			mdb.once = new(sync.Once)
			mdb.Get()
			return
		}
		time.Sleep(time.Second * time.Duration(mdb.PingInterval))
	}
}
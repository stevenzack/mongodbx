package mongodbx

import (
	"context"
	"log"
	"strings"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/x/mongo/driver/connstring"
)

var (
	pool     = sync.Map{}
	poolLock = sync.Mutex{}
)

func TakeClient(dsn string) (*mongo.Client, error) {
	poolLock.Lock()
	defer poolLock.Unlock()
	v, ok := pool.Load(dsn)
	if !ok {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		defer cancel()
		mgo, e := mongo.Connect(ctx, options.Client().ApplyURI(dsn))
		if e != nil {
			log.Println(e)
			return nil, e
		}
		pool.Store(dsn, mgo)
		return mgo, nil
	}
	return v.(*mongo.Client), nil
}

func TakeDatabase(dsn string) (*mongo.Database, error) {
	info, e := connstring.Parse(dsn)
	if e != nil {
		log.Println(e)
		return nil, e
	}

	client, e := TakeClient(strings.Replace(dsn, "/"+info.Database, "/admin", len("mongodb://")))
	if e != nil {
		log.Println(e)
		return nil, e
	}
	return client.Database(info.Database), nil
}

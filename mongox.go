package mongodbx

import (
	"context"
	"time"

	"github.com/StevenZack/tools/timeToolkit"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

// DialCollection DialCollection
func DialCollection(db *mongo.Database, name string) *mongo.Collection {
	coll := db.Collection(name)
	return coll
}

// CollectionExists check if collection exists
func CollectionExists(db *mongo.Database, coll string) (bool, error) {
	names, e := db.ListCollectionNames(context.TODO(), bson.M{})
	if e != nil {
		return false, e
	}

	for _, name := range names {
		if name == coll {
			return true, nil
		}
	}
	return false, nil
}

func BetweenTime(field string, start, end time.Time, m bson.M) bson.M {
	if start == timeToolkit.ZeroTime && end == timeToolkit.ZeroTime {
		return m
	}
	b := bson.M{}
	if start != timeToolkit.ZeroTime {
		b["$gte"] = start
	}
	if end != timeToolkit.ZeroTime {
		b["$lt"] = end
	}

	m[field] = b
	return m
}

func BetweenTimeD(field string, start, end time.Time, d bson.D) bson.D {
	if start == timeToolkit.ZeroTime && end == timeToolkit.ZeroTime {
		return d
	}

	b := bson.M{}
	if start != timeToolkit.ZeroTime {
		b["$gte"] = start
	}
	if end != timeToolkit.ZeroTime {
		b["$lt"] = end
	}

	d = append(d, bson.E{
		Key:   field,
		Value: b,
	})
	return d
}

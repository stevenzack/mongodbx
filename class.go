package mongodbx

import "go.mongodb.org/mongo-driver/bson"

type (
	SumClass struct {
		Sum float64 `bson:"sum"`
	}
	GroupCountClass struct {
		Id    string `bson:"_id"`
		Count int64  `bson:"count"`
	}
)

func NullableString(s string) interface{} {
	if s != "" {
		return s
	}
	return bson.M{"$exists": false}
}

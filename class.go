package mongodbx

type (
	SumClass struct {
		Sum float64 `bson:"sum"`
	}
	GroupCountClass struct {
		Id    string `bson:"_id"`
		Count int64  `bson:"count"`
	}
)

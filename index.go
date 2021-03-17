package mongodbx

import (
	"context"
	"errors"
	"log"
	"net/url"
	"strconv"
	"strings"

	"github.com/StevenZack/tools/strToolkit"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Index struct {
	Unique bool           `bson:"unique"`
	Key    map[string]int `bson:"key"`
	Name   string         `bson:"name"`
}

func (i Index) String() string {
	buf := new(strings.Builder)
	for k := range i.Key {
		buf.WriteString(k)
		buf.WriteString("_")
	}
	return buf.String()
}

func parseIndexes(indexes map[string]string) ([]mongo.IndexModel, error) {
	imodels := []mongo.IndexModel{}
	groups := make(map[string]mongo.IndexModel)
	for key, v := range indexes {
		vs, e := url.ParseQuery(strings.ReplaceAll(v, ",", "&"))
		if e != nil {
			return nil, errors.New("field '" + key + "', invalid value format:" + v)
		}
		groupseq := 1
		unique := false
		group := ""
		single := 0
		for k := range vs {
			switch k {
			case "groupseq":
				sequence := vs.Get(k)
				if sequence != "" {
					groupseq, e = strconv.Atoi(sequence)
					if e != nil {
						return nil, errors.New("field '" + key + "', invalid groupseq format:" + v)
					}
					if groupseq != -1 {
						groupseq = 1
					}
				}
			case "unique":
				unique = vs.Get("unique") == "true"
			case "group":
				group = vs.Get(k)
			case "single":
				sequence := vs.Get(k)
				if sequence != "" {
					single, e = strconv.Atoi(sequence)
					if e != nil {
						return nil, errors.New("field '" + key + "', invalid single format:" + v)
					}
					if single != -1 {
						single = 1
					} else {
						single = -1
					}
				}
			default:
				return nil, errors.New("field '" + key + "', unsupported key:" + k)
			}
		}

		if group == "" {
			single = 1
		}

		//single index
		if single != 0 {
			imodel := mongo.IndexModel{
				Keys: bson.D{
					{
						Key:   strToolkit.SubBefore(key, ",", key),
						Value: single,
					},
				},
				Options: options.Index(),
			}
			if unique {
				imodel.Options.SetUnique(unique)
			}
			imodels = append(imodels, imodel)
		}
		//group index
		if group != "" {
			imodel, ok := groups[group]
			if !ok {
				imodel = mongo.IndexModel{
					Keys: bson.D{
						{
							Key:   strToolkit.SubBefore(key, ",", key),
							Value: groupseq,
						},
					},
					Options: options.Index(),
				}
				if strings.HasPrefix(group, "unique") {
					imodel.Options.SetUnique(true)
				}
				groups[group] = imodel
				continue
			}
			imodel.Keys = append(imodel.Keys.(bson.D), bson.E{
				Key:   strToolkit.SubBefore(key, ",", key),
				Value: groupseq,
			})
			groups[group] = imodel
		}
	}

	//add group indexes
	for _, v := range groups {
		imodels = append(imodels, v)
	}

	return imodels, nil
}

// CreateIndex creates indexes for coll
func CreateIndex(coll *mongo.Collection, imodels []mongo.IndexModel) error {
	if len(imodels) == 0 {
		return nil
	}
	_, e := coll.Indexes().CreateMany(context.TODO(), imodels)
	return e
}

// CreateIndexIfNotExists create indexes if collection doesn't exists
func CreateIndexIfNotExists(db *mongo.Database, collname string, indexes map[string]string) (bool, error) {
	b, e := CollectionExists(db, collname)
	if e != nil {
		return false, e
	}

	imodels, e := parseIndexes(indexes)
	if e != nil {
		return false, e
	}

	coll := DialCollection(db, collname)

	if b {
		m := make(map[string]mongo.IndexModel)
		for _, imodel := range imodels {
			m[imodelToString(imodel)] = imodel
		}

		cursor, e := coll.Indexes().List(context.TODO())
		if e != nil {
			return false, e
		}
		defer cursor.Close(context.TODO())
		idx := []Index{}
		e = cursor.All(context.TODO(), &idx)
		if e != nil {
			return false, e
		}

		m2 := make(map[string]Index)
		for _, i := range idx {
			if i.Name == "_id_" {
				continue
			}
			m2[i.String()] = i
			imodel, ok := m[i.String()]
			if !ok {
				log.Println(collname, " collection", ",index to be dropped:", i.String())
				continue
			}
			if isUnique(imodel) != i.Unique {
				log.Println(collname, " collection", ",index.unique inconsistant:"+i.String())
				continue
			}
		}

		for _, imodel := range imodels {
			_, ok := m2[imodelToString(imodel)]
			if !ok {
				log.Println(collname, " collection", ",index to be created:", imodelToString(imodel))
				continue
			}
		}
		return false, nil
	}

	return true, CreateIndex(coll, imodels)
}

func imodelToString(i mongo.IndexModel) string {
	d := i.Keys.(bson.D)
	buf := new(strings.Builder)
	for _, e := range d {
		buf.WriteString(e.Key)
		buf.WriteString("_")
	}
	return buf.String()
}

func isUnique(i mongo.IndexModel) bool {
	if i.Options == nil {
		return false
	}
	if i.Options.Unique == nil {
		return false
	}
	return *i.Options.Unique
}

package main

import (
	"github.com/tuvistavie/securerandom"

	mgo "gopkg.in/mgo.v2"
	bson "gopkg.in/mgo.v2/bson"
)

type Record struct {
	id     bson.ObjectId `bson:"_id,omitempty"`
	Data0  string        `json:"data0"`
	Data1  string        `json:"data1"`
	Data2  string        `json:"data2"`
	Data3  string        `json:"data3"`
	Data4  string        `json:"data4"`
	Data5  string        `json:"data5"`
	Data6  string        `json:"data6"`
	Data7  string        `json:"data7"`
	Data8  string        `json:"data8"`
	Data9  string        `json:"data9"`
	Data10 string        `json:"data10"`
	Data11 string        `json:"data11"`
	Data12 string        `json:"data12"`
	Data13 string        `json:"data13"`
	Data14 string        `json:"data14"`
	Data15 string        `json:"data15"`
	Data16 string        `json:"data16"`
	Data17 string        `json:"data17"`
	Data18 string        `json:"data18"`
	Data19 string        `json:"data19"`
}

func (self *Record) Create(collection *mgo.Collection) error {
	return collection.Insert(self)
}

func RecordGenerate() *Record {
	return &Record{
		Data0:  generateSecureRandomHex(64),
		Data1:  generateSecureRandomHex(64),
		Data2:  generateSecureRandomHex(64),
		Data3:  generateSecureRandomHex(64),
		Data4:  generateSecureRandomHex(64),
		Data5:  generateSecureRandomHex(64),
		Data6:  generateSecureRandomHex(64),
		Data7:  generateSecureRandomHex(64),
		Data8:  generateSecureRandomHex(64),
		Data9:  generateSecureRandomHex(64),
		Data10: generateSecureRandomHex(64),
		Data11: generateSecureRandomHex(64),
		Data12: generateSecureRandomHex(64),
		Data13: generateSecureRandomHex(64),
		Data14: generateSecureRandomHex(64),
		Data15: generateSecureRandomHex(64),
		Data16: generateSecureRandomHex(64),
		Data17: generateSecureRandomHex(64),
		Data18: generateSecureRandomHex(64),
		Data19: generateSecureRandomHex(64),
	}
}

func generateSecureRandomHex(n int) string {
	hex, err := securerandom.Hex(n)
	if err != nil {
		panic(err)
	}
	return hex
}

func RecordFind(collection *mgo.Collection, query interface{}) (result *Record, err error) {
	results := make([]*Record, 0, 1)
	err = collection.Find(query).Limit(1).All(&results)
	if len(results) > 0 {
		result = results[0]
	}
	return
}

package metric

import (
	"encoding/json"

	util "github.com/TerrexTech/go-commonutils/commonutil"
	"github.com/TerrexTech/uuuid"
	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/mongodb/mongo-go-driver/bson/objectid"
	"github.com/pkg/errors"
)

// AggregateID is the global AggregateID for Metrics Aggregate.
const AggregateID int8 = 5

//Metric struct
type Metric struct {
	ID            objectid.ObjectID `bson:"id,omitempty" json:"id,omitempty"`
	MetricID      uuuid.UUID        `bson:"metricID,omitempty" json:"metricID,omitempty"`
	ItemID        uuuid.UUID        `bson:"itemID,omitempty" json:"itemID,omitempty"`
	DeviceID      uuuid.UUID        `bson:"deviceID,omitempty" json:"deviceID,omitempty"`
	Timestamp     int64             `bson:"timestamp,omitempty" json:"timestamp,omitempty"`
	TempIn        float64           `bson:"tempIn,omitempty" json:"tempIn,omitempty"`
	Humidity      float64           `bson:"humidity,omitempty" json:"humidity,omitempty"`
	Ethylene      float64           `bson:"ethylene,omitempty" json:"ethylene,omitempty"`
	CarbonDioxide float64           `bson:"carbonDioxide,omitempty" json:"carbonDioxide,omitempty"`
}

// MarshalBSON returns bytes of BSON-type.
func (m *Metric) MarshalBSON() ([]byte, error) {
	mm := map[string]interface{}{
		"metricID":      m.MetricID.String(),
		"itemID":        m.ItemID.String(),
		"deviceID":      m.DeviceID.String(),
		"timestamp":     m.Timestamp,
		"tempIn":        m.TempIn,
		"humidity":      m.Humidity,
		"ethylene":      m.Ethylene,
		"carbonDioxide": m.CarbonDioxide,
	}

	if m.ID != objectid.NilObjectID {
		mm["_id"] = m.ID
	}

	mar, err := bson.Marshal(mm)
	if err != nil {
		err = errors.Wrap(err, "MarshalJSON Error")
	}
	return mar, err
}

// MarshalJSON returns bytes of JSON-type.
func (m *Metric) MarshalJSON() ([]byte, error) {
	mm := map[string]interface{}{
		"metricID":      m.MetricID.String(),
		"itemID":        m.ItemID.String(),
		"deviceID":      m.DeviceID.String(),
		"timestamp":     m.Timestamp,
		"tempIn":        m.TempIn,
		"humidity":      m.Humidity,
		"ethylene":      m.Ethylene,
		"carbonDioxide": m.CarbonDioxide,
	}

	if m.ID != objectid.NilObjectID {
		mm["_id"] = m.ID.Hex()
	}

	mar, err := json.Marshal(mm)
	if err != nil {
		err = errors.Wrap(err, "MarshalJSON Error")
	}
	return mar, err
}

// UnmarshalBSON returns BSON-type from bytes.
func (m *Metric) UnmarshalBSON(in []byte) error {
	metMap := make(map[string]interface{})
	err := bson.Unmarshal(in, metMap)
	if err != nil {
		err = errors.Wrap(err, "UnmarshalBSON Error")
		return err
	}

	err = m.unmarshalFromMap(metMap)
	if err != nil {
		err = errors.Wrap(err, "UnmarshalBSON Error")
	}
	return err
}

// UnmarshalJSON returns JSON-type from bytes.
func (m *Metric) UnmarshalJSON(in []byte) error {
	metMap := make(map[string]interface{})
	err := json.Unmarshal(in, &metMap)
	if err != nil {
		err = errors.Wrap(err, "UnmarshalBSON Error")
		return err
	}

	err = m.unmarshalFromMap(metMap)
	if err != nil {
		err = errors.Wrap(err, "UnmarshalJSON Error")
	}
	return err
}

func (m *Metric) unmarshalFromMap(metMap map[string]interface{}) error {
	var err error
	var assertOK bool

	if metMap["_id"] != nil {
		m.ID, assertOK = metMap["_id"].(objectid.ObjectID)
		if !assertOK {
			m.ID, err = objectid.FromHex(metMap["_id"].(string))
			if err != nil {
				err = errors.Wrap(err, "Error while asserting ObjectID")
				return err
			}
		}
	}

	if metMap["metricID"] != nil {
		metricIDstr, assertOK := metMap["metricID"].(string)
		if !assertOK {
			return errors.New("error asserting MetricID")
		}
		m.MetricID, err = uuuid.FromString(metricIDstr)
		if err != nil {
			err = errors.Wrap(err, "Error while asserting metricID")
			return err
		}
	}

	if metMap["itemID"] != nil {
		itemIDStr, assertOK := metMap["itemID"].(string)
		if !assertOK {
			return errors.New("error asserting ItemID")
		}
		m.ItemID, err = uuuid.FromString(itemIDStr)
		if err != nil {
			err = errors.Wrap(err, "Error while asserting itemID")
			return err
		}
	}

	if metMap["deviceID"] != nil {
		deviceIDStr, assertOK := metMap["deviceID"].(string)
		if !assertOK {
			return errors.New("error asserting DeviceID")
		}
		m.DeviceID, err = uuuid.FromString(deviceIDStr)
		if err != nil {
			err = errors.Wrap(err, "Error while asserting deviceID")
			return err
		}
	}

	if metMap["timestamp"] != nil {
		m.Timestamp, err = util.AssertInt64(metMap["timestamp"])
		if err != nil {
			err = errors.Wrap(err, "Error while asserting Timestamp")
			return err
		}
	}

	if metMap["tempIn"] != nil {
		m.TempIn, err = util.AssertFloat64(metMap["tempIn"])
		if err != nil {
			err = errors.Wrap(err, "Error while asserting tempIn")
			return err
		}
	}

	if metMap["humidity"] != nil {
		m.Humidity, err = util.AssertFloat64(metMap["humidity"])
		if err != nil {
			err = errors.Wrap(err, "Error while asserting humidity")
			return err
		}
	}

	if metMap["ethylene"] != nil {
		m.Ethylene, err = util.AssertFloat64(metMap["ethylene"])
		if err != nil {
			err = errors.Wrap(err, "Error while asserting ethylene")
			return err
		}
	}

	if metMap["carbonDioxide"] != nil {
		m.CarbonDioxide, err = util.AssertFloat64(metMap["carbonDioxide"])
		if err != nil {
			err = errors.Wrap(err, "Error while asserting carbonDioxide")
			return err
		}
	}

	return nil
}

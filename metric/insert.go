package metric

import (
	"encoding/json"
	"log"
	"time"

	"github.com/TerrexTech/go-eventstore-models/model"
	"github.com/TerrexTech/go-mongoutils/mongo"
	"github.com/TerrexTech/uuuid"
	"github.com/mongodb/mongo-go-driver/bson/objectid"
	"github.com/pkg/errors"
)

// Insert handles "insert" events.
func Insert(collection *mongo.Collection, event *model.Event) *model.KafkaResponse {
	metric := &Metric{}
	err := json.Unmarshal(event.Data, metric)
	if err != nil {
		err = errors.Wrap(err, "Insert: Error while unmarshalling Event-data")
		log.Println(err)
		return &model.KafkaResponse{
			AggregateID:   event.AggregateID,
			CorrelationID: event.CorrelationID,
			Error:         err.Error(),
			ErrorCode:     InternalError,
			UUID:          event.TimeUUID,
		}
	}

	if metric.MetricID == (uuuid.UUID{}) {
		metricID, err := uuuid.NewV4()
		if err != nil {
			err = errors.Wrap(err, "Insert: Error generating MetricID")
			log.Println(err)
			return &model.KafkaResponse{
				AggregateID:   event.AggregateID,
				CorrelationID: event.CorrelationID,
				Error:         err.Error(),
				ErrorCode:     InternalError,
				UUID:          event.TimeUUID,
			}
		}
		metric.MetricID = metricID

		//Insert timestamp
		metric.Timestamp = time.Now().Unix()
	}

	if metric.ItemID == (uuuid.UUID{}) {
		err := errors.New("Insert: ItemID is required")
		log.Println(err)
		return &model.KafkaResponse{
			AggregateID:   event.AggregateID,
			CorrelationID: event.CorrelationID,
			Error:         err.Error(),
			ErrorCode:     InternalError,
			UUID:          event.TimeUUID,
		}
	}

	if metric.DeviceID == (uuuid.UUID{}) {
		err := errors.New("Insert: DeviceID is required")
		log.Println(err)
		return &model.KafkaResponse{
			AggregateID:   event.AggregateID,
			CorrelationID: event.CorrelationID,
			Error:         err.Error(),
			ErrorCode:     InternalError,
			UUID:          event.TimeUUID,
		}
	}

	insertResult, err := collection.InsertOne(metric)
	if err != nil {
		err = errors.Wrap(err, "Insert: Error Inserting Metric into Mongo")
		log.Println(err)
		return &model.KafkaResponse{
			AggregateID:   event.AggregateID,
			CorrelationID: event.CorrelationID,
			Error:         err.Error(),
			ErrorCode:     DatabaseError,
			UUID:          event.TimeUUID,
		}
	}
	insertedID, assertOK := insertResult.InsertedID.(objectid.ObjectID)
	if !assertOK {
		err = errors.New("error asserting InsertedID from InsertResult to ObjectID")
		err = errors.Wrap(err, "Insert")
		log.Println(err)
		return &model.KafkaResponse{
			AggregateID:   event.AggregateID,
			CorrelationID: event.CorrelationID,
			Error:         err.Error(),
			ErrorCode:     InternalError,
			UUID:          event.TimeUUID,
		}
	}

	metric.ID = insertedID
	result, err := json.Marshal(metric)
	if err != nil {
		err = errors.Wrap(err, "Insert: Error marshalling Metric Insert-result")
		log.Println(err)
		return &model.KafkaResponse{
			AggregateID:   event.AggregateID,
			CorrelationID: event.CorrelationID,
			Error:         err.Error(),
			ErrorCode:     InternalError,
			UUID:          event.TimeUUID,
		}
	}

	return &model.KafkaResponse{
		AggregateID:   event.AggregateID,
		CorrelationID: event.CorrelationID,
		Result:        result,
		UUID:          event.TimeUUID,
	}
}
package metric

import (
	"encoding/json"
	"log"
	"testing"
	"time"

	"github.com/TerrexTech/go-eventstore-models/model"
	"github.com/TerrexTech/uuuid"
	"github.com/mongodb/mongo-go-driver/bson"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

// TestMetrics only tests basic pre-processing error-checks for Aggregate functions.
func TestMetrics(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "MetricAggregate Suite")
}

func toBSON(data interface{}) (*bson.Document, error) {
	doc, err := bson.NewDocumentEncoder().EncodeDocument(data)
	if err != nil {
		return nil, err
	}

	// If no object ID is specified, delete the existing so it gets
	// automatically generated.
	dataObjectIDField := doc.Lookup("_id")

	if dataObjectIDField != nil {
		dataObjectID := dataObjectIDField.ObjectID().String()
		zeroObjectID := "ObjectID(\"000000000000000000000000\")"
		if dataObjectID == zeroObjectID {
			doc.Delete("_id")
		}
	}
	return doc, nil
}

var _ = Describe("MetricAggregate", func() {
	Describe("insert", func() {
		var metric *Metric

		BeforeEach(func() {
			metricID, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())

			deviceID, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())

			itemID, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())

			soldItem, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())

			soldItem2, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())

			timestamp := time.Now().Unix()
			Expect(err).ToNot(HaveOccurred())

			metric = &Metric{
				MetricID:      metricID,
				ItemID:        itemID,
				DeviceID:      deviceID,
				Timestamp:     timestamp,
				TempIn:        23.5,
				Humidity:      45,
				Ethylene:      50,
				CarbonDioxide: 400,
				Items: []SoldItem{
					SoldItem{
						ItemID:  soldItem,
						Barcode: "test",
						Weight:  22.4,
						Lot:     "234sdafs",
						SKU:     "teafsdf",
					},
					SoldItem{
						ItemID:  soldItem2,
						Barcode: "test2",
						Weight:  22.42,
						Lot:     "234sdafs2",
						SKU:     "teafsdf2",
					},
				},
			}
		})

		It("should return error if itemID is empty", func() {
			metric.ItemID = uuuid.UUID{}
			log.Println(metric)
			marshalMetric, err := toBSON(metric)
			log.Println(marshalMetric.ToExtJSON(true))
			Expect(err).ToNot(HaveOccurred())

			timeUUID, err := uuuid.NewV1()
			Expect(err).ToNot(HaveOccurred())
			cid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			uid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())

			mockEvent := &model.Event{
				Action:        "insert",
				CorrelationID: cid,
				AggregateID:   1,
				// Data:          marshalMetric,
				Timestamp:  time.Now(),
				UserUUID:   uid,
				TimeUUID:   timeUUID,
				Version:    3,
				YearBucket: 2018,
			}
			kr := Insert(nil, mockEvent)
			Expect(kr.AggregateID).To(Equal(mockEvent.AggregateID))
			Expect(kr.CorrelationID).To(Equal(mockEvent.CorrelationID))
			Expect(kr.Error).ToNot(BeEmpty())
			Expect(kr.ErrorCode).To(Equal(int16(InternalError)))
			Expect(kr.UUID).To(Equal(mockEvent.TimeUUID))
		})

		It("should return error if deviceID is empty", func() {
			metric.DeviceID = uuuid.UUID{}
			marshalMetric, err := json.Marshal(metric)
			Expect(err).ToNot(HaveOccurred())

			timeUUID, err := uuuid.NewV1()
			Expect(err).ToNot(HaveOccurred())
			cid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			uid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())

			mockEvent := &model.Event{
				Action:        "insert",
				CorrelationID: cid,
				AggregateID:   1,
				Data:          marshalMetric,
				Timestamp:     time.Now(),
				UserUUID:      uid,
				TimeUUID:      timeUUID,
				Version:       3,
				YearBucket:    2018,
			}
			kr := Insert(nil, mockEvent)
			Expect(kr.AggregateID).To(Equal(mockEvent.AggregateID))
			Expect(kr.CorrelationID).To(Equal(mockEvent.CorrelationID))
			Expect(kr.Error).ToNot(BeEmpty())
			Expect(kr.ErrorCode).To(Equal(int16(InternalError)))
			Expect(kr.UUID).To(Equal(mockEvent.TimeUUID))
		})
	})
})

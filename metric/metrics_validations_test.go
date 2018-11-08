package metric

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/TerrexTech/go-eventstore-models/model"
	"github.com/TerrexTech/uuuid"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

// TestMetrics only tests basic pre-processing error-checks for Aggregate functions.
func TestMetrics(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "MetricAggregate Suite")
}

var _ = Describe("MetricAggregate", func() {
	Describe("insert", func() {
		var metric *Metric

		BeforeEach(func() {
			// metricID, err := uuuid.NewV4()
			deviceID, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())

			itemID, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())

			metric = &Metric{
				ItemID:        itemID,
				DeviceID:      deviceID,
				SKU:           "test-sku",
				Timestamp:     time.Now().Unix(),
				TempIn:        23.5,
				Humidity:      45,
				Ethylene:      50,
				CarbonDioxide: 400,
			}
		})

		It("should return error if itemID is empty", func() {
			metric.ItemID = uuuid.UUID{}
			marshalMetric, err := json.Marshal(metric)
			Expect(err).ToNot(HaveOccurred())

			uuid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			cid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			uid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())

			mockEvent := &model.Event{
				EventAction:   "insert",
				CorrelationID: cid,
				AggregateID:   1,
				Data:          marshalMetric,
				NanoTime:      time.Now().UnixNano(),
				UserUUID:      uid,
				UUID:          uuid,
				Version:       3,
				YearBucket:    2018,
			}
			kr := Insert(nil, mockEvent)
			Expect(kr.AggregateID).To(Equal(mockEvent.AggregateID))
			Expect(kr.CorrelationID).To(Equal(mockEvent.CorrelationID))
			Expect(kr.Error).ToNot(BeEmpty())
			Expect(kr.ErrorCode).To(Equal(int16(InternalError)))
			Expect(kr.UUID).To(Equal(mockEvent.UUID))
		})

		It("should return error if deviceID is empty", func() {
			metric.DeviceID = uuuid.UUID{}
			marshalMetric, err := json.Marshal(metric)
			Expect(err).ToNot(HaveOccurred())

			uuid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			cid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			uid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())

			mockEvent := &model.Event{
				EventAction:   "insert",
				CorrelationID: cid,
				AggregateID:   1,
				Data:          marshalMetric,
				NanoTime:      time.Now().UnixNano(),
				UserUUID:      uid,
				UUID:          uuid,
				Version:       3,
				YearBucket:    2018,
			}
			kr := Insert(nil, mockEvent)
			Expect(kr.AggregateID).To(Equal(mockEvent.AggregateID))
			Expect(kr.CorrelationID).To(Equal(mockEvent.CorrelationID))
			Expect(kr.Error).ToNot(BeEmpty())
			Expect(kr.ErrorCode).To(Equal(int16(InternalError)))
			Expect(kr.UUID).To(Equal(mockEvent.UUID))
		})

		It("should return error if SKU is empty", func() {
			metric.SKU = ""
			marshalMetric, err := json.Marshal(metric)
			Expect(err).ToNot(HaveOccurred())

			uuid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			cid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			uid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())

			mockEvent := &model.Event{
				EventAction:   "insert",
				CorrelationID: cid,
				AggregateID:   1,
				Data:          marshalMetric,
				NanoTime:      time.Now().UnixNano(),
				UserUUID:      uid,
				UUID:          uuid,
				Version:       3,
				YearBucket:    2018,
			}
			kr := Insert(nil, mockEvent)
			Expect(kr.AggregateID).To(Equal(mockEvent.AggregateID))
			Expect(kr.CorrelationID).To(Equal(mockEvent.CorrelationID))
			Expect(kr.Error).ToNot(BeEmpty())
			Expect(kr.ErrorCode).To(Equal(int16(InternalError)))
			Expect(kr.UUID).To(Equal(mockEvent.UUID))
		})
	})
})

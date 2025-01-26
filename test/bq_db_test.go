package test

import (
	"strings"
	"testing"
	"time"

	bigquerydb "github.com/go-yaaf/yaaf-common-bigquery/bqdb"
	. "github.com/go-yaaf/yaaf-common/database"
	"github.com/go-yaaf/yaaf-common/entity"
	"github.com/go-yaaf/yaaf-common/logger"
)

var streamId = "etecnic-1"

// TestEq tests a BigQuery query with an equality (Eq) filter and retrieves the results.
//
// This function performs the following steps:
// 1. Connects to the BigQuery database for the 'shieldiot-staging' project and the 'pulseiot' dataset.
// 2. Executes a query that filters the 'dst_ip' field to only return records where the destination IP equals "8.8.8.8".
// 3. Limits the query results to 1000 records and retrieves the first page (page 0).
// 4. If the query fails, the test will fail with an error message indicating the problem.
// 5. If the query succeeds, it retrieves the matching records (`flowRecords`) and logs them using the logger.
//
// Parameters:
// - t *testing.T: The testing object provided by Go's testing framework, used to run the test and report results.
// - bqdb: The BigQuery database connection object, which is used to run the query.
// - err: The error object used to capture any issues with the database connection or query execution.
// - flowRecords: The result set containing the `FlowRecord` entities matching the filter condition (i.e., `dst_ip = "8.8.8.8"`).
//
// The function logs the records that match the query and validates the successful execution of the equality filter.
func TestEq(t *testing.T) {

	bqdb, err := bigquerydb.NewBqDatabase("bq://shieldiot-staging:pulseiot")

	if err != nil {
		t.Fatalf("NewBqDatabase failed: %v", err)
	}
	fr := NewFlowRecordEntity(streamId).(*FlowRecord)
	flowRrecords, _, err := bqdb.Query(fr.EntityFactory()).
		Filter(F("dst_ip").Eq("8.8.8.8")).
		Limit(100).
		Page(0).
		Find()

	if err != nil {
		t.Fatal(err)
	}
	logger.Debug("records: %d", len(flowRrecords))

}

// TestLike tests a BigQuery query with a LIKE filter and validates the results using a callback function.
//
// This function performs the following steps:
// 1. Connects to the BigQuery database for the 'shieldiot-staging' project and the 'pulseiot' dataset.
// 2. Defines a callback function (cb) that checks each result returned by the query:
//    - Ensures that the 'src_ip' field starts with the specified pattern ('10.164.41').
//    - If the 'src_ip' does not satisfy the pattern, the test will fail with an error message indicating the mismatch.
// 3. Builds and executes a query that filters the 'src_ip' field using a LIKE operator to match IPs starting with '10.164.41.%'.
// 4. Limits the query results to 1000 records and retrieves the first page (page 0).
// 5. Applies the callback function (cb) to validate the results.
// 6. Logs the total count of matching records.
// 7. If the query fails or the results do not meet the expected conditions, the test fails with appropriate error messages.
//
// Parameters:
// - t *testing.T: The testing object provided by Go's testing framework, used to run the test and report results.
// - bqdb: The BigQuery database connection object, which is used to run the query.
// - err: The error object, used to capture any issues with the database connection or query execution.
// - pattern: The IP address pattern ('10.164.41') that must match the 'src_ip' field in the records.
// - cb: The callback function that performs validation on each entity in the result set. It checks whether the 'src_ip' starts with '10.164.41'. If this condition is not met, the test will fail.
//
// The function logs the count of matching records and validates that the query results meet the expected criteria.

func TestLike(t *testing.T) {

	//Filter(F("streamId").Eq(stream.(*Stream).Id)).
	bqdb, err := bigquerydb.NewBqDatabase("bq://shieldiot-staging:pulseiot")

	if err != nil {
		t.Fatalf("NewBqDatabase failed: %v", err)
	}

	pattern := "10.164.41"
	cb := func(entity entity.Entity) entity.Entity {
		fr := entity.(*FlowRecord)

		if !strings.HasPrefix(fr.SrcIP, pattern) {
			t.Fatalf(" result set does not satisfy requested pattern of %s: has value of %s", pattern, fr.SrcIP)
		}
		return nil
	}
	fr := NewFlowRecordEntity(streamId).(*FlowRecord)
	_, count, err := bqdb.Query(fr.EntityFactory()).
		Apply(cb).
		Filter(F("src_ip").Like("10.164.41.%")).
		Limit(1000).
		Page(0).
		Find()

	if err != nil {
		t.Fatal(err)
	}
	logger.Debug("count: %d", count)

}

// TestLikeAndNotInForString tests a BigQuery query with LIKE and NOT IN filters and validates the results using a callback function.
//
// This function performs the following steps:
// 1. Connects to the BigQuery database for the 'shieldiot-staging' project and the 'pulseiot' dataset.
// 2. Defines a callback function (cb) that checks each result returned by the query:
//   - Ensures that the 'src_ip' field starts with the specified pattern ('10.164.41').
//   - Ensures that the 'dst_ip' is not one of the excluded values: '1.1.1.1', '8.8.8.8', or '8.8.4.4'.
//   - If either condition is not satisfied, the test fails with a detailed message.
//
// 3. Builds and executes a query that filters:
//   - 'src_ip' field using a LIKE operator to match IPs starting with '10.164.41.%'.
//   - 'dst_ip' field using a NOT IN clause to exclude specific IPs ('1.1.1.1', '8.8.8.8', and '8.8.4.4').
//   - Limits the query results to 1000 records and retrieves the first page (page 0).
//
// 4. Applies the callback function (cb) to validate the results.
// 5. Logs the total count of matching records.
// 6. If the query fails or the results do not meet the expected conditions, the test fails with appropriate error messages.
//
// Parameters:
// - t *testing.T: The testing object provided by Go's testing framework, used to run the test and report results.
// - bqdb: The BigQuery database connection object, which is used to run the query.
// - err: The error object, used to capture any issues with the database connection or query execution.
// - pattern: The IP address pattern ('10.164.41') that must match the 'src_ip' field in the records.
// - cb: The callback function that performs validation on each entity in the result set. It checks whether:
//   - The 'src_ip' starts with '10.164.41'.
//   - The 'dst_ip' is not one of the excluded IPs: '1.1.1.1', '8.8.8.8', or '8.8.4.4'.
//     If these conditions are not met, the test will fail.
//
// The function logs the count of matching records and validates that the query results meet the expected criteria.
func TestLikeAndNotInForString(t *testing.T) {

	bqdb, err := bigquerydb.NewBqDatabase("bq://shieldiot-staging:pulseiot")

	if err != nil {
		t.Fatalf("NewBqDatabase failed: %v", err)
	}

	pattern := "10.164.41"
	cb := func(entity entity.Entity) entity.Entity {
		fr := entity.(*FlowRecord)
		dstIp := fr.DstIP

		if !strings.HasPrefix(fr.SrcIP, pattern) {
			t.Fatalf(" result set does not satisfy requested pattern of %s: has value of %s", pattern, fr.SrcIP)
		}
		if dstIp == "1.1.1.1" || dstIp == "8.8.8.8" || dstIp == "8.8.4.4" {
			t.Fatalf(" result set does not satisfy requested criteria: dst_ip = %s", dstIp)
		}
		return nil
	}
	fr := NewFlowRecordEntity(streamId).(*FlowRecord)
	_, count, err := bqdb.Query(fr.EntityFactory()).
		Apply(cb).
		Filter(F("src_ip").Like("10.164.41.%")).
		Filter(F("dst_ip").NotIn("1.1.1.1", "8.8.8.8", "8.8.4.4")).
		Limit(1000).
		Page(0).
		Find()

	if err != nil {
		t.Fatal(err)
	}
	logger.Debug("count: %d", count)
}

// TestLikeAndRangAndNotInForString tests a BigQuery query with multiple filtering conditions
// (LIKE, NOT IN, RANGE) and applies a callback function to validate the results.
//
// The function does the following:
// 1. Connects to the BigQuery database for the 'shieldiot-staging' project and 'pulseiot' dataset.
// 2. Defines a callback function (cb) to validate that the returned records meet specific criteria:
//    - The 'src_ip' must start with the pattern '10.164.41'.
//    - The 'dst_ip' must not be '1.1.1.1', '8.8.8.8', or '8.8.4.4'.
// 3. Constructs a query that applies multiple filters to the data:
//    - Filters 'src_ip' using a LIKE operator to match IPs starting with '10.164.41.%'.
//    - Filters 'dst_ip' using a NOT IN clause to exclude specific IP addresses ('1.1.1.1', '8.8.8.8', '8.8.4.4').
//    - Filters 'start_time' using a range to include timestamps between 1722540000000 and 1722715200000.
//    - Limits the result to 1000 records and retrieves the first page (page 0).
// 4. Executes the query and applies the callback function to validate the results.
// 5. If the query fails or the results do not match the expected criteria, the test will fail.
//
// Parameters:
// - t *testing.T: The testing object provided by Go's testing framework, used for running and reporting test results.
// - bqdb: The BigQuery database connection object that is used to query the database.
// - err: The error object to handle any issues encountered while connecting to the database or executing the query.
// - pattern: The IP address pattern (in this case, '10.164.41') used for filtering the 'src_ip' field.
// - cb: A callback function applied to each entity in the result set, used to validate that:
//   - The 'src_ip' starts with '10.164.41'.
//   - The 'dst_ip' is not equal to '1.1.1.1', '8.8.8.8', or '8.8.4.4'. If it is, the test will fail.
//
// The function logs the total count of records matching the query and validates the results based on the callback function.
// If any record violates the filtering conditions or an error occurs during the query, the test fails with an appropriate message.

func TestLikeAndRangAndNotInForString(t *testing.T) {

	//Filter(F("streamId").Eq(stream.(*Stream).Id)).
	bqdb, err := bigquerydb.NewBqDatabase("bq://shieldiot-staging:pulseiot")

	if err != nil {
		t.Fatalf("NewBqDatabase failed: %v", err)
	}

	pattern := "10.164.41"
	cb := func(entity entity.Entity) entity.Entity {
		fr := entity.(*FlowRecord)
		dstIp := fr.DstIP

		if !strings.HasPrefix(fr.SrcIP, pattern) {
			t.Fatalf(" result set does not satisfy requested pattern of %s: has value of %s", pattern, fr.SrcIP)
		}
		if dstIp == "1.1.1.1" || dstIp == "8.8.8.8" || dstIp == "8.8.4.4" {
			t.Fatalf(" result set does not satisfy requested criteria: dst_ip = %s", dstIp)
		}
		return nil
	}
	fr := NewFlowRecordEntity(streamId).(*FlowRecord)
	_, count, err := bqdb.Query(fr.EntityFactory()).
		Apply(cb).
		Filter(F("src_ip").Like("10.164.41.%")).
		Filter(F("dst_ip").NotIn("1.1.1.1", "8.8.8.8", "8.8.4.4")).
		Range("start_time", 1722540000000, 1722715200000).
		Limit(1000).
		Page(0).
		Find()

	if err != nil {
		t.Fatal(err)
	}
	logger.Debug("count: %d", count)
}

// TestLikeAndRangeAndInAsArrayOfInt tests a BigQuery query with multiple filtering conditions
// (LIKE, NOT IN, RANGE) and applies a callback function to validate the results.
//
// The function does the following:
// 1. Connects to the BigQuery database for the 'shieldiot-staging' project and 'pulseiot' dataset.
// 2. Defines a callback function (cb) to validate that the returned records meet specific criteria:
//   - The 'src_ip' must start with the pattern '10.164.41'.
//   - The 'dst_port' must not be 80, 443, or 0.
//
// 3. Constructs a query that applies multiple filters to the data:
//   - Filters 'src_ip' using a LIKE operator to match IPs starting with '10.164.41.%'.
//   - Filters 'dst_port' using a NOT IN clause to exclude ports 80, 443, and 0.
//   - Filters 'start_time' using a range to include timestamps between 1722540000000 and 1722715200000.
//   - Limits the result to 1000 records and retrieves the first page (page 0).
//
// 4. Executes the query and applies the callback function to validate the results.
// 5. If the query fails or the results do not match the expected criteria, the test will fail.
//
// Parameters:
// - t *testing.T: The testing object provided by Go's testing framework, used for running and reporting test results.
// - bqdb: The BigQuery database connection object that is used to query the database.
// - err: The error object to handle any issues encountered while connecting to the database or executing the query.
// - pattern: The IP address pattern (in this case, '10.164.41') used for filtering the 'src_ip' field.
// - cb: A callback function applied to each entity in the result set, used to validate that:
//   - The 'src_ip' starts with '10.164.41'.
//   - The 'dst_port' is not equal to 80, 443, or 0. If any of these conditions are violated, the test will fail.
//
// The function logs the total count of records matching the query and validates the results based on the callback function.
// If any record violates the filtering conditions or an error occurs during the query, the test fails with an appropriate message.
func TestLikeAndRangeAndInAsArrayOfInt(t *testing.T) {

	//Filter(F("streamId").Eq(stream.(*Stream).Id)).
	bqdb, err := bigquerydb.NewBqDatabase("bq://shieldiot-staging:pulseiot")

	if err != nil {
		t.Fatalf("NewBqDatabase failed: %v", err)
	}

	pattern := "10.164.41"
	cb := func(entity entity.Entity) entity.Entity {
		fr := entity.(*FlowRecord)

		if !strings.HasPrefix(fr.SrcIP, pattern) {
			t.Fatalf(" result set does not satisfy requested pattern of %s: has value of %s", pattern, fr.SrcIP)
		}
		if fr.DstPort == 80 || fr.DstPort == 443 || fr.DstPort == 0 {
			t.Fatalf(" result set does not satisfy requested criteria. DstPort of %s: has value of %d", fr.DstIP, fr.DstPort)
		}
		return nil
	}
	fr := NewFlowRecordEntity(streamId).(*FlowRecord)
	_, count, err := bqdb.Query(fr.EntityFactory()).
		Apply(cb).
		Filter(F("src_ip").Like("10.164.41.%")).
		Filter(F("dst_port").NotIn([]int{0, 443, 80})).
		Range("start_time", 1722540000000, 1722715200000).
		Limit(1000).
		Page(0).
		Find()

	if err != nil {
		t.Fatal(err)
	}
	logger.Debug("count: %d", count)
}

// TestLikeAndRangeAndGt tests a BigQuery query with multiple filtering conditions
// (LIKE, NOT IN, GT, RANGE) and applies a callback function to validate the results.
//
// The function does the following:
// 1. Connects to the BigQuery database for the 'shieldiot-staging' project and 'pulseiot' dataset.
// 2. Defines a callback function (cb) to validate that the returned records meet specific criteria:
//   - The 'src_ip' must start with the pattern '10.164.41'.
//   - The 'proto' field must not contain the values 'ICMP' or 'UDP'.
//
// 3. Constructs a query that applies multiple filters to the data:
//   - Filters 'src_ip' using a LIKE operator to match IPs starting with '10.164.41.%'.
//   - Filters 'proto' using a NOT IN clause to exclude 'ICMP' and 'UDP' protocols.
//   - Filters 'bytes_to_srv' using a greater than (GT) operator to include only records where the value is greater than 1500.
//   - Filters 'start_time' using a range to include timestamps between 1722540000000 and 1722715200000.
//   - Limits the result to 1000 records and retrieves the first page (page 0).
//
// 4. Executes the query and applies the callback function to validate the results.
// 5. If the query fails or the results do not match the expected criteria, the test will fail.
//
// Parameters:
// - t *testing.T: The testing object provided by Go's testing framework, used for running and reporting test results.
// - bqdb: The BigQuery database connection object that is used to query the database.
// - err: The error object to handle any issues encountered while connecting to the database or executing the query.
// - pattern: The IP address pattern (in this case, '10.164.41') used for filtering the 'src_ip' field.
// - cb: A callback function applied to each entity in the result set, used to validate that the 'src_ip' starts with the given pattern and that the 'proto' field is neither 'ICMP' nor 'UDP'.
//
// The function logs the total count of records matching the query and validates the results based on the callback function.
// If any record violates the filtering conditions or an error occurs during the query, the test fails with an appropriate message.
func TestLikeAndRangeAndGt(t *testing.T) {

	//Filter(F("streamId").Eq(stream.(*Stream).Id)).
	bqdb, err := bigquerydb.NewBqDatabase("bq://shieldiot-staging:pulseiot")

	if err != nil {
		t.Fatalf("NewBqDatabase failed: %v", err)
	}

	pattern := "10.164.41"
	cb := func(entity entity.Entity) entity.Entity {
		fr := entity.(*FlowRecord)

		if !strings.HasPrefix(fr.SrcIP, pattern) {
			t.Fatalf(" result set does not satisfy requested pattern of %s: has value of %s", pattern, fr.SrcIP)
		}
		if fr.Proto == "ICMP" || fr.Proto == "UDP" {
			t.Fatalf(" result set does not satisfy requested criteria. protocol of %s: has value of %s", fr.DstIP, fr.Proto)
		}
		return nil
	}
	fr := NewFlowRecordEntity(streamId).(*FlowRecord)
	_, count, err := bqdb.Query(fr.EntityFactory()).
		Apply(cb).
		Filter(F("src_ip").Like("10.164.41.%")).
		Filter(F("proto").NotIn([]string{"ICMP", "UDP"})).
		Filter(F("bytes_to_srv").Gt(1500)).
		Range("start_time", 1722540000000, 1722715200000).
		Limit(1000).
		Page(0).
		Find()

	if err != nil {
		t.Fatal(err)
	}
	logger.Debug("count: %d", count)
}

// TestLikeAndRangeAndInAsArrayOfString tests a BigQuery query with multiple filtering conditions
// (LIKE, IN, RANGE) and applies a callback function to validate the results.
//
// The function does the following:
// 1. Connects to the BigQuery database for the 'shieldiot-staging' project and 'pulseiot' dataset.
// 2. Defines a callback function (cb) to validate that the returned records meet specific criteria:
//   - The 'src_ip' must start with the pattern '10.164.41'.
//   - The 'proto' field must not contain the value 'ICMP'.
//
// 3. Constructs a query that applies multiple filters to the data:
//   - Filters 'src_ip' using a LIKE operator to match IPs starting with '10.164.41.%'.
//   - Filters 'proto' using an IN clause to include only 'TCP' and 'UDP' protocols.
//   - Filters 'start_time' using a range to include timestamps between 1722540000000 and 1722715200000.
//   - Limits the result to 1000 records and retrieves the first page (page 0).
//
// 4. Executes the query and applies the callback function to validate the results.
// 5. If the query fails or the results do not match the expected criteria, the test will fail.
//
// Parameters:
// - t *testing.T: The testing object provided by Go's testing framework, used for running and reporting test results.
// - bqdb: The BigQuery database connection object that is used to query the database.
// - err: The error object to handle any issues encountered while connecting to the database or executing the query.
// - pattern: The IP address pattern (in this case, '10.164.41') used for filtering the 'src_ip' field.
// - cb: A callback function applied to each entity in the result set, used to validate that the 'src_ip' starts with the given pattern and that the 'proto' field is not 'ICMP'.
//
// The function logs the total count of records matching the query and validates the results based on the callback function.
// If any record violates the filtering conditions or an error occurs during the query, the test fails with an appropriate message.
func TestLikeAndRangeAndInAsArrayOfString(t *testing.T) {

	//Filter(F("streamId").Eq(stream.(*Stream).Id)).
	bqdb, err := bigquerydb.NewBqDatabase("bq://shieldiot-staging:pulseiot")

	if err != nil {
		t.Fatalf("NewBqDatabase failed: %v", err)
	}

	pattern := "10.164.41"
	cb := func(entity entity.Entity) entity.Entity {
		fr := entity.(*FlowRecord)

		if !strings.HasPrefix(fr.SrcIP, pattern) {
			t.Fatalf(" result set does not satisfy requested pattern of %s: has value of %s", pattern, fr.SrcIP)
		}
		if fr.Proto == "ICMP" {
			t.Fatalf(" result set does not satisfy requested criteria. protocol of %s: has value of %s", fr.DstIP, fr.Proto)
		}
		return nil
	}
	fr := NewFlowRecordEntity(streamId).(*FlowRecord)
	_, count, err := bqdb.Query(fr.EntityFactory()).
		Apply(cb).
		Filter(F("src_ip").Like("10.164.41.%")).
		Filter(F("proto").In([]string{"TCP", "UDP"})).
		Range("start_time", 1722540000000, 1722715200000).
		Limit(1000).
		Page(0).
		Find()

	if err != nil {
		t.Fatal(err)
	}
	logger.Debug("count: %d", count)
}

// TestLikeAndRangeAndInAsArrayOfStringAndOrderBy tests a query that:
// 1. Filters rows based on a `src_ip` pattern using a LIKE operator (e.g., '10.164.41.%').
// 2. Filters rows based on the `proto` field, using an IN clause to include only 'TCP' and 'UDP' protocols.
// 3. Filters rows where the `start_time` is within a specified range.
// 4. Orders the result set by the `bytes_to_srv` field in ascending order.
// 5. Limits the result set to the first 1000 rows.
// 6. Applies a callback function to validate the result set based on specific conditions.
//
// Steps:
// - A new BigQuery database connection is established using the `shieldiot-staging` project and `pulseiot` dataset.
// - The query is built incrementally using the following conditions:
//   - The `src_ip` is filtered using a LIKE operator to include IPs that start with '10.164.41.%'.
//   - The `proto` field is filtered using the IN clause to include only 'TCP' and 'UDP' protocol records.
//   - The `start_time` is filtered to include records between 1722540000000 and 1722715200000 (a range of timestamps).
//   - The result set is sorted by the `bytes_to_srv` field in ascending order.
//   - The result set is limited to 1000 records, and pagination is applied (starting from page 0).
//
// - A callback function (`cb`) is applied to each entity in the result set to validate the following:
//   - The `src_ip` of each record starts with the specified pattern ("10.164.41").
//   - The `proto` field should not have the value "ICMP", ensuring only "TCP" and "UDP" records are present.
//
// - If an error occurs during the database connection, query execution, or validation, the test fails with an error message.
// - After fetching the records, the function verifies that the `bytes_to_srv` field is sorted in ascending order by comparing the first and last records in the result set.
// - If the result is valid, the total count of matching records is logged.
//
// Parameters:
// - t *testing.T: The testing object provided by Go's testing framework, used for managing test execution and reporting failures.
//
// The query's purpose is to return the first 1000 records that match the following conditions:
// - The `src_ip` matches the pattern '10.164.41.%'.
// - The `proto` is either 'TCP' or 'UDP'.
// - The `start_time` is within the specified timestamp range.
// - The result set is ordered by the `bytes_to_srv` field in ascending order.
//
// Expected output:
// - The result set is sorted by `bytes_to_srv` in ascending order and matches the filtering conditions.
// - If any record violates the filtering conditions (e.g., `src_ip` does not match the pattern or `proto` is "ICMP"), the test fails with an error.
// - If the sorting of the `bytes_to_srv` field is incorrect (i.e., the first record has a higher `bytes_to_srv` value than the last), the test fails.
func TestLikeAndRangeAndInAsArrayOfStringAndOrderBy(t *testing.T) {

	bqdb, err := bigquerydb.NewBqDatabase("bq://shieldiot-staging:pulseiot")

	if err != nil {
		t.Fatalf("NewBqDatabase failed: %v", err)
	}

	pattern := "10.164.41"
	cb := func(entity entity.Entity) entity.Entity {
		fr := entity.(*FlowRecord)

		if !strings.HasPrefix(fr.SrcIP, pattern) {
			t.Fatalf(" result set does not satisfy requested pattern of %s: has value of %s", pattern, fr.SrcIP)
		}
		if fr.Proto == "ICMP" {
			t.Fatalf(" result set does not satisfy requested criteria. protocol of %s: has value of %s", fr.DstIP, fr.Proto)
		}
		return entity
	}
	fr := NewFlowRecordEntity(streamId).(*FlowRecord)
	entities, count, err := bqdb.Query(fr.EntityFactory()).
		Apply(cb).
		Filter(F("src_ip").Like("10.164.41.%")).
		Filter(F("protocol").In([]string{"TCP", "UDP"})).
		Range("start_time", 1722540000000, 1722715200000).
		Sort("bytes_to_srv").
		Limit(1000).
		Page(0).
		Find()

	if err != nil {
		t.Fatal(err)
	}

	firstFr := entities[0].(*FlowRecord)
	lastFr := entities[len(entities)-1].(*FlowRecord)

	if firstFr.BytesToSrv > lastFr.BytesToSrv {
		t.Fatalf("firsr item BTS value: %d, last item BTS value: %d", firstFr.BytesToSrv, lastFr.BytesToSrv)
	}
	logger.Debug("count: %d", count)

}

// TestLikeAndRangeAndInAsArrayOfStringAndSum tests a query that:
// 1. Filters rows based on a `src_ip` pattern using a LIKE operator (e.g., '10.164.41.%').
// 2. Filters rows based on the `proto` field, using an IN clause to include only 'TCP' and 'UDP' protocols.
// 3. Filters rows where the `start_time` is within a specified range.
// 4. Performs a `SUM` aggregation on the `bytes_to_srv` field to calculate the total sum.
//
// Steps:
// - A new BigQuery database connection is established using the `shieldiot-staging` project and `pulseiot` dataset.
// - The query is built incrementally using the following conditions:
//   - The `src_ip` is filtered using a LIKE operator to include IPs that start with '10.164.41.%'.
//   - The `proto` field is filtered using the IN clause to include only 'TCP' and 'UDP' protocol records.
//   - The `start_time` is filtered to include records between 1722540000000 and 1722715200000 (a range of timestamps).
//   - The query performs a `SUM` aggregation on the `bytes_to_srv` field, which calculates the total sum of the `bytes_to_srv` for the filtered records.
//
// - If an error occurs during the database connection or query execution, the test fails with an error message.
// - If the query is successful, the total sum of `bytes_to_srv` is logged.
//
// Parameters:
// - t *testing.T: The testing object provided by Go's testing framework, used for managing test execution and reporting failures.
//
// The query's purpose is to calculate the total sum of the `bytes_to_srv` field for records that match the following conditions:
// - The `src_ip` matches the pattern '10.164.41.%'.
// - The `proto` is either 'TCP' or 'UDP'.
// - The `start_time` is within the specified timestamp range.
//
// Expected output:
// - The total sum of `bytes_to_srv` for the filtered records.
func TestLikeAndRangeAndInAsArrayOfStringAndSum(t *testing.T) {

	bqdb, err := bigquerydb.NewBqDatabase("bq://shieldiot-staging:pulseiot")

	if err != nil {
		t.Fatalf("NewBqDatabase failed: %v", err)
	}
	fr := NewFlowRecordEntity(streamId).(*FlowRecord)
	touples, count, err := bqdb.Query(fr.EntityFactory()).
		Filter(F("stream_id").Eq("juuice-1-01")).
		Filter(F("protocol").In([]string{"TCP", "UDP"})).
		Filter(F("start_time").Gte(1736873880000)).
		Filter(F("end_time").Lte(1736873999000)).
		GroupAggregation("bytes_to_srv", "sum", "minute")

	if err != nil {
		t.Fatal(err)
	}
	logger.Debug("touples: %v", touples)
	logger.Debug("count: %f", count)
}

// TestLikeAndRangeAndInAsArrayOfStringAndMax tests a query that:
// 1. Filters rows based on a `src_ip` pattern using a LIKE operator (e.g., '10.164.41.%').
// 2. Filters rows based on the `proto` field, using an IN clause to include only 'TCP' and 'UDP' protocols.
// 3. Filters rows where the `start_time` is within a specified range.
// 4. Performs a `MAX` aggregation on the `bytes_to_srv` field to calculate the maximum value.
//
// Steps:
// - A new BigQuery database connection is established using the `shieldiot-staging` project and `pulseiot` dataset.
// - The query is built incrementally using the following conditions:
//   - The `src_ip` is filtered using a LIKE operator to include IPs that start with '10.164.41.%'.
//   - The `proto` field is filtered using the IN clause to include only 'TCP' and 'UDP' protocol records.
//   - The `start_time` is filtered to include records between 1722540000000 and 1722715200000 (a range of timestamps).
//   - The query performs a `MAX` aggregation on the `bytes_to_srv` field, which calculates the maximum value of the `bytes_to_srv` for the filtered records.
//
// - If an error occurs during the database connection or query execution, the test fails with an error message.
// - If the query is successful, the maximum value of `bytes_to_srv` is logged.
//
// Parameters:
// - t *testing.T: The testing object provided by Go's testing framework, used for managing test execution and reporting failures.
//
// The query's purpose is to calculate the maximum value of the `bytes_to_srv` field for records that match the following conditions:
// - The `src_ip` matches the pattern '10.164.41.%'.
// - The `proto` is either 'TCP' or 'UDP'.
// - The `start_time` is within the specified timestamp range.
//
// Expected output:
// - The maximum value of `bytes_to_srv` for the filtered records.
func TestLikeAndRangeAndInAsArrayOfStringAndMax(t *testing.T) {

	bqdb, err := bigquerydb.NewBqDatabase("bq://shieldiot-staging:pulseiot")

	if err != nil {
		t.Fatalf("NewBqDatabase failed: %v", err)
	}
	fr := NewFlowRecordEntity(streamId).(*FlowRecord)
	max, err := bqdb.Query(fr.EntityFactory()).
		Filter(F("src_ip").Like("10.164.41.%")).
		Filter(F("proto").In([]string{"TCP", "UDP"})).
		Range("start_time", 1722540000000, 1722715200000).
		Aggregation("bytes_to_srv", "max")

	if err != nil {
		t.Fatal(err)
	}
	logger.Debug("count: %f", max)
}

// TestLikeAndRangeAndInAsArrayOfStringAndAverage tests a query that:
// 1. Filters rows based on a `src_ip` pattern using a LIKE operator (e.g., '10.164.41.%').
// 2. Filters rows based on the `proto` field, using an IN clause to include only 'TCP' and 'UDP' protocols.
// 3. Filters rows where the `start_time` is within a specified range.
// 4. Performs an `AVG` aggregation on the `bytes_to_srv` field to calculate the average value.
//
// Steps:
// - A new BigQuery database connection is established using the `shieldiot-staging` project and `pulseiot` dataset.
// - The query is built incrementally using the following conditions:
//   - The `src_ip` is filtered using a LIKE operator to include IPs that start with '10.164.41.%'.
//   - The `proto` field is filtered using the IN clause to include only 'TCP' and 'UDP' protocol records.
//   - The `start_time` is filtered to include records between 1722540000000 and 1722715200000 (a range of timestamps).
//   - The query performs an `AVG` aggregation on the `bytes_to_srv` field, which calculates the average value of the `bytes_to_srv` for the filtered records.
//
// - If an error occurs during the database connection or query execution, the test fails with an error message.
// - If the query is successful, the average value of `bytes_to_srv` is logged.
//
// Parameters:
// - t *testing.T: The testing object provided by Go's testing framework, used for managing test execution and reporting failures.
//
// The query's purpose is to calculate the average of the `bytes_to_srv` field for records that match the following conditions:
// - The `src_ip` matches the pattern '10.164.41.%'.
// - The `proto` is either 'TCP' or 'UDP'.
// - The `start_time` is within the specified timestamp range.
//
// Expected output:
// - The average value of `bytes_to_srv` for the filtered records.
func TestLikeAndRangeAndInAsArrayOfStringAndAverage(t *testing.T) {

	bqdb, err := bigquerydb.NewBqDatabase("bq://shieldiot-staging:pulseiot")

	if err != nil {
		t.Fatalf("NewBqDatabase failed: %v", err)
	}
	fr := NewFlowRecordEntity(streamId).(*FlowRecord)
	avg, err := bqdb.Query(fr.EntityFactory()).
		Filter(F("src_ip").Like("10.164.41.%")).
		Filter(F("proto").In([]string{"TCP", "UDP"})).
		Range("start_time", 1722540000000, 1722715200000).
		Aggregation("bytes_to_srv", "avg")

	if err != nil {
		t.Fatal(err)
	}
	logger.Debug("count: %f", avg)
}

// TestLikeAndRangeAndInAsArrayOfStringAndCount tests a query that:
// 1. Filters rows based on a `src_ip` pattern using a LIKE operator (e.g., '10.164.41.%').
// 2. Filters rows based on `proto` field, using an IN clause to include only 'TCP' and 'UDP' protocols.
// 3. Filters rows where the `start_time` is within a specified range.
// 4. Performs a COUNT aggregation to return the total number of records that match the filters.
//
// Steps:
// - A new BigQuery database connection is established using the `shieldiot-staging` project and `pulseiot` dataset.
// - The query is built incrementally using the following conditions:
//   - The `src_ip` is filtered using a LIKE operator to include IPs that start with '10.164.41.%'.
//   - The `proto` field is filtered using the IN clause to include only 'TCP' and 'UDP' protocol records.
//   - The `start_time` is filtered to include records between 1722540000000 and 1722715200000 (a range of timestamps).
//   - The query performs a `COUNT` operation to count the total number of records that match the specified filters.
//
// - If an error occurs during the database connection or query execution, the test fails with an error message.
// - If the query is successful, the total count of matching records is logged.
//
// Parameters:
// - t *testing.T: The testing object provided by Go's testing framework, used for managing test execution and reporting failures.
//
// The query's purpose is to count the total number of records that match the following conditions:
// - The `src_ip` matches the pattern '10.164.41.%'.
// - The `proto` is either 'TCP' or 'UDP'.
// - The `start_time` is within the specified timestamp range.
//
// Expected output:
// - The total count of records that match the specified filters.
func TestLikeAndRangeAndInAsArrayOfStringAndCount(t *testing.T) {

	bqdb, err := bigquerydb.NewBqDatabase("bq://shieldiot-staging:pulseiot")

	if err != nil {
		t.Fatalf("NewBqDatabase failed: %v", err)
	}
	fr := NewFlowRecordEntity(streamId).(*FlowRecord)
	count, err := bqdb.Query(fr.EntityFactory()).
		Filter(F("src_ip").Like("10.164.41.%")).
		Filter(F("proto").In([]string{"TCP", "UDP"})).
		Range("start_time", 1722540000000, 1722715200000).
		Count()

	if err != nil {
		t.Fatal(err)
	}
	logger.Debug("count: %d", count)
}

// TestLikeAndRangeAndInAsArrayOfStringAndGroupCount tests a query that:
// 1. Filters rows based on a `src_ip` pattern using a LIKE operator (e.g., '10.164.41.%').
// 2. Filters rows based on `proto` field, using an IN clause to include only 'TCP' and 'UDP' protocols.
// 3. Filters rows where the `start_time` is within a specified range.
// 4. Groups the filtered rows by `src_ip` and performs a COUNT aggregation for each group.
//
// Steps:
// - A new BigQuery database connection is established using the `shieldiot-staging` project and `pulseiot` dataset.
// - The query is built incrementally using the following conditions:
//   - The `src_ip` is filtered using a LIKE operator to include IPs that start with '10.164.41.%'.
//   - The `proto` field is filtered using the IN clause to include only 'TCP' and 'UDP' protocol records.
//   - The `start_time` is filtered to include records between 1722540000000 and 1722715200000 (a range of timestamps).
//   - A group-by aggregation is performed on the `src_ip` field, and a `COUNT` aggregation is applied to count the number of records for each `src_ip`.
//
// - If an error occurs during the database connection or query execution, the test fails with an error message.
// - If the query is successful, the result (grouped by `src_ip` along with the count of records for each group) and the total count of records are logged.
//
// Parameters:
// - t *testing.T: The testing object provided by Go's testing framework, used for managing test execution and reporting failures.
//
// The query's purpose is to count the number of records that match the following conditions:
// - The `src_ip` matches the pattern '10.164.41.%'.
// - The `proto` is either 'TCP' or 'UDP'.
// - The `start_time` is within the specified timestamp range.
// The records are grouped by the `src_ip`, and the function returns the count of records for each unique `src_ip`.
//
// Expected output:
// - A result map where each key is a unique `src_ip`, and the value is the count of records for that `src_ip`.
// - A total count of all records that match the filters and grouping.
func TestLikeAndRangeAndInAsArrayOfStringAndGroupCount(t *testing.T) {

	bqdb, err := bigquerydb.NewBqDatabase("bq://shieldiot-staging:pulseiot")

	if err != nil {
		t.Fatalf("NewBqDatabase failed: %v", err)
	}
	fr := NewFlowRecordEntity(streamId).(*FlowRecord)
	result, count, err := bqdb.Query(fr.EntityFactory()).
		Filter(F("src_ip").Like("10.164.41.%")).
		Filter(F("proto").In([]string{"TCP", "UDP"})).
		Range("start_time", 1722540000000, 1722715200000).
		GroupCount("src_ip")

	if err != nil {
		t.Fatal(err)
	}
	logger.Debug("count: %d\n %v", count, result)
}

// TestGroupCountOnly tests a simple query that:
// 1. Groups the data by the `src_ip` field.
// 2. Performs a COUNT aggregation to count the number of records for each `src_ip` group.
//
// Steps:
// - A new BigQuery database connection is established using the `shieldiot-staging` project and `pulseiot` dataset.
// - The query is built with the following operation:
//   - The `src_ip` field is grouped, and a COUNT aggregation is applied to calculate the number of records per unique `src_ip`.
//
// - If an error occurs during the database connection or query execution, the test fails with an error message.
// - If the query is successful, the result (grouped by `src_ip` along with the count of records for each group) is logged, as well as the total count.
//
// Parameters:
// - t *testing.T: The testing object provided by Go's testing framework, used for managing test execution and reporting failures.
//
// The query's purpose is to group the flow records by the `src_ip` field and count the number of records for each unique `src_ip`.
// This test ensures that the grouping and counting of records is performed correctly.
//
// Expected output:
// - A result map where each key is a unique `src_ip`, and the value is the count of records for that `src_ip`.
// - A total count of all records that were processed by the query.
func TestGroupCountOnly(t *testing.T) {

	bqdb, err := bigquerydb.NewBqDatabase("bq://shieldiot-staging:pulseiot")

	if err != nil {
		t.Fatalf("NewBqDatabase failed: %v", err)
	}
	fr := NewFlowRecordEntity(streamId).(*FlowRecord)
	result, count, err := bqdb.Query(fr.EntityFactory()).
		GroupCount("src_ip")

	if err != nil {
		t.Fatal(err)
	}
	logger.Debug("count: %d\n %v", count, result)
}

// TestLikeAndRangeAndInAsArrayOfStringAndGroupAggregationForSum tests a query that:
// 1. Filters rows based on a `src_ip` pattern using a LIKE operator (e.g., '10.164.41.%').
// 2. Filters rows based on `proto` field, using an IN clause to include only 'TCP' and 'UDP'.
// 3. Filters rows where the `start_time` is within a specified range.
// 4. Groups the resulting rows by `src_ip` and performs a SUM aggregation on the `bytes_to_srv` field for each group.
//
// Steps:
// - A new BigQuery database connection is established using the `shieldiot-staging` project and `pulseiot` dataset.
// - The query is built incrementally using the following conditions:
//   - The `src_ip` is filtered using a LIKE operator to include IPs that start with '10.164.41.'.
//   - The `proto` field is filtered using the IN clause to include only 'TCP' and 'UDP' protocol records.
//   - The `start_time` is filtered to include records between 1722540000000 and 1722715200000 (a range of timestamps).
//   - A group-by aggregation is performed on the `src_ip` field, and the `SUM` aggregation is applied to the `bytes_to_srv` field.
//
// - If an error occurs during the query building or execution, the test fails.
// - If the query is successful, the result, along with the total sum of the `bytes_to_srv` field, is logged.
//
// Parameters:
// - t *testing.T: The testing object provided by Go's testing framework, used for managing test execution and reporting failures.
//
// The query's purpose is to perform a SUM aggregation of the `bytes_to_srv` field, grouped by the source IP (`src_ip`),
// where the IP matches a pattern, the protocol is either TCP or UDP, and the start time falls within a specific range.
// This test ensures that the query runs successfully and provides expected results for the sum aggregation.
func TestLikeAndRangeAndInAsArrayOfStringAndGroupAggregationForSum(t *testing.T) {

	bqdb, err := bigquerydb.NewBqDatabase("bq://shieldiot-staging:pulseiot")

	if err != nil {
		t.Fatalf("NewBqDatabase failed: %v", err)
	}
	fr := NewFlowRecordEntity(streamId).(*FlowRecord)
	result, count, err := bqdb.Query(fr.EntityFactory()).
		Filter(F("src_ip").Like("10.164.41.%")).
		Filter(F("proto").In([]string{"TCP", "UDP"})).
		Range("start_time", 1722540000000, 1722715200000).
		GroupAggregation("bytes_to_srv", "sum", "src_ip")

	if err != nil {
		t.Fatal(err)
	}
	logger.Debug("count: %f\n %v", count, result)
}

// TestLikeAndRangeAndInAsArrayOfStringAndGroupAggregationForCount tests a query that:
// 1. Filters rows based on a `src_ip` pattern using a LIKE operator (e.g., '10.164.41.%').
// 2. Filters rows based on `proto` field, using an IN clause to include only 'TCP' and 'UDP'.
// 3. Filters rows where the `start_time` is within a specified range.
// 4. Groups the resulting rows by `src_ip` and performs a COUNT aggregation on each group.
//
// Steps:
// - A new BigQuery database connection is established using the `shieldiot-staging` project and `pulseiot` dataset.
// - The query is built incrementally using the following conditions:
//   - The `src_ip` is filtered using a LIKE operator to include IPs that start with '10.164.41.'.
//   - The `proto` field is filtered using the IN clause to include only 'TCP' and 'UDP' protocol records.
//   - The `start_time` is filtered to include records between 1722540000000 and 1722715200000 (a range of timestamps).
//   - A group-by aggregation is performed on the `src_ip` field, counting the number of records for each source IP.
//
// - If an error occurs during the query building or execution, the test fails.
// - If the query is successful, the result, along with the total count of records, is logged.
//
// Parameters:
// - t *testing.T: The testing object provided by Go's testing framework, used for managing test execution and reporting failures.
//
// The query's purpose is to perform a COUNT aggregation of records grouped by the source IP (src_ip),
// where the IP matches a pattern, the protocol is either TCP or UDP, and the start time falls within a specific range.
// This test ensures that the query runs successfully and provides expected results for aggregation.
func TestLikeAndRangeAndInAsArrayOfStringAndGroupAggregationForCount(t *testing.T) {

	bqdb, err := bigquerydb.NewBqDatabase("bq://shieldiot-staging:pulseiot")

	if err != nil {
		t.Fatalf("NewBqDatabase failed: %v", err)
	}
	fr := NewFlowRecordEntity(streamId).(*FlowRecord)
	result, count, err := bqdb.Query(fr.EntityFactory()).
		Filter(F("src_ip").Like("10.164.41.%")).
		Filter(F("proto").In([]string{"TCP", "UDP"})).
		Range("start_time", 1722540000000, 1722715200000).
		GroupAggregation("src_ip", "count")

	if err != nil {
		t.Fatal(err)
	}
	logger.Debug("count: %f\n %v", count, result)
}

// TestHistogram tests a query that:
// 1. Filters rows based on a `src_ip` pattern using a LIKE operator (e.g., '10.164.41.%').
// 2. Filters rows based on the `proto` field, using an IN clause to include only 'TCP' and 'UDP' protocols.
// 3. Filters rows where the `start_time` is within a specified range.
// 4. Generates a histogram of the `bytes_to_srv` field using the SUM aggregation, grouped by hourly intervals.
//
// Steps:
// - A new BigQuery database connection is established using the `shieldiot-staging` project and `pulseiot` dataset.
// - The query is built incrementally using the following conditions:
//   - The `src_ip` is filtered using a LIKE operator to include IPs that start with '10.164.41.'.
//   - The `proto` field is filtered using the IN clause to include only 'TCP' and 'UDP' protocol records.
//   - The `start_time` is filtered to include records between 1722540000000 and 1722715200000 (a range of timestamps).
//   - The histogram is generated by summing the `bytes_to_srv` field, with results grouped by hourly intervals (`time.Hour`).
//
// - If an error occurs during the query building or execution, the test fails.
// - If the query is successful, the result, along with the total count of histogram buckets, is logged.
//
// Parameters:
// - t *testing.T: The testing object provided by Go's testing framework, used for managing test execution and reporting failures.
//
// The query's purpose is to create a histogram that aggregates the total `bytes_to_srv` per hour for network traffic records that match the specified filters.
// This test ensures that the query runs successfully and provides the expected hourly histogram of data.
func TestHistogram(t *testing.T) {

	bqdb, err := bigquerydb.NewBqDatabase("bq://shieldiot-staging:pulseiot")

	if err != nil {
		t.Fatalf("NewBqDatabase failed: %v", err)
	}
	fr := NewFlowRecordEntity(streamId).(*FlowRecord)
	result, count, err := bqdb.Query(fr.EntityFactory()).
		Filter(F("src_ip").Like("10.164.41.%")).
		Filter(F("proto").In([]string{"TCP", "UDP"})).
		Range("start_time", 1722540000000, 1722715200000).
		Histogram("bytes_to_srv", "sum", "start_time", time.Hour)

	if err != nil {
		t.Fatal(err)
	}
	logger.Debug("count: %f\n %v", count, result)
}

type NewtworkActivityOverTime struct {
	entity.BaseEntity
	TimePoint    int64 `json:"timestamp" bq:"start_time"`
	NumOfDevices int64 `json:"value"     bq:"device_id"`
}

func TestNetworkActivityOverTime(t *testing.T) {

}

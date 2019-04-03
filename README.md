go-bulk-influx
===
Bulk query & write to InfluxDB

Examples
---
### Query last (my_field, timestamp) pairs of given tags
``go
import (
    bulk        "github.com/cjongseok/go-bulk-influx"
	influxdb    "github.com/influxdata/influxdb/client/v2"
)

// DB info
dbName := "my_db"
dbPrecision := bulk.Second
dbClient, _ := influxdb.NewHTTPClient(influxdb.HTTPConfig{
    Addr: "http://localhost:8086",
    Username: "my_user",
    Password: "my_password",
})

// Tag & Field types of my_measurement
var myTagTypes = map[string]types.BasicKind{
    "my_tag": types.String,
}
var myFieldTypes = map[string]types.BasicKind{
    "my_field": types.Float64,
}

// Add statements
bq := utils.NewBulkQuery(ctx, dbClient, dbName, dbPrecision)
var res []chan []*utils.Series
for _, t := range myTags {
	ch := make(chan []*utils.Series)
	res = append(res, ch)
	cmd := fmt.Sprintf("select last(my_field) as last_my_field from my_measurement where my_tag=%s order by desc limit 1", t)
	stmt := utils.Statement{Cmd: cmd, Res: ch, TagTypes: tagTypes, FieldTypes: fieldTypes}
	bq.Add(stmt)
}

// Execute statements
err = bq.Execute(false)     // disable Google Cloud Tracing
if err != nil {
	return
}

// Transform results
type LastValue struct {
    Value       float64
    Timestamp   int32
}
lasts = make([]LastValue, len(myTags))
for i, resCh := range res {
	s := <-resCh
	if len(s) != 1 || len(s[0].Fields) != 1 {
    	lasts[i] = LastValue{}
		continue
	}
	lasts[i] = LastValue{
    	Value: s[0].Fields[0]["latest_my_field"].(float64),
    	Timestamp: s[0].Fields[0][bulk.InfluxFieldTime].(int32),
    }
}
``

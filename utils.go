package bulk

import (
	"time"
	"fmt"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/client/v2"
	"golang.org/x/net/context"
	"cloud.google.com/go/trace"
	"sync"
	"strings"
	"go/types"
	"encoding/json"
	"hash/fnv"
	"strconv"
	"sort"
	"github.com/sirupsen/logrus"
	"math"
)

// influx time dims
type TimeDimension string
const (
	Nanosecond          TimeDimension = "ns"
	Microsecond         TimeDimension = "u"
	Millisecond         TimeDimension = "ms"
	Second              TimeDimension = "s"
	Minute TimeDimension = "m"
	Hour   TimeDimension = "h"
	Day    TimeDimension = "d"
	Week   TimeDimension = "w"
)

const (
	DayInNanos  = 24 * time.Hour
	WeekInNanos = 7 * DayInNanos
)

// InfluxFieldTime is timestamp of Influx points
const InfluxFieldTime = "time"

// MaxMultiStatements is the maximum number of statements in multi-statements query
const MaxMultiStatements = 1

// MaxPoints is the maximum number of InfluxDB points to write
const MaxPoints = 5000

// logrusFieldFunc is a field to add to structured logs w/ logrus
const logrusFieldFunc = "func"

// influx time dims in nanos in descending order
var dims = []time.Duration {
	WeekInNanos, DayInNanos, time.Hour, time.Minute, time.Second, time.Millisecond, time.Microsecond, time.Nanosecond,
}

var dimSymbolMap = map[time.Duration]TimeDimension{
	time.Nanosecond:  Nanosecond,
	time.Microsecond: Microsecond,
	time.Millisecond: Millisecond,
	time.Second:      Second,
	time.Minute:      Minute,
	time.Hour:        Hour,
	DayInNanos:       Day,
	WeekInNanos:      Week,
}

// InfluxTime is an amount of time elapsed from the base timeline in Influx time dimension
type InfluxTime string

func ToInfluxTime(precision TimeDimension, t time.Time) InfluxTime {
	return NanosToInfluxTime(precision, t.UnixNano())
}

func SecondsToInfluxTime(precision TimeDimension, seconds int32) InfluxTime {
	return NanosToInfluxTime(precision, int64(seconds) * 1e9)
}

func NanosToInfluxTime(precision TimeDimension, nanos int64) InfluxTime {
	var component int64
	switch precision {
	case Nanosecond:
		component = nanos
	case Microsecond:
		component = nanos / 1e3
	case Millisecond:
		component = nanos / 1e6
	case Second:
		component = nanos / 1e9
	case Minute:
		component = nanos / 1e9 / 60
	case Hour:
		component = nanos / 1e9 / 3600
	case Day:
		component = nanos / 1e9 / 3600 / 24
	case Week:
		component = nanos / 1e9 / 3600 / 24 / 7
	default:
		return ""
	}
	return InfluxTime(fmt.Sprintf("%d%s", component, precision))
}

// DurationVector is an amount of time in Influx time dimension
type DurationVector string

// DecomposeIntoInfluxDuration casts a time.Duration into InfluxDuration. It accepts positive `duration` only
func DecomposeDuration(scalar time.Duration) DurationVector {
	if scalar == 0 {
		return "0"
	}

	vector := ""
	remain := scalar
	for _, unit := range dims {
		if remain == 0 {
			break
		}
		component := remain / unit
		if component != 0 {
			remain = remain - component*unit
			vector = fmt.Sprintf("%s%d%s", vector, component, dimSymbolMap[unit])
		}
	}

	return DurationVector(vector)
}

type BulkWrite struct {
	ctx context.Context
	c client.Client
	database string
	precision TimeDimension
	pts []*client.Point
}

func NewBulkWrite(ctx context.Context, c client.Client, database string, precision TimeDimension) *BulkWrite {
	return &BulkWrite{ctx, c, database, precision, nil}
}

func (bw *BulkWrite) AddPoint(point *client.Point) {
	bw.pts = append(bw.pts, point)
}

func (bw *BulkWrite) Write(subSpans bool) error {
	var bps []client.BatchPoints
	if len(bw.pts) == 0 {
		return nil
	}
	for i := 0; i < len(bw.pts) / MaxPoints + 1; i++ {
		bp, err := client.NewBatchPoints(client.BatchPointsConfig{
			Database:  bw.database,
			Precision: string(bw.precision),
		})
		if err != nil {
			return err
		}
		from := i * MaxPoints
		to := int(math.Min(float64((i+1)*MaxPoints), float64(len(bw.pts))))
		bp.AddPoints(bw.pts[from:to])
		bps = append(bps, bp)
	}
	for _, bp := range bps {
		var cspan *trace.Span
		if subSpans {
			cspan = trace.FromContext(bw.ctx).NewChild("influxdb.Write")
		}
		err := bw.c.Write(bp)
		if cspan != nil {
			cspan.Finish()
		}
		// TODO: manage transaction. It should roll-back the whole transaction on a write failure
		if err != nil {
			return err
		}
	}
	return nil
}

type BulkQuery struct {
	stmts []Statement
	wg *sync.WaitGroup
	ctx context.Context
	c client.Client
	database string
	precision TimeDimension
}
type Statement struct {
	Cmd string
	Res chan []*Series
	TagTypes map[string]types.BasicKind
	FieldTypes map[string]types.BasicKind
}
//type RawSeries *models.Row
type rawMultiSeries []models.Row
//func (m rawMultiSeries) ToSingleSeries() (RawSeries, error) {
//	switch len(m) {
//	case 0:
//		return nil, nil
//	case 1:
//		return &m[0], nil
//	default:
//		return nil, fmt.Errorf("many series; %d", len(m))
//	}
//}

type Series struct {
	Hash uint64
	Tags map[string]interface{}
	Fields []map[string]interface{}
}

func seriesHash(tagMap map[string]interface{}) uint64 {
	var s string
	var sorted []string
	for tag, _ := range tagMap {
		sorted = append(sorted, tag)
	}
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i] < sorted[j]
	})

	for _, tag := range sorted {
		s = fmt.Sprintf("%s&%v:%v", s, tag, tagMap[tag])
	}
	h := fnv.New64a()
	h.Write([]byte(s))
	return h.Sum64()
}

// mergeMaps aggregate maps. It can overwrite a key-value pair, if it exists in other maps.
func mergeMaps(maps ...map[string]interface{}) (merged map[string]interface{}) {
	merged = make(map[string]interface{})
	for _, m := range maps {
		for k, v := range m {
			merged[k] = v
		}
	}
	return
}

func (s *Series) DeepCopy() (copy *Series) {
	copy = &Series{
		Hash: s.Hash,
		Tags: make(map[string]interface{}),
	}
	for tag, val := range s.Tags {
		copy.Tags[tag] = val
	}
	for _, fields := range s.Fields {
		fieldsCopy := make(map[string]interface{})
		for field, val := range fields {
			fieldsCopy[field] = val
		}
		copy.Fields = append(copy.Fields, fieldsCopy)
	}
	return
}

func Compact(multi []*Series) (compacts []*Series) {
	seriesMap := make(map[uint64]*Series)
	for _, s := range multi {
		if compact, ok := seriesMap[s.Hash]; ok {
			compact.Fields = append(compact.Fields, s.Fields...)
		} else {
			copy := s.DeepCopy()
			seriesMap[copy.Hash] = copy
			compacts = append(compacts, copy)
			continue
		}
	}
	return
}

func (stmt Statement) unmarshal(raw rawMultiSeries) (multiSeries []*Series, err error) {
	seriesMap := make(map[uint64]*Series)
	for _, row := range raw {
		staticTags := make(map[string]interface{})
		index := make(map[string]int)
		for i, col := range row.Columns {
			index[col] = i
		}

		// get tags. tags does not duplicate with each other. It is maybe guaranteed by InfluxDB
		for tag, val := range row.Tags {
			t, ok := stmt.TagTypes[tag]
			if !ok {
				err = fmt.Errorf("unknown tag %v", tag)
				return
			}
			staticTags[tag] = jsonToPrimitive(val, t)
		}

		// tags in Columns does not duplicate with static tags. It is maybe guaranteed by InfluxDB
		tagsInValues := make(map[string]types.BasicKind)
		for tag, t := range stmt.TagTypes {
			if _, ok := index[tag]; ok {
				tagsInValues[tag] = t
			}
		}

		// parse values
		for _, set := range row.Values {
			// get tags
			dynamicTags := make(map[string]interface{})
			for tag, t := range tagsInValues {
				if i, ok := index[tag]; ok && set[i] != nil {
					dynamicTags[tag] = jsonToPrimitive(set[i], t)
				}
			}

			// get fields
			fields := make(map[string]interface{})
			for field, t := range stmt.FieldTypes {
				if i, ok := index[field]; ok && set[i] != nil {
					fields[field] = jsonToPrimitive(set[i], t)
				}
			}

			tags := mergeMaps(staticTags, dynamicTags)
			hash := seriesHash(tags)
			series, ok := seriesMap[hash]
			if !ok {
				series = &Series{Hash: hash, Tags: tags}
				multiSeries = append(multiSeries, series)
				seriesMap[hash] = series
			}
			series.Fields = append(series.Fields, fields)
		}
	}
	return
}

func jsonToPrimitive(j interface{}, t types.BasicKind) (p interface{}) {
	if j == nil {
		return nil
	}
	switch t {
	//case types.Bool:
	case types.Int:
		{
			i, err := j.(json.Number).Int64()
			if err != nil {
				break
			}
			p = int(i)
		}
	case types.Int8:
		{
			i, err := j.(json.Number).Int64()
			if err != nil {
				break
			}
			p = int8(i)
		}
	case types.Int16:
		{
			i, err := j.(json.Number).Int64()
			if err != nil {
				break
			}
			p = int16(i)
		}
	case types.Int32:
		{
			i, err := j.(json.Number).Int64()
			if err != nil {
				break
			}
			p = int32(i)
		}
	case types.Int64:
		{
			i, err := j.(json.Number).Int64()
			if err != nil {
				break
			}
			p = i
		}
	case types.Uint:
		{
			var str string
			switch j.(type) {
			case json.Number:
				str = string(j.(json.Number))
			case string:
				str = j.(string)
			default:
				return nil
			}
			u, err := strconv.ParseUint(str, 10, 64)
			if err != nil {
				break
			}
			p = u
		}
	case types.Uint8:
		{
			var str string
			switch j.(type) {
			case json.Number:
				str = string(j.(json.Number))
			case string:
				str = j.(string)
			default:
				return nil
			}
			u, err := strconv.ParseUint(str, 10, 64)
			if err != nil {
				break
			}
			p = uint8(u)
		}
	case types.Uint16:
		{
			var str string
			switch j.(type) {
			case json.Number:
				str = string(j.(json.Number))
			case string:
				str = j.(string)
			default:
				return nil
			}
			u, err := strconv.ParseUint(str, 10, 64)
			if err != nil {
				break
			}
			p = uint16(u)
		}
	case types.Uint32:
		{
			var str string
			switch j.(type) {
			case json.Number:
				str = string(j.(json.Number))
			case string:
				str = j.(string)
			default:
				return nil
			}
			u, err := strconv.ParseUint(str, 10, 64)
			if err != nil {
				break
			}
			p = uint32(u)
		}
	case types.Uint64:
		{
			var str string
			switch j.(type) {
			case json.Number:
				str = string(j.(json.Number))
			case string:
				str = j.(string)
			default:
				return nil
			}
			u, err := strconv.ParseUint(str, 10, 64)
			if err != nil {
				break
			}
			p = uint64(u)
		}
		//case types.Uintptr:
	case types.Float32:
		{
			f, err := j.(json.Number).Float64()
			if err != nil {
				break
			}
			p = float32(f)
		}
	case types.Float64:
		{
			f, err := j.(json.Number).Float64()
			if err != nil {
				break
			}
			p = f
		}
		//case types.Complex64:
		//case types.Complex128:
	case types.String:
		p = j.(string)
		//case types.UnsafePointer:
		//case types.UntypedBool:
		//case types.UntypedInt:
		//case types.UntypedRune:
		//case types.UntypedFloat:
		//case types.UntypedComplex:
		//case types.UntypedString:
		//case types.UntypedNil:
	default:
	}
	return
}

func NewBulkQuery(ctx context.Context, c client.Client, database string, precision TimeDimension) *BulkQuery {
	return &BulkQuery{nil, &sync.WaitGroup{}, ctx, c, database, precision}
}
func (bq *BulkQuery) Add(stmt Statement) *BulkQuery {
	//TODO: remove redundant statements. e.g., duplications
	bq.stmts = append(bq.stmts, stmt)
	return bq
}
func (bq *BulkQuery) Execute(subSpans bool) error {
	var cspan, qspan *trace.Span
	if subSpans {
		cspan = trace.FromContext(bq.ctx).NewChild("influx-bq.Run")
	}
	defer func() {
		if cspan != nil {
			cspan.Finish()
		}
	}()

	// check statements
	for _, stmt := range bq.stmts {
		if strings.Contains(stmt.Cmd, ";") {
			return fmt.Errorf("';' is found in the statement(%v)", stmt)
		}
	}

	// remove duplications
	queryToID := make(map[string]int)   //query: queryID
	queryToStmts := make(map[int][]int) //queryID: stmtIDs
	var queryMat [][]string
	queryIndex := 0
	for stmtID, stmt := range bq.stmts {
		queryID, ok := queryToID[stmt.Cmd]
		if !ok {
			// new query
			queryID = queryIndex
			queryToID[stmt.Cmd] = queryID
			i := queryID / MaxMultiStatements
			//j := queryID % MaxMultiStatements
			if len(queryMat) <= i {
				queryMat = append(queryMat, []string{stmt.Cmd})
			} else {
				queryMat[i] = append(queryMat[i], stmt.Cmd)
			}
			queryIndex++
		}
		queryToStmts[queryID] = append(queryToStmts[queryID], stmtID)
	}

	// aggregate queries
	var aggQueries []string
	for _, queries := range queryMat {
		var aggregated string
		for _, q := range queries {
			aggregated = fmt.Sprintf("%s; %s", aggregated, q)
		}
		aggregated = strings.Replace(aggregated, "; ", "", 1)
		aggQueries = append(aggQueries, aggregated)
	}

	// query to DB
	responses := make([]*client.Response, len(aggQueries))
	for i, aggQuery := range aggQueries {
			if cspan != nil {
				qspan = cspan.NewChild("influxdb.Query")
			}
			query := client.NewQuery(aggQuery, bq.database, string(bq.precision))
			logrus.WithField(logrusFieldFunc, "BulkQuery.Execute").Debugf("agg-query[%d]: %s", i, aggQuery)
			res, err := bq.c.Query(query)
			logrus.WithField(logrusFieldFunc, "BulkQuery.Execute").Debugf("res[%d]: %v", i, res)
			if qspan != nil {
				qspan.Finish()
			}
			if err != nil {
				bq.closeChannels()
				return err
			}
			if res != nil && res.Error() != nil {
				bq.closeChannels()
				return res.Error()
			}
			responses[i] = res
	}


	// distribute
	for aggIndex, res := range responses {
		for queryIndex, result := range res.Results {
			queryID := aggIndex *MaxMultiStatements + queryIndex
			if len(queryToStmts[queryID]) == 0 {
				continue
			}
			series, err := bq.stmts[0].unmarshal(result.Series)
			for _, stmtID := range queryToStmts[queryID] {
				bq.wg.Add(1)
				go func(s Statement, in rawMultiSeries) {
					defer func() {
						if r := recover(); r != nil {
							logrus.WithField(logrusFieldFunc, "BulkQuery.Execute").Errorln("distribution failure;", r)
						}
						bq.wg.Done()
					}()
					if err == nil {
						s.Res <- series
					}
				}(bq.stmts[stmtID], result.Series)
			}
		}
	}
	return nil
}

func (bq *BulkQuery) WaitForDistributions() {
	bq.wg.Wait()
}

func (bq *BulkQuery) closeChannels() {
	for _, stmt := range bq.stmts {
		close(stmt.Res)
	}
}

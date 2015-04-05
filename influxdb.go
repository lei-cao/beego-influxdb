package beego_influxdb

import (
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/astaxie/beego"
	"github.com/astaxie/beego/context"
	"github.com/influxdb/influxdb/client"
	"github.com/rcrowley/go-metrics"
)

type BeegoInfluxDb struct {
	Host            string        // localhost:8080
	Username        string        // root
	Password        string        // root
	TablePreifx     string        // app-dev
	SendingInterval time.Duration // Sending to server interval
	SlowThreshold   time.Duration // If longer than this, sending to slow query db as well
	*client.Client
}

var defaultDb = "default"
var slowQueryDb = "slow_query"

var slowQueriesRegister = metrics.NewRegistry()

var dbs = map[string]string{"default": "default", "slow_query": "slow_query"}

func (this *BeegoInfluxDb) InitBeegoInfluxDb() {
	c, err := client.New(this.GetClientConfig(""))
	if err != nil {
		beego.Notice("Init influxdb failed.")
		return
	}
	this.Client = c
	for _, v := range dbs {
		if this.TablePreifx != "" {
			v = this.TablePreifx + "-" + v
		}
		this.Client.CreateDatabase(v)
	}

	beego.InsertFilter("*", beego.BeforeRouter, InitNewRelicTimer)
	beego.InsertFilter("*", beego.FinishRouter, this.ReportMetricsToNewrelic)

	this.SendToInfluxDb()
}

func (this *BeegoInfluxDb) GetClientConfig(db string) *client.ClientConfig {
	c := &client.ClientConfig{
		Host:       this.Host,
		Username:   this.Username,
		Password:   this.Password,
		HttpClient: &http.Client{},
	}
	if db != "" {
		c.Database = this.TablePreifx + "-" + db
	}
	return c
}

func InitNewRelicTimer(ctx *context.Context) {
	startTime := time.Now()
	ctx.Input.SetData("influxdb_timer", startTime)
}
func (this *BeegoInfluxDb) ReportMetricsToNewrelic(ctx *context.Context) {
	startTimeInterface := ctx.Input.GetData("influxdb_timer")
	if startTime, ok := startTimeInterface.(time.Time); ok {
		url := ctx.Request.URL.String()
		path := ctx.Request.URL.Path
		if time.Since(startTime) > this.SlowThreshold {
			st := slowQueriesRegister.GetOrRegister("timer"+url, func() metrics.Timer { return metrics.NewTimer() }).(metrics.Timer)
			st.UpdateSince(startTime)
		}
		t := metrics.GetOrRegister("timer"+path, func() metrics.Timer { return metrics.NewTimer() }).(metrics.Timer)
		t.UpdateSince(startTime)
	}
}

func (this *BeegoInfluxDb) SendToInfluxDb() {
	go Influxdb(metrics.DefaultRegistry, this.SendingInterval, this.GetClientConfig(dbs[defaultDb]))
	go Influxdb(slowQueriesRegister, this.SendingInterval, this.GetClientConfig(dbs[slowQueryDb]))
}

func Influxdb(r metrics.Registry, d time.Duration, config *client.ClientConfig) {
	c, err := client.New(config)
	if err != nil {
		log.Println(err)
		return
	}
	for _ = range time.Tick(d) {
		beego.Debug("Sending data to infuxdb ", config.Database, "every", d)
		if err := send(r, c); err != nil {
			log.Println(err)
		}
	}
}

func send(r metrics.Registry, c *client.Client) error {
	series := []*client.Series{}

	r.Each(func(name string, i interface{}) {
		now := getCurrentTime()
		switch metric := i.(type) {
		case metrics.Counter:
			series = append(series, &client.Series{
				Name:    fmt.Sprintf("%s.count", name),
				Columns: []string{"time", "count"},
				Points: [][]interface{}{
					{now, metric.Count()},
				},
			})
		case metrics.Gauge:
			series = append(series, &client.Series{
				Name:    fmt.Sprintf("%s.value", name),
				Columns: []string{"time", "value"},
				Points: [][]interface{}{
					{now, metric.Value()},
				},
			})
		case metrics.GaugeFloat64:
			series = append(series, &client.Series{
				Name:    fmt.Sprintf("%s.value", name),
				Columns: []string{"time", "value"},
				Points: [][]interface{}{
					{now, metric.Value()},
				},
			})
		case metrics.Histogram:
			h := metric.Snapshot()
			ps := h.Percentiles([]float64{0.5, 0.75, 0.95, 0.99, 0.999})
			series = append(series, &client.Series{
				Name: fmt.Sprintf("%s.histogram", name),
				Columns: []string{"time", "count", "min", "max", "mean", "std-dev",
					"50-percentile", "75-percentile", "95-percentile",
					"99-percentile", "999-percentile"},
				Points: [][]interface{}{
					{now, h.Count(), h.Min(), h.Max(), h.Mean(), h.StdDev(),
						ps[0], ps[1], ps[2], ps[3], ps[4]},
				},
			})
		case metrics.Meter:
			m := metric.Snapshot()
			series = append(series, &client.Series{
				Name: fmt.Sprintf("%s.meter", name),
				Columns: []string{"count", "one-minute",
					"five-minute", "fifteen-minute", "mean"},
				Points: [][]interface{}{
					{m.Count(), m.Rate1(), m.Rate5(), m.Rate15(), m.RateMean()},
				},
			})
		case metrics.Timer:
			h := metric.Snapshot()
			ps := h.Percentiles([]float64{0.5, 0.75, 0.95, 0.99, 0.999})
			series = append(series, &client.Series{
				Name: fmt.Sprintf("%s.timer", name),
				Columns: []string{"count", "min", "max", "mean", "std-dev",
					"50-percentile", "75-percentile", "95-percentile",
					"99-percentile", "999-percentile", "one-minute", "five-minute", "fifteen-minute", "mean-rate"},
				Points: [][]interface{}{
					{h.Count(), h.Min(), h.Max(), h.Mean(), h.StdDev(),
						ps[0], ps[1], ps[2], ps[3], ps[4],
						h.Rate1(), h.Rate5(), h.Rate15(), h.RateMean()},
				},
			})
		}
	})
	if len(series) == 0 {
		return nil
	}
	if err := c.WriteSeries(series); err != nil {
		log.Println(err)
	} else {
		beego.Debug("Sent data to influxdb server.")
		// We want to remove the registers which have been sent to the influxdb server
		r.UnregisterAll()
	}
	return nil
}

func getCurrentTime() int64 {
	return time.Now().UnixNano() / 1000000
}

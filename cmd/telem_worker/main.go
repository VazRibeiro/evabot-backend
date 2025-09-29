package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"os"
	"strings"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
	"github.com/nats-io/nats.go"
)

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func unixAnyToTime(ts int64) time.Time {
	// Heuristics: current epoch in...
	//   s  ~ 1e9
	//   ms ~ 1e12
	//   µs ~ 1e15
	//   ns ~ 1e18
	switch {
	case ts >= 1e17: // nanoseconds
		return time.Unix(0, ts)
	case ts >= 1e14: // microseconds
		return time.Unix(0, ts*1_000)
	case ts >= 1e11: // milliseconds
		return time.Unix(0, ts*1_000_000)
	default: // seconds
		return time.Unix(ts, 0)
	}
}

type anyMap = map[string]interface{}

func asInt64(v interface{}) (int64, bool) {
	switch t := v.(type) {
	case float64:
		// JSON numbers are float64; only treat as int if it's an integer
		if math.Trunc(t) == t {
			return int64(t), true
		}
	case int64:
		return t, true
	case json.Number:
		if i, err := t.Int64(); err == nil {
			return i, true
		}
	}
	return 0, false
}

// flatten one level of { "data": { ... } } into fields
func extractFields(m anyMap) (fields map[string]interface{}, topic string) {
	fields = map[string]interface{}{}
	// optional topic
	if tv, ok := m["topic"].(string); ok {
		topic = tv
	}
	// prefer "data" block for numeric/bool fields
	if dv, ok := m["data"].(map[string]interface{}); ok {
		for k, v := range dv {
			switch vv := v.(type) {
			case float64:
				// numeric
				fields[k] = vv
			case bool:
				fields[k] = vv
			case json.Number:
				if f, err := vv.Float64(); err == nil {
					fields[k] = f
				}
			}
		}
	}
	// also allow top-level numeric/bool fields
	for k, v := range m {
		if k == "data" || k == "topic" || k == "trace_id" || k == "ts_ns" {
			continue
		}
		switch vv := v.(type) {
		case float64:
			fields[k] = vv
		case bool:
			fields[k] = vv
		case json.Number:
			if f, err := vv.Float64(); err == nil {
				fields[k] = f
			}
		}
	}
	return fields, topic
}

func main() {
	// --- NATS / JetStream ---
	natsURL := getenv("NATS_URL", "nats://127.0.0.1:4222")
	nc, err := nats.Connect(natsURL, nats.Name("evabot-telem-worker"))
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Drain()

	js, err := nc.JetStream()
	if err != nil {
		log.Fatal(err)
	}

	// --- Influx ---
	influxURL := getenv("INFLUX_URL", "http://127.0.0.1:8086")
	influxOrg := getenv("INFLUX_ORG", "r4f")
	influxBucket := getenv("INFLUX_BUCKET", "telemetry_raw")
	influxToken := os.Getenv("INFLUX_TOKEN")

	var write api.WriteAPIBlocking
	var influxClient influxdb2.Client
	if influxToken != "" {
		influxClient = influxdb2.NewClient(influxURL, influxToken)
		defer influxClient.Close()
		write = influxClient.WriteAPIBlocking(influxOrg, influxBucket)
		log.Printf("Influx enabled → %s (org=%s bucket=%s)", influxURL, influxOrg, influxBucket)
	} else {
		log.Printf("Influx disabled (no INFLUX_TOKEN). Will just log.")
	}

	// Durable consumer; manual ack for at-least-once semantics
	sub, err := js.Subscribe("telemetry.>", func(msg *nats.Msg) {
		// default timestamp = JetStream server timestamp
		ts := time.Now()
		if md, e := msg.Metadata(); e == nil {
			ts = md.Timestamp
		}

		// parse JSON if possible
		raw := string(msg.Data)
		dec := json.NewDecoder(bytes.NewReader(msg.Data))
		dec.UseNumber()
		var parsed map[string]interface{}
		_ = dec.Decode(&parsed)

		// consider ts_ns override
		if v, ok := asInt64(parsed["ts_ns"]); ok && v > 0 {
			ts = unixAnyToTime(v)
		}

		now := time.Now()
		if ts.Before(now.AddDate(-10, 0, 0)) || ts.After(now.Add(24*time.Hour)) {
			log.Printf("drop bad timestamp %s (subject=%s)", ts.Format(time.RFC3339Nano), msg.Subject)
			_ = msg.Ack() // do NOT retry this one
			return
		}

		fields, topic := extractFields(parsed)
		// always keep raw for debug
		fields["raw"] = raw

		tags := map[string]string{
			"subject": msg.Subject,
		}
		if topic != "" {
			tags["topic"] = topic
		}

		if write != nil {
			p := influxdb2.NewPoint("telemetry", tags, fields, ts)
			if err := write.WritePoint(context.Background(), p); err != nil {
				// If Influx says this point can never be accepted, ack it so it doesn't loop.
				if strings.Contains(err.Error(), "outside retention policy") ||
					strings.Contains(err.Error(), "unprocessable entity") {
					log.Printf("drop unsalvageable point (%s): %v", ts.Format(time.RFC3339Nano), err)
					_ = msg.Ack()
					return
				}
				// Otherwise it's likely transient (network, etc): let JetStream retry.
				log.Printf("influx write error (will retry): %v", err)
				_ = msg.Nak()
				return
			}
		} else {
			fmt.Printf("telemetry %s @ %s: %s\n", msg.Subject, ts.Format(time.RFC3339Nano), raw)
		}

		_ = msg.Ack()
	}, nats.Durable("telem-worker"), nats.ManualAck(), nats.AckWait(30*time.Second), nats.MaxDeliver(3))
	if err != nil {
		log.Fatal(err)
	}
	defer sub.Drain()

	log.Printf("Worker running. NATS=%s subject=telemetry.>", natsURL)
	select {}
}

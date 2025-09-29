package main

import (
	"log"
	"net/http"
	"os"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/gorilla/websocket"
	"github.com/nats-io/nats.go"

	"encoding/json"
	"regexp"
	"strings"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
)

var influxClient influxdb2.Client
var influxOrg, influxBucket string

var upgrader = websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func main() {
	natsURL := env("NATS_URL", "nats://127.0.0.1:4222")
	nc, err := nats.Connect(natsURL)
	must(err)
	js, err := nc.JetStream()
	must(err)

	// Ensure streams exist
	ensure := func(cfg *nats.StreamConfig) {
		if _, err := js.AddStream(cfg); err != nil && err != nats.ErrStreamNameAlreadyInUse {
			log.Fatal(err)
		}
	}
	ensure(&nats.StreamConfig{Name: "TELEMETRY", Subjects: []string{"telemetry.>"}, Storage: nats.FileStorage, MaxAge: 365 * 24 * time.Hour})
	ensure(&nats.StreamConfig{Name: "CTRL", Subjects: []string{"ctrl.>"}, Storage: nats.MemoryStorage, MaxMsgsPerSubject: 1000})

	influxURL := env("INFLUX_URL", "http://127.0.0.1:8086")
	influxOrg = env("INFLUX_ORG", "r4f")
	influxBucket = env("INFLUX_BUCKET", "telemetry_raw")
	influxToken := os.Getenv("INFLUX_TOKEN")
	if influxToken != "" {
		influxClient = influxdb2.NewClient(influxURL, influxToken)
		defer influxClient.Close()
		log.Printf("influx query enabled → %s (org=%s bucket=%s)", influxURL, influxOrg, influxBucket)
	} else {
		log.Printf("influx query disabled (no INFLUX_TOKEN)")
	}

	r := chi.NewRouter()
	r.Get("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(204) })

	// WebSocket: stream TELEMETRY to client
	r.Get("/ws", func(w http.ResponseWriter, req *http.Request) {
		c, err := upgrader.Upgrade(w, req, nil)
		if err != nil {
			return
		}
		defer c.Close()

		sub, err := js.SubscribeSync("telemetry.>")
		if err != nil {
			log.Println(err)
			return
		}
		defer sub.Unsubscribe()

		for {
			msg, err := sub.NextMsg(5 * time.Second)
			if err != nil {
				continue
			} // idle
			if err := c.WriteMessage(websocket.BinaryMessage, msg.Data); err != nil {
				return
			}
		}
	})

	// REST: e-stop (publish a tiny JSON)
	r.Post("/api/robot/{id}/estop", func(w http.ResponseWriter, req *http.Request) {
		id := chi.URLParam(req, "id")
		_, err := js.Publish("ctrl."+id+".estop", []byte(`{"reason":"ui"}`))
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		w.WriteHeader(204)
	})

	// GET /api/ts?field=angle_deg&subject=telemetry.demo&start=-15m&window=1s
	r.Get("/api/ts", func(w http.ResponseWriter, req *http.Request) {
		if influxClient == nil {
			http.Error(w, "influx not configured", http.StatusNotImplemented)
			return
		}

		field := req.URL.Query().Get("field")
		if field == "" {
			field = "raw"
		}
		subject := req.URL.Query().Get("subject") // optional
		start := req.URL.Query().Get("start")
		if start == "" {
			start = "-15m"
		}
		window := req.URL.Query().Get("window") // optional; mean aggregation

		// basic input hygiene for durations; allow RFC3339 too
		okDur, _ := regexp.MatchString(`^-\d+[smhdw]$`, start)
		if !okDur && !strings.Contains(start, "T") {
			http.Error(w, "bad 'start' (use -15m or RFC3339 time)", 400)
			return
		}

		flux := strings.Builder{}
		flux.WriteString(`from(bucket:"` + influxBucket + `") |> range(start:` + start + `)`)
		flux.WriteString(` |> filter(fn:(r)=> r._measurement == "telemetry")`)
		flux.WriteString(` |> filter(fn:(r)=> r._field == "` + field + `")`)
		if subject != "" {
			flux.WriteString(` |> filter(fn:(r)=> r.subject == "` + subject + `")`)
		}
		if window != "" && field != "raw" {
			flux.WriteString(` |> aggregateWindow(every:` + window + `, fn: mean, createEmpty: false)`)
		}
		flux.WriteString(` |> keep(columns: ["_time","_value","subject"])`)

		q := influxClient.QueryAPI(influxOrg)
		res, err := q.Query(req.Context(), flux.String())
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		defer res.Close()

		type point struct {
			T time.Time   `json:"t"`
			V interface{} `json:"v"`
		}
		type series struct {
			Subject string  `json:"subject"`
			Points  []point `json:"points"`
		}

		if subject != "" {
			out := struct {
				Field   string  `json:"field"`
				Subject string  `json:"subject"`
				Points  []point `json:"points"`
			}{
				Field: field, Subject: subject, Points: make([]point, 0), // ensure [] not null
			}

			for res.Next() {
				out.Points = append(out.Points, point{T: res.Record().Time(), V: res.Record().Value()})
			}
			if res.Err() != nil {
				http.Error(w, res.Err().Error(), 500)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(out)
			return
		}

		// group by subject
		m := map[string][]point{}
		for res.Next() {
			s := res.Record().ValueByKey("subject")
			sub, _ := s.(string)
			m[sub] = append(m[sub], point{T: res.Record().Time(), V: res.Record().Value()})
		}
		if res.Err() != nil {
			http.Error(w, res.Err().Error(), 500)
			return
		}

		out := struct {
			Field  string   `json:"field"`
			Series []series `json:"series"`
		}{
			Field:  field,
			Series: make([]series, 0), // ensure [] not null
		}
		for sub, pts := range m {
			out.Series = append(out.Series, series{Subject: sub, Points: pts})
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(out)
	})

	addr := env("BIND", ":8080")
	log.Printf("backend listening on %s (NATS %s)", addr, natsURL)
	must(http.ListenAndServe(addr, r))
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

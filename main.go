package main

import (
	"log"
	"net/http"
	"os"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/gorilla/websocket"
	"github.com/nats-io/nats.go"
)

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

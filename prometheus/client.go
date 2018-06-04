package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	requestsMapStatus = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "io_http_requests_total",
		Help: "The total number of requests which were performed.",
	}, []string{"account", "workspace", "method", "path", "status"})

	requestsMapCurrent = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "io_http_requests_current",
		Help: "The current number of requests in course.",
	}, []string{"account", "workspace", "method", "path"})

	requestsMapDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "io_http_request_duration_seconds",
		Help: "The duration of the requests in seconds.",
	}, []string{"account", "workspace", "method", "path"})
)

var client PrometheusClient

type PrometheusClient interface {
	OpenRequest(req RequestData)
	ObserveDuration(req RequestData, initTime time.Time)
	CloseRequest(req RequestData, status string)
}

type prometheusClient struct {
}

type RequestData struct {
	Account, Workspace, Method, Path string
}

func (p *prometheusClient) OpenRequest(req RequestData) {
	labels := getDefaultLabels(req)
	requestsMapCurrent.With(labels).Inc()
}

func (p *prometheusClient) ObserveDuration(req RequestData, initTime time.Time) {
	labels := getDefaultLabels(req)
	requestsMapDuration.With(labels).Observe(time.Since(initTime).Seconds())
}

func (p *prometheusClient) CloseRequest(req RequestData, status string) {
	labels := getDefaultLabels(req)
	requestsMapCurrent.With(labels).Dec()

	labels["status"] = status
	requestsMapStatus.With(labels).Inc()
}

func getDefaultLabels(req RequestData) prometheus.Labels {
	return prometheus.Labels{"account": req.Account, "workspace": req.Workspace, "method": req.Method, "path": req.Path}
}

func InitClient() {
	if client != nil {
		panic("The client has already been initialized.")
	}

	prometheus.MustRegister(requestsMapStatus)
	prometheus.MustRegister(requestsMapCurrent)
	prometheus.MustRegister(requestsMapDuration)

	client = &prometheusClient{}
}

func GetClient() PrometheusClient {
	return client
}

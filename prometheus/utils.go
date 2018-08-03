package prometheus

import (
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
)

func RequestsStatusTracker(serviceName string, version string, path string) gin.HandlerFunc {
	return func(g *gin.Context) {
		startTracker(g, serviceName, version, path, false)
	}
}

func MetricsTracker(serviceName string, version string, path string) gin.HandlerFunc {
	return func(g *gin.Context) {
		startTracker(g, serviceName, version, path, true)
	}
}

func startTracker(g *gin.Context, serviceName, version, path string, withLatency bool) {
	startTime := time.Now()
	reqData := prepareRequestInfo(serviceName, version, path, g)
	client.OpenRequest(reqData)

	g.Next()

	if withLatency {
		client.ObserveDuration(reqData, startTime)
	}
	client.CloseRequest(reqData, strconv.Itoa(g.Writer.Status()))
}

func prepareRequestInfo(serviceName string, version string, path string, g *gin.Context) RequestData {
	return RequestData{
		ServiceName: serviceName,
		Version:     version,
		Method:      g.Request.Method,
		Path:        path,
	}
}

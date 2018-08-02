package prometheus

import (
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
)

func MetricsTracker(serviceName string, version string, path string) gin.HandlerFunc {
	return func(g *gin.Context) {
		reqData := prepareRequestInfo(serviceName, version, path, g)
		startTracker(g, reqData)
	}
}

func startTracker(g *gin.Context, reqData RequestData) {
	startTime := time.Now()
	client.OpenRequest(reqData)

	g.Next()

	client.ObserveDuration(reqData, startTime)
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

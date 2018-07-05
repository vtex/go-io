package prometheus

import (
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

func MetricsTracker() gin.HandlerFunc {
	return func(g *gin.Context) {
		reqData := getRequestInfo(g)
		startTracker(g, reqData)
	}
}

func MetricsTrackerWithPath(path string) gin.HandlerFunc {
	return func(g *gin.Context) {
		reqData := prepareRequestInfo(g, path)
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

func getRequestInfo(g *gin.Context) RequestData {
	pathSplitted := strings.SplitN(g.Request.URL.Path, "/", 4)
	path := "/" + pathSplitted[3]
	return prepareRequestInfo(g, path)
}

func prepareRequestInfo(g *gin.Context, path string) RequestData {
	return RequestData{
		Account:   g.Param("account"),
		Workspace: g.Param("workspace"),
		Method:    g.Request.Method,
		Path:      path,
	}
}

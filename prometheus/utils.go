package prometheus

import (
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

func InitRequest(g *gin.Context) {
	GetClient().OpenRequest(getRequestInfo(g))
}

func Observe(g *gin.Context, init time.Time) {
	GetClient().ObserveDuration(getRequestInfo(g), init)
}

func EndRequest(g *gin.Context) {
	GetClient().CloseRequest(getRequestInfo(g), strconv.Itoa(g.Writer.Status()))
}

func InitRequestWithPath(path string) gin.HandlerFunc {
	return func(g *gin.Context) {
		reqData := prepareRequestInfo(g, path)
		GetClient().OpenRequest(reqData)
	}
}

func EndRequestWithPath(path string) gin.HandlerFunc {
	return func(g *gin.Context) {
		reqData := prepareRequestInfo(g, path)
		GetClient().CloseRequest(reqData, strconv.Itoa(g.Writer.Status()))
	}
}

func ObserveWithPath(g *gin.Context, init time.Time, path string) {
	reqData := prepareRequestInfo(g, path)
	GetClient().ObserveDuration(reqData, init)
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

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

func getRequestInfo(g *gin.Context) RequestData {
	path := strings.SplitN(g.Request.URL.Path, "/", 4)
	return RequestData{
		Account:   g.Param("account"),
		Workspace: g.Param("workspace"),
		Method:    g.Request.Method,
		Path:      "/" + path[3],
	}
}

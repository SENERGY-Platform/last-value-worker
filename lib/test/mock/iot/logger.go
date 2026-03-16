package iot

import (
	"bytes"
	"io"
	"io/ioutil"
	"net/http"

	"github.com/SENERGY-Platform/go-service-base/struct-logger/attributes"
	"github.com/SENERGY-Platform/last-value-worker/lib/log"
)

func NewLogger(handler http.Handler, logLevel string) *LoggerMiddleWare {
	return &LoggerMiddleWare{handler: handler, logLevel: logLevel}
}

type LoggerMiddleWare struct {
	handler  http.Handler
	logLevel string //DEBUG | CALL | NONE
}

func (this *LoggerMiddleWare) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	this.log(r)
	if this.handler != nil {
		this.handler.ServeHTTP(w, r)
	} else {
		http.Error(w, "Forbidden", 403)
	}
}

func (this *LoggerMiddleWare) log(request *http.Request) {
	if this.logLevel != "NONE" {
		method := request.Method
		path := request.URL

		if this.logLevel == "CALL" {
			log.Logger.Info("iot request", "method", method, "path", path)
		}

		if this.logLevel == "DEBUG" {
			//read on request.Body would empty it -> create new ReadCloser for request.Body while reading
			var buf bytes.Buffer
			temp := io.TeeReader(request.Body, &buf)
			b, err := ioutil.ReadAll(temp)
			if err != nil {
				log.Logger.Error("read error in debuglog", attributes.ErrorKey, err)
			}
			request.Body = ioutil.NopCloser(bytes.NewReader(buf.Bytes()))

			client := request.RemoteAddr
			log.Logger.Debug("iot request debug", "client", client, "method", method, "path", path, "body", string(b))

		}

	}
}

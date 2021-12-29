// OpenIO SDS Go rawx
// Copyright (C) 2015-2020 OpenIO SAS
// Copyright (C) 2021 OVH SAS
//
// This library is free software; you can redistribute it and/or
// modify it under the terms of the GNU Affero General Public
// License as published by the Free Software Foundation; either
// version 3.0 of the License, or (at your option) any later version.
//
// This library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
// Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public
// License along with this program. If not, see <http://www.gnu.org/licenses/>.

package main

import (
	"bytes"
	"fmt"
	"log"
	"log/syslog"
	"os"
	"sync"
	"text/template"
	"time"
)

type oioLogger interface {
	close()
	writeAccess(message string)
	writeInfo(message string)
	writeError(message string)
}

var accessLogGet = configAccessLogDefaultGet
var accessLogPut = configAccessLogDefaultPut
var accessLogDel = configAccessLogDefaultDelete

var logFormat = "{{ .Pid }} log {{ .Severity }} - {{ .Message }}"
var logRequestFormat = "{{ .Pid }} log {{ .Severity }} - {{ .Local }} {{ .Peer }} {{ .Method }} - {{ .ReqId }} {{ .Path }} http{{ if .TLS }}s{{ end }} - {{ .Message }}"
var logAccessFormat = "{{ .Pid }} access INF - {{ .Local }} {{ .Peer }} {{ .Method }} {{ .Status }} {{ .TimeSpent }} {{ .BytesOut }} {{ .BytesIn }} - {{ .ReqId }} {{ .Path }} http{{ if .TLS }}s{{ end }} {{ .TTFB }}"
var logTemplate *template.Template = nil
var logRequestTemplate *template.Template = nil
var logAccessTemplate *template.Template = nil

// Activate the extreme verbosity on the RAWX. This is has to be set at the
// startup of the service.
var logExtremeVerbosity = false

// The high severity (a.k.a. log level) that will be logged by the application.
var logDefaultSeverity = syslog.LOG_NOTICE

// When using
var logSeverity = logDefaultSeverity

// The RAWX doesn't daemonize, we can save one syscall for each access log line
// with this little variable caching the PID once for all.
var pid = os.Getpid()

// The singleton logger that will be used by all the coroutine
var logger oioLogger

type AccessLogEvent struct {
	Pid       int
	Status    int
	BytesIn   uint64
	BytesOut  uint64
	TimeSpent uint64
	Method    string
	Local     string
	Peer      string
	Path      string
	ReqId     string
	TLS       bool
	TTFB      uint64
}

type NoopLogger struct{}

type StderrLogger struct {
	logger *log.Logger
}

type LogTemplateInventory struct {
	Pid      int
	Severity string
	Message  string
}

type LogRequestTemplateInventory struct {
	Pid      int
	Severity string
	Local    string
	Peer     string
	Method   string
	ReqId    string
	Path     string
	TLS      bool
	Message  string
}

func InitLogTemplates() error {
	var err error
	logTemplate, err = template.New("logTemplate").Parse(logFormat)
	if err != nil {
		return err
	}
	logRequestTemplate, err = template.New("logRequestTemplate").Parse(logRequestFormat)
	if err != nil {
		return err
	}
	logAccessTemplate, err = template.New("logAccessTemplate").Parse(logAccessFormat)
	return err
}

func isVerbose() bool {
	return logExtremeVerbosity && severityAllowed(syslog.LOG_DEBUG)
}

func severityAllowed(severity syslog.Priority) bool {
	return severity <= logSeverity
}

func initVerbosity(severity syslog.Priority) {
	logDefaultSeverity = severity
	logSeverity = severity
}

func maximizeVerbosity() {
	logExtremeVerbosity = true
	logDefaultSeverity = syslog.LOG_DEBUG
	logSeverity = syslog.LOG_DEBUG
}

func increaseVerbosity() {
	if logSeverity < syslog.LOG_DEBUG {
		logSeverity = logSeverity + 1
	}
}

func resetVerbosity() {
	logSeverity = logDefaultSeverity
}

func getSeverity(priority syslog.Priority) (bool, string) {
	switch priority {
	case syslog.LOG_EMERG, syslog.LOG_CRIT, syslog.LOG_ERR:
		return true, "ERR"
	case syslog.LOG_WARNING:
		return false, "WRN"
	case syslog.LOG_NOTICE, syslog.LOG_INFO:
		return false, "INF"
	default:
		return false, "DBG"
	}
}

func writeLogFmt(pri syslog.Priority, format string, v ...interface{}) {
	if !severityAllowed(pri) {
		return
	}
	erroneous, severityName := getSeverity(pri)

	var output bytes.Buffer
	if logTemplate != nil {
		err := logTemplate.Execute(&output, LogTemplateInventory{
			Pid:      pid,
			Severity: severityName,
			Message:  fmt.Sprintf(format, v...),
		})

		if err != nil {
			log.Printf("Error while executing logTemplate: %v", err)
			return
		}
	} else {
		log.Printf(format, v...)
		return
	}

	if erroneous {
		logger.writeError(output.String())
	} else {
		logger.writeInfo(output.String())
	}
}

func LogFatal(format string, v ...interface{}) {
	writeLogFmt(syslog.LOG_ERR, format, v...)
	log.Fatalf(format, v...)
}

func LogError(format string, v ...interface{}) {
	writeLogFmt(syslog.LOG_ERR, format, v...)
}

func LogWarning(format string, v ...interface{}) {
	writeLogFmt(syslog.LOG_WARNING, format, v...)
}

func LogInfo(format string, v ...interface{}) {
	writeLogFmt(syslog.LOG_INFO, format, v...)
}

func LogDebug(format string, v ...interface{}) {
	writeLogFmt(syslog.LOG_DEBUG, format, v...)
}

func writeLogRequestFmt(rr *rawxRequest, pri syslog.Priority, format string, v ...interface{}) {
	if !severityAllowed(pri) {
		return
	}
	erroneous, severityName := getSeverity(pri)

	var output bytes.Buffer
	if logRequestTemplate != nil {
		var local string
		var peer string
		var method string
		var reqId string
		var path string
		var TLS bool
		if rr != nil {
			local = rr.req.Host
			peer = rr.req.RemoteAddr
			method = rr.req.Method
			reqId = rr.reqid
			path = rr.req.URL.Path
			TLS = rr.req.TLS != nil
		} else {
			local = ""
			peer = ""
			method = ""
			reqId = ""
			path = ""
			TLS = false
		}
		err := logRequestTemplate.Execute(&output, LogRequestTemplateInventory{
			Pid:      pid,
			Severity: severityName,
			Local:    local,
			Peer:     peer,
			Method:   method,
			ReqId:    reqId,
			Path:     path,
			TLS:      TLS,
			Message:  fmt.Sprintf(format, v...),
		})

		if err != nil {
			log.Printf("Error while executing logRequestTemplate: %v", err)
			return
		}
	} else {
		log.Printf(format, v...)
		return
	}

	if erroneous {
		logger.writeError(output.String())
	} else {
		logger.writeInfo(output.String())
	}
}

func LogRequestFatal(rr *rawxRequest, format string, v ...interface{}) {
	writeLogRequestFmt(rr, syslog.LOG_ERR, format, v...)
	log.Fatalf(format, v...)
}

func LogRequestError(rr *rawxRequest, format string, v ...interface{}) {
	writeLogRequestFmt(rr, syslog.LOG_ERR, format, v...)
}

func LogRequestWarning(rr *rawxRequest, format string, v ...interface{}) {
	writeLogRequestFmt(rr, syslog.LOG_WARNING, format, v...)
}

func LogRequestInfo(rr *rawxRequest, format string, v ...interface{}) {
	writeLogRequestFmt(rr, syslog.LOG_INFO, format, v...)
}

func LogRequestDebug(rr *rawxRequest, format string, v ...interface{}) {
	writeLogRequestFmt(rr, syslog.LOG_DEBUG, format, v...)
}

func (evt AccessLogEvent) String() string {
	evt.Pid = pid
	var output bytes.Buffer
	err := logAccessTemplate.Execute(&output, evt)

	if err != nil {
		log.Printf("Error while executing logAccessTemplate: %v", err)
		return ""
	}
	return output.String()
}

func LogHttp(evt AccessLogEvent) {
	logger.writeAccess(evt.String())
}

func InitNoopLogger() {
	logger = &NoopLogger{}
}

func (*NoopLogger) writeAccess(string) {}
func (*NoopLogger) writeInfo(string)   {}
func (*NoopLogger) writeError(string)  {}
func (*NoopLogger) close()             {}

func InitStderrLogger() {
	initVerbosity(syslog.LOG_DEBUG)
	var stderrLogger StderrLogger
	stderrLogger.logger = log.New(os.Stderr, "", 0)
	stderrLogger.logger.SetFlags(log.Ldate | log.Lshortfile | log.Llongfile | log.Lmicroseconds)
	logger = &stderrLogger
}

func (l *StderrLogger) writeAll(m string) {
	now := time.Now()
	l.logger.Println(fmt.Sprintf("%v.%06d", now.Unix(), (now.UnixNano()/1000)%1000000), m)
}

func (l *StderrLogger) writeAccess(m string) { l.writeAll(m) }
func (l *StderrLogger) writeInfo(m string)   { l.writeAll(m) }
func (l *StderrLogger) writeError(m string)  { l.writeAll(m) }
func (l *StderrLogger) close()               {}

type SysLogger struct {
	queue         chan string
	wg            sync.WaitGroup
	running       bool
	syslogID      string
	alertThrottle PeriodicThrottle
	loggerAccess  *syslog.Writer
	loggerInfo    *syslog.Writer
	loggerError   *syslog.Writer
}

func InitSysLogger(syslogID string) {
	initVerbosity(syslog.LOG_INFO)
	l := &SysLogger{}
	l.alertThrottle = PeriodicThrottle{period: 1000000000}
	l.queue = make(chan string, configAccessLogQueueDefaultLength)
	l.running = true
	l.syslogID = syslogID
	l.loggerAccess, _ = syslog.New(syslog.LOG_LOCAL1|syslog.LOG_INFO, syslogID)
	l.loggerInfo, _ = syslog.New(syslog.LOG_LOCAL0|syslog.LOG_INFO, syslogID)
	l.loggerError, _ = syslog.New(syslog.LOG_LOCAL0|syslog.LOG_ERR, syslogID)
	l.wg.Add(1)
	go func() {
		for evt := range l.queue {
			l.loggerAccess.Info(evt)
		}
		l.wg.Done()
	}()
	logger = l
}

func (l *SysLogger) writeAccess(m string) {
	select {
	case l.queue <- m: // no-blocking call, everything is fine
	default:
		if l.alertThrottle.Ok() {
			LogWarning("syslog clogged")
		}
		// FIXME(jfs): Uncomment this upon an absolute necessity
		// l.queue <- m
	}
}

func (l *SysLogger) writeInfo(m string)  { l.loggerInfo.Info(m) }
func (l *SysLogger) writeError(m string) { l.loggerError.Err(m) }
func (l *SysLogger) close() {
	l.running = false
	close(l.queue)
	l.wg.Wait()
}

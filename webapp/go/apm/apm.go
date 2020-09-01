package apm

import (
	"github.com/newrelic/go-agent/v3/newrelic"
	goji "goji.io"
	"goji.io/pat"
	"net/http"
)

const (
	DBInsert string = "INSERT"
	DBSelect string = "SELECT"
	DBUpdate string = "UPDATE"
	DBDelete string = "DELETE"
)

var app *newrelic.Application

// Transaction (NoWeb)トランザクション用
type Transaction struct {
	txn *newrelic.Transaction
}

func (t *Transaction) End() {
	if t.txn != nil {
		t.txn.End()
	}
}

// DatastoreSegment (NoWeb)Datastore用セグメント
type DatastoreSegment struct {
	seg newrelic.DatastoreSegment
}

func (d DatastoreSegment) End() {
	if isEnable() {
		d.seg.End()
	}
}

// Setup APM設定
func Setup(appName, license string) (err error) {
	if appName == "" || license == "" {
		return
	}

	app, err = newrelic.NewApplication(
		newrelic.ConfigAppName(appName),
		newrelic.ConfigLicense(license),
		newrelic.ConfigDistributedTracerEnabled(true),
	)
	return
}

// HandleFunc Web handler 設定
func HandleFunc(mux *goji.Mux, pattern *pat.Pattern, hdl http.HandlerFunc) {
	whdl := hdl
	if isEnable() {
		_, whdl = newrelic.WrapHandleFunc(app, pattern.String(), hdl)
	}
	mux.HandleFunc(pattern, whdl)
}

// Handle Web handler 設定
func Handle(mux *goji.Mux, pattern *pat.Pattern, hdl http.Handler) {
	whdl := hdl
	if isEnable() {
		_, whdl = newrelic.WrapHandle(app, pattern.String(), hdl)
	}
	mux.Handle(pattern, whdl)
}

// StartTransaction (NoWeb)トランザクション開始
func StartTransaction(name string) *Transaction {
	st := new(Transaction)
	if isEnable() {
		st.txn = app.StartTransaction(name)
	}
	return st
}

// StartDatastoreSegment (NoWeb)Datastore用セグメント開始
func StartDatastoreSegment(tx *Transaction, sqlType, table, sql string) DatastoreSegment {
	d := DatastoreSegment{}
	if tx.txn != nil && isEnable() {
		d.seg = newrelic.DatastoreSegment{
			StartTime:          tx.txn.StartSegmentNow(),
			Product:            newrelic.DatastoreMySQL,
			Collection:         table,
			Operation:          sqlType,
			ParameterizedQuery: sql,
			QueryParameters:    map[string]interface{}{},
			Host:               "mysql-server-1",
			PortPathOrID:       "3306",
			DatabaseName:       "isucari",
		}
	}
	return d
}

func isEnable() bool {
	return app != nil
}

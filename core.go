package zapdriver

import (
	"fmt"
	"math"
	"strings"
	"time"

	"cloud.google.com/go/logging"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	logpb "google.golang.org/genproto/googleapis/logging/v2"
)

// driverConfig is used to configure core.
type driverConfig struct {
	// Report all logs with level error or above to stackdriver using
	// `ErrorReport()` when set to true
	ReportAllErrors bool

	// ServiceName is added as `ServiceContext()` to all logs when set
	ServiceName string
}

// Core is a zapdriver specific core wrapped around the default zap core. It
// allows to merge all defined labels
type core struct {
	zapcore.Core

	fields []zap.Field
	lg     *logging.Logger

	// permLabels is a collection of labels that have been added to the logger
	// through the use of `With()`. These labels should never be cleared after
	// logging a single entry, unlike `tempLabel`.
	permLabels *labels

	// tempLabels keeps a record of all the labels that need to be applied to the
	// current log entry. Zap serializes log fields at different parts of the
	// stack, one such location is when calling `core.With` and the other one is
	// when calling `core.Write`. This makes it impossible to (for example) take
	// all `labels.xxx` fields, and wrap them in the `labels` namespace in one go.
	//
	// Instead, we have to filter out these labels at both locations, and then add
	// them back in the proper format right before we call `Write` on the original
	// Zap core.
	tempLabels *labels

	// Configuration for the zapdriver core
	config driverConfig
}

// zapdriver core option to report all logs with level error or above to stackdriver
// using `ErrorReport()` when set to true
func ReportAllErrors(report bool) func(*core) {
	return func(c *core) {
		c.config.ReportAllErrors = report
	}
}

// zapdriver core option to add `ServiceContext()` to all logs with `name` as
// service name
func ServiceName(name string) func(*core) {
	return func(c *core) {
		c.config.ServiceName = name
	}
}

// WrapCore returns a `zap.Option` that wraps the default core with the
// zapdriver one.
func WrapCore(options ...func(*core)) zap.Option {
	return zap.WrapCore(func(c zapcore.Core) zapcore.Core {
		newcore := &core{
			Core:       c,
			permLabels: newLabels(),
			tempLabels: newLabels(),
		}
		for _, option := range options {
			option(newcore)
		}
		return newcore
	})
}

// With adds structured context to the Core.
func (c *core) With(fields []zap.Field) zapcore.Core {
	var lbls *labels
	lbls, fields = c.extractLabels(fields)

	lbls.mutex.RLock()
	c.permLabels.mutex.Lock()
	for k, v := range lbls.store {
		c.permLabels.store[k] = v
	}
	c.permLabels.mutex.Unlock()
	lbls.mutex.RUnlock()

	fieldsCopy := make([]zap.Field, len(c.fields), len(c.fields)+len(fields))
	copy(fieldsCopy, c.fields)
	fieldsCopy = append(fieldsCopy, fields...)
	return &core{
		fields:     fieldsCopy,
		lg:         c.lg,
		Core:       c.Core.With(fields),
		permLabels: c.permLabels,
		tempLabels: newLabels(),
		config:     c.config,
	}
}

// Check determines whether the supplied Entry should be logged (using the
// embedded LevelEnabler and possibly some extra logic). If the entry
// should be logged, the Core adds itself to the CheckedEntry and returns
// the result.
//
// Callers must use Check before calling Write.
func (c *core) Check(ent zapcore.Entry, ce *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if c.Enabled(ent.Level) {
		return ce.AddCore(ent, c)
	}

	return ce
}

var logLevelSeverityGoogle = map[zapcore.Level]logging.Severity{
	zapcore.DebugLevel:  logging.Debug,
	zapcore.InfoLevel:   logging.Info,
	zapcore.WarnLevel:   logging.Warning,
	zapcore.ErrorLevel:  logging.Error,
	zapcore.DPanicLevel: logging.Critical,
	zapcore.PanicLevel:  logging.Alert,
	zapcore.FatalLevel:  logging.Emergency,
}

func WithLogger(logger *logging.Logger) func(c *core) {
	return func(c *core) {
		c.lg = logger
	}
}

func ToInterface(f zapcore.Field) interface{} {
	switch f.Type {
	case zapcore.ArrayMarshalerType:
		return f.Interface.(zapcore.ArrayMarshaler)
	case zapcore.ObjectMarshalerType:
		return f.Interface.(zapcore.ObjectMarshaler)
	case zapcore.BinaryType:
		return f.Interface.([]byte)
	case zapcore.BoolType:
		return f.Integer == 1
	case zapcore.ByteStringType:
		return f.Interface.([]byte)
	case zapcore.Complex128Type:
		return f.Interface.(complex128)
	case zapcore.Complex64Type:
		return f.Interface.(complex64)
	case zapcore.DurationType:
		return time.Duration(f.Integer)
	case zapcore.Float64Type:
		return math.Float64frombits(uint64(f.Integer))
	case zapcore.Float32Type:
		return math.Float32frombits(uint32(f.Integer))
	case zapcore.Int64Type:
		return f.Integer
	case zapcore.Int32Type:
		return int32(f.Integer)
	case zapcore.Int16Type:
		return int16(f.Integer)
	case zapcore.Int8Type:
		return int8(f.Integer)
	case zapcore.StringType:
		return f.String
	case zapcore.TimeType:
		if f.Interface != nil {
			return time.Unix(0, f.Integer).In(f.Interface.(*time.Location))
		} else {
			// Fall back to UTC if location is nil.
			return time.Unix(0, f.Integer)
		}
	case zapcore.Uint64Type:
		return uint64(f.Integer)
	case zapcore.Uint32Type:
		return uint32(f.Integer)
	case zapcore.Uint16Type:
		return uint16(f.Integer)
	case zapcore.Uint8Type:
		return uint8(f.Integer)
	case zapcore.UintptrType:
		return uintptr(f.Integer)
	case zapcore.ReflectType:
		return f.Interface
	case zapcore.NamespaceType:
		return nil
	case zapcore.StringerType:
		return f.Interface
	case zapcore.ErrorType:
		return f.Interface
	case zapcore.SkipType:
		break
	default:
		return fmt.Sprintf("unknown field type: %v", f)
	}
	return nil
}

func (c *core) Write(ent zapcore.Entry, fields []zapcore.Field) error {
	//fmt.Printf("%#v | %v\n", ent, c.fields)
	var lbls *labels
	lbls, fields = c.extractLabels(fields)

	lbls.mutex.RLock()
	c.tempLabels.mutex.Lock()
	for k, v := range lbls.store {
		c.tempLabels.store[k] = v
	}
	c.tempLabels.mutex.Unlock()
	lbls.mutex.RUnlock()

	payload := map[string]interface{}{}

	for _, f := range c.fields {
		payload[f.Key] = ToInterface(f)
	}
	payload["message"] = ent.Message

	glog := logging.Entry{
		Timestamp:    ent.Time,
		Severity:     logLevelSeverityGoogle[ent.Level],
		Payload:      payload,
		Labels:       c.allLabels().store,
		InsertID:     "",
		HTTPRequest:  nil,
		Operation:    nil,
		LogName:      "",
		Resource:     nil,
		Trace:        "",
		SpanID:       "",
		TraceSampled: false,
		SourceLocation: &logpb.LogEntrySourceLocation{
			File: ent.Caller.File,
			Line: int64(ent.Caller.Line),
		},
	}
	//fmt.Printf("glog: %#v\n", glog)
	c.lg.Log(glog)

	fields = append(fields, labelsField(c.allLabels()))
	fields = c.withSourceLocation(ent, fields)
	if c.config.ServiceName != "" {
		fields = c.withServiceContext(c.config.ServiceName, fields)
	}
	if c.config.ReportAllErrors && zapcore.ErrorLevel.Enabled(ent.Level) {
		fields = c.withErrorReport(ent, fields)
		if c.config.ServiceName == "" {
			// A service name was not set but error report needs it
			// So attempt to add a generic service name
			fields = c.withServiceContext("unknown", fields)
		}
	}

	c.tempLabels.reset()

	err := c.Core.Write(ent, fields)
	return err
}

// Sync flushes buffered logs (if any).
func (c *core) Sync() error {
	_ = c.lg.Flush()
	return c.Core.Sync()
}

func (c *core) allLabels() *labels {
	lbls := newLabels()

	lbls.mutex.Lock()
	c.permLabels.mutex.RLock()
	for k, v := range c.permLabels.store {
		lbls.store[k] = v
	}
	c.permLabels.mutex.RUnlock()

	c.tempLabels.mutex.RLock()
	for k, v := range c.tempLabels.store {
		lbls.store[k] = v
	}
	c.tempLabels.mutex.RUnlock()
	lbls.mutex.Unlock()

	return lbls
}

func (c *core) extractLabels(fields []zapcore.Field) (*labels, []zapcore.Field) {
	lbls := newLabels()
	out := []zapcore.Field{}

	lbls.mutex.Lock()
	for i := range fields {
		if !isLabelField(fields[i]) {
			out = append(out, fields[i])
			continue
		}

		lbls.store[strings.Replace(fields[i].Key, "labels.", "", 1)] = fields[i].String
	}
	lbls.mutex.Unlock()

	return lbls, out
}

func (c *core) withLabels(fields []zapcore.Field) []zapcore.Field {
	lbls := newLabels()
	out := []zapcore.Field{}

	lbls.mutex.Lock()
	for i := range fields {
		if isLabelField(fields[i]) {
			lbls.store[strings.Replace(fields[i].Key, "labels.", "", 1)] = fields[i].String
			continue
		}

		out = append(out, fields[i])
	}
	lbls.mutex.Unlock()

	return append(out, labelsField(lbls))
}

func (c *core) withSourceLocation(ent zapcore.Entry, fields []zapcore.Field) []zapcore.Field {
	// If the source location was manually set, don't overwrite it
	for i := range fields {
		if fields[i].Key == sourceKey {
			return fields
		}
	}

	if !ent.Caller.Defined {
		return fields
	}

	return append(fields, SourceLocation(ent.Caller.PC, ent.Caller.File, ent.Caller.Line, true))
}

func (c *core) withServiceContext(name string, fields []zapcore.Field) []zapcore.Field {
	// If the service context was manually set, don't overwrite it
	for i := range fields {
		if fields[i].Key == serviceContextKey {
			return fields
		}
	}

	return append(fields, ServiceContext(name))
}

func (c *core) withErrorReport(ent zapcore.Entry, fields []zapcore.Field) []zapcore.Field {
	// If the error report was manually set, don't overwrite it
	for i := range fields {
		if fields[i].Key == contextKey {
			return fields
		}
	}

	if !ent.Caller.Defined {
		return fields
	}

	return append(fields, ErrorReport(ent.Caller.PC, ent.Caller.File, ent.Caller.Line, true))
}

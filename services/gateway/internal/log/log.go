// Package log 提供基于 zap 的结构化日志。
package log

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var global *zap.Logger

// Init 初始化全局日志器。level 支持 debug/info/warn/error；encoding 支持 json/console。
func Init(level, encoding string) error {
	lvl := zapcore.InfoLevel
	_ = lvl.Set(level)

	if encoding == "" {
		encoding = "json"
	}

	cfg := zap.Config{
		Level:            zap.NewAtomicLevelAt(lvl),
		Development:      false,
		Encoding:         encoding,
		EncoderConfig:    encoderConfig(),
		OutputPaths:      []string{"stdout"},
		ErrorOutputPaths: []string{"stderr"},
	}

	l, err := cfg.Build(zap.AddCallerSkip(1))
	if err != nil {
		return err
	}
	global = l
	return nil
}

func encoderConfig() zapcore.EncoderConfig {
	c := zap.NewProductionEncoderConfig()
	c.TimeKey = "ts"
	c.EncodeTime = zapcore.ISO8601TimeEncoder
	c.EncodeLevel = zapcore.LowercaseLevelEncoder
	return c
}

// L 返回全局日志器，未初始化时返回 no-op。
func L() *zap.Logger {
	if global == nil {
		return zap.NewNop()
	}
	return global
}

// Debug 记录 debug 级别日志。
func Debug(msg string, fields ...zap.Field) { L().Debug(msg, fields...) }

// Info 记录 info 级别日志。
func Info(msg string, fields ...zap.Field) { L().Info(msg, fields...) }

// Warn 记录 warn 级别日志。
func Warn(msg string, fields ...zap.Field) { L().Warn(msg, fields...) }

// Error 记录 error 级别日志。
func Error(msg string, fields ...zap.Field) { L().Error(msg, fields...) }

// Sync 刷新缓冲日志。
func Sync() { _ = L().Sync() }

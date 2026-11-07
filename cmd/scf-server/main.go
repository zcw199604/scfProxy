// scf-server 是腾讯云 SCF Web 函数入口。
package main

import (
	"context"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/zcw199604/scfproxy/internal/server"
)

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "9000"
	}
	addr := flag.String("addr", "0.0.0.0:"+port, "监听地址")
	cfg := server.Config{Token: os.Getenv("SCFPROXY_TOKEN"), Logger: slog.Default()}
	flag.BoolVar(&cfg.AllowPrivate, "allow-private", false, "允许私有目标地址（仅限本地测试）")
	flag.DurationVar(&cfg.HandshakeTimeout, "handshake-timeout", 10*time.Second, "握手消息读取超时")
	flag.DurationVar(&cfg.PrewarmTimeout, "prewarm-timeout", 5*time.Minute, "等待预热连接首条消息的超时")
	flag.DurationVar(&cfg.DialTimeout, "dial-timeout", 10*time.Second, "目标拨号超时")
	flag.IntVar(&cfg.MaxMessageSize, "max-message-size", 64*1024, "下行消息最大字节数（不超过256KB）")
	flag.IntVar(&cfg.MaxMessagesPerSecond, "max-messages-per-second", 0, "下行每秒消息数（0不限）")
	flag.IntVar(&cfg.MaxBytesPerSecond, "max-bytes-per-second", 0, "下行每秒最大字节数（0不限）")
	flag.Parse()
	handler, err := server.New(cfg)
	if err != nil {
		slog.Error("启动失败", "error", err)
		os.Exit(1)
	}
	srv := &http.Server{Addr: *addr, Handler: handler, ReadHeaderTimeout: 10 * time.Second}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	shutdownDone := make(chan struct{})
	go func() {
		defer close(shutdownDone)
		<-ctx.Done()
		handler.Close()
		shutdown, done := context.WithTimeout(context.Background(), 10*time.Second)
		defer done()
		srv.Shutdown(shutdown)
	}()
	slog.Info("服务启动", "addr", *addr)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		slog.Error("监听失败", "error", err)
		os.Exit(1)
	}
	<-shutdownDone
}

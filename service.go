package main

import (
	"flag"
	"fmt"
	"github.com/siddontang/go-log/log"
	"gobinlog/src"
	"gobinlog/src/conf"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	flag.Parse()
	s := make(chan os.Signal, 1)
	signal.Notify(s, syscall.SIGUSR1)
	go func() {
		for {
			<-s
			conf.Init()
			log.Println("Reload config")
		}
	}()
	src.BinlogListener()
	fmt.Print("start binlog service\n")
}

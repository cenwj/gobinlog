package main

import (
	"flag"
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
	signal.Notify(s,
		os.Kill,
		os.Interrupt,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	conf.Init()
	r, err := src.InitRiver(&conf.Conf().C)
	if err != nil {
		println(err.Error())
		return
	}

	done := make(chan struct{}, 1)

	go func() {
		err := r.Run()
		if err != nil {
			log.Error("Canal run Err:%v\n", err)
		}
		done <- struct{}{}
	}()

	select {
	case n := <-s:
		r.ShutdownCh <- n
	case <-r.Ctx().Done():
		log.Infof("ctx down %v, closing", r.Ctx().Err())
	}

	r.Close()
	<-done
}

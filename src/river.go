package src

import (
	"context"
	"fmt"
	"github.com/siddontang/go-log/log"
	"github.com/siddontang/go-mysql/canal"
	"gobinlog/src/conf"
	"sync"
)

type River struct {
	canal      *canal.Canal
	c          *conf.C
	syncCh     chan *canal.RowsEvent
	posCh      chan interface{}
	ctx        context.Context
	wg         sync.WaitGroup
	master     *masterInfo
	ShutdownCh chan interface{}
	cancel     context.CancelFunc
}

func InitRiver(c *conf.C) (*River, error) {
	r := new(River)
	r.c = c
	r.syncCh = make(chan *canal.RowsEvent, 100)
	r.posCh = make(chan interface{}, 100)
	r.ShutdownCh = make(chan interface{}, 1)
	r.ctx, r.cancel = context.WithCancel(context.Background())

	var err error
	if r.master, err = loadMasterInfo(c.DataDir); err != nil {
		return nil, err
	}

	if err = r.newCanal(); err != nil {
		return nil, err
	}

	if err = r.canal.CheckBinlogRowImage("FULL"); err != nil {
		return nil, err
	}

	return r, nil
}

func (r *River) newCanal() error {
	cfg := canal.NewDefaultConfig()
	cfg.Addr = fmt.Sprintf("%s:%d", r.c.BinlogDbHost, 3306)
	cfg.User = r.c.BinlogDbUser
	cfg.Password = r.c.BinlogDbPass
	cfg.Flavor = "mysql"
	cfg.Dump.ExecutionPath = ""
	cfg.ServerID = r.c.ServerID
	cfg.Dump.DiscardErr = true
	var err error
	r.canal, err = canal.NewCanal(cfg)
	r.canal.SetEventHandler(&BinLogHandler{r: r})
	return err
}

func (r *River) Run() error {
	r.wg.Add(2)
	go r.SyncPos()
	go r.RowLoop()

	pos := r.master.Position()
	if pos.Name == "" || pos.Pos == 0 {
		pos, _ = r.canal.GetMasterPos()
	}
	err := r.canal.RunFrom(pos)
	if err != nil {
		log.Errorf("run canal Err:%s\n ", err.Error())
		return err
	}
	return nil
}

func (r *River) Ctx() context.Context {
	return r.ctx
}

func (r *River) Close() {
	r.cancel()
	r.canal.Close()
	r.master.Close()
	r.wg.Wait()
	log.Infof("Closed River")
}

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
	*canal.DummyEventHandler
	canal      *canal.Canal
	cEvent     *canal.RowsEvent
	c          *conf.C
	syncCh     chan [][]interface{}
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
	r.syncCh = make(chan [][]interface{}, 100)
	r.posCh = make(chan interface{}, 100)
	r.ShutdownCh = make(chan interface{}, 1)
	r.ctx, r.cancel = context.WithCancel(context.Background())

	var err error
	if r.master, err = loadMasterInfo(c.DataDir); err != nil {
		return nil, err
	}

	//if err = r.canal.CheckBinlogRowImage("FULL"); err != nil {
	//	return nil, err
	//}

	log.Infof("InitRiver:%s\n", r)

	return r, nil
}

func (r *River) GetDefaultCanal() (*canal.Canal, error) {
	cfg := canal.NewDefaultConfig()
	cfg.Addr = fmt.Sprintf("%s:%d", r.c.BinlogDbHost, 3306)
	cfg.User = r.c.BinlogDbUser
	cfg.Password = r.c.BinlogDbPass
	cfg.Flavor = "mysql"
	cfg.Dump.ExecutionPath = ""

	var err error
	c, err := canal.NewCanal(cfg)
	//r.canal.SetEventHandler(&canal.DummyEventHandler{})
	//c.SetEventHandler(&BinLogHandler{})
	//r.canal.SetEventHandler(&EventHandler{r})
	return c, err
}

func (r *River) Run() error {
	r.wg.Add(2)
	go r.syncPos()
	go r.txLoop()

	c, _ := r.GetDefaultCanal()
	//pos := r.master.Position()
	//log.Infof("Run pos:%v\n", pos)
	c.SetEventHandler(&BinLogHandler{r:r})
	pos, _ := c.GetMasterPos()
	err := c.RunFrom(pos)
	log.Infof("run canal Success12:%v,pos:%s\n", err, pos)
	if err != nil {
		log.Errorf("run canal Err:%s\n ", err.Error())
		return err
	}
	log.Infof("run canal Successpos:%s\n", pos)
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
	log.Infof("closed river")
}

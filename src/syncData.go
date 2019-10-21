package src

import (
	"fmt"
	"github.com/siddontang/go-log/log"
	"github.com/siddontang/go-mysql/canal"
	"gobinlog/src/models"
)
type SyncDataToBinlog struct{}

func (h *BinlogParser) SyncCopyToBinlog(e *canal.RowsEvent, i int) {
	copy := models.Copy{}
	h.GetBinLogData(&copy, e, i)
	switch e.Action {
	case canal.UpdateAction:
		oldCopy := models.Copy{}
		h.GetBinLogData(&oldCopy, e, i-1)
		log.Infof("%v %v\n", copy, oldCopy)
		fmt.Printf("Copy %d id changed from %d to %d\n", copy.Id, oldCopy.Id, copy.ToMt4)
	case canal.InsertAction:
		fmt.Printf("Copy %d is created with name %s\n", copy.Id, copy.FromMt4)
	case canal.DeleteAction:
		fmt.Printf("Copy %d is deleted with name %s\n", copy.Id, copy.FromMt4)
	default:
		fmt.Printf("Unknown action")
	}
}

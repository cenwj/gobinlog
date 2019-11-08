package src

import (
	"fmt"
	"github.com/siddontang/go-log/log"
	"github.com/siddontang/go-mysql/canal"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"gobinlog/src/db"
	"strings"
	"time"
)

type posSave struct {
	pos   mysql.Position
	force bool
}

type BinLogHandler struct {
	canal.DummyEventHandler
	r *River
}

func (h *BinLogHandler) OnRotate(e *replication.RotateEvent) error {
	pos := mysql.Position{
		string(e.NextLogName),
		uint32(e.Position),
	}

	h.r.posCh <- posSave{pos, true}
	return h.r.ctx.Err()
}

func (h *BinLogHandler) OnDDL(nextPos mysql.Position, _ *replication.QueryEvent) error {
	h.r.posCh <- posSave{nextPos, true}
	return h.r.ctx.Err()
}

func (h *BinLogHandler) OnXID(nextPos mysql.Position) error {
	h.r.posCh <- posSave{nextPos, true}
	return h.r.ctx.Err()
}

func (h *BinLogHandler) OnPosSynced(p mysql.Position, set mysql.GTIDSet, f bool) error {
	h.r.posCh <- posSave{p, true}
	return h.r.ctx.Err()
}

func (h *BinLogHandler) OnRow(e *canal.RowsEvent) error {
	defer func() {
		if r := recover(); r != nil {
			log.Errorf("recover error - %s\n", r)
		}
	}()

	var mapTB = h.r.c.BinlogTbs
	var fTb = e.Table.Schema + "." + e.Table.Name
	_, ok := mapTB[strings.ToLower(fTb)]
	if !ok {
		return nil
	}

	var res []byte
	if e.Action == canal.InsertAction || e.Action == canal.UpdateAction || e.Action == canal.DeleteAction {
		h.r.syncCh <- e
	} else {
		return nil
	}
	h.r.posCh <- res

	return nil
}

func (h *BinLogHandler) OnTableChanged(schema string, table string) error { return nil }

func (h *BinLogHandler) String() string {
	return "BinLogHandler"
}

func (r *River) SyncPos() {
	bulkSize := r.c.BulkSize
	if bulkSize == 0 {
		bulkSize = 128
	}

	interval := r.c.FlushBulkTime.Duration
	if interval == 0 {
		interval = 200 * time.Millisecond
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	defer r.wg.Done()

	lastSavedTime := time.Now()

	var pos mysql.Position

	for {
		savePos := false
		select {
		case v := <-r.posCh:
			switch v := v.(type) {
			case posSave:
				now := time.Now()
				if v.force || now.Sub(lastSavedTime) > 3*time.Second {
					lastSavedTime = now
					savePos = true
					pos = v.pos
				}
			}
		case <-r.ctx.Done():
			return
		}

		if savePos {
			if err := r.master.Save(pos); err != nil {
				log.Errorf("save position %s err %v, close", pos, err)
				r.cancel()
				return
			}
		}
	}
}

var maxRoutineNum = 10

func (r *River) RowLoop() {
	defer r.wg.Done()

	chHandler := make(chan int, maxRoutineNum)
	for {
		select {
		case v := <-r.syncCh:
			chHandler <- 1
			if strings.ToLower(v.Table.Schema) == r.c.SyncGroupDbName && strings.ToLower(v.Table.Name) == r.c.SyncGroupTbName {
				go r.SyncGroupData(v, chHandler)
			} else {
				go r.SyncData(v, chHandler)
			}
		case <-r.ShutdownCh:
			chLen := len(r.syncCh)
			log.Infof("receive shutdown signal closing, ch %d to write syncCh", chLen)
			if chLen > 0 {
				for i := 0; i < chLen; i++ {
					v := <-r.syncCh
					chHandler <- 1
					if strings.ToLower(v.Table.Schema) == r.c.SyncGroupDbName && strings.ToLower(v.Table.Name) == r.c.SyncGroupTbName {
						go r.SyncGroupData(v, chHandler)
					} else {
						go r.SyncData(v, chHandler)
					}
				}
			}

			return
		}
	}
}

func (r *River) SyncGroupData(v *canal.RowsEvent, chHandler chan int) {
	switch v.Action {
	case "update":
		r.UpdateGroupSql(v, chHandler)
	case "insert":
		r.InsetGroupSql(v, chHandler)
	default:
		log.Info("group table action not in update,insert")
	}
}

func (r *River) SyncData(v *canal.RowsEvent, chHandler chan int) {
	switch v.Action {
	case "update":
		r.UpdateSql(v, chHandler)
	case "insert":
		r.InsetSql(v, chHandler)
	case "delete":
		r.DeleteSql(v, chHandler)
	default:
		log.Fatal("table action err")
	}
}

func (r *River) DeleteSql(e *canal.RowsEvent, chHandler chan int) {
	for i := 0; i < len(e.Rows); i++ {
		pv, _ := e.Table.GetPKValues(e.Rows[i])
		pkLen := e.Table.PKColumns

		var where = ""
		for j := 0; j < len(pkLen); j++ {
			pk := e.Table.GetPKColumn(j).Name
			if where != "" {
				where += " and "
			}
			s := ToStrings(pv[j])
			where += "`" + pk + "`" + "=" + "'" + s + "'"
		}

		sql := "DELETE FROM " + r.c.DbName + "." + e.Table.Name + " WHERE " + where
		QuerySql(sql)
	}

	<-chHandler
}

func (r *River) UpdateGroupSql(e *canal.RowsEvent, chHandler chan int) {
	var n = len(e.Rows)
	for i := 0; i < len(e.Rows); i++ {
		if i%2 != 0 {
			continue
		}

		if n == i+1 {
			break
		}

		pkValue, _ := e.Table.GetPKValues(e.Rows[i+1])
		var where = ""
		if len(pkValue) == 1 {
			where += "`trade_account_id`" + "=" + ToStrings(pkValue[0])
		}

		var sets = ""
		for _, v := range e.Table.Columns {
			if strings.ToUpper(v.Name) != "GROUP" {
				continue
			}

			oldStr, _ := e.Table.GetColumnValue(v.Name, e.Rows[i])
			str, _ := e.Table.GetColumnValue(v.Name, e.Rows[i+1])
			if ToStrings(oldStr) == ToStrings(str) {
				continue
			}

			s := ToStrings(str)
			sets += "`" + "group" + "`" + "=" + "'" + s + "'"
		}

		if sets != "" && where != "" {
			sql := "UPDATE " + r.c.DbGroupName + "." + r.c.GroupTbName + " SET " + sets + " WHERE " + where
			QueryGroupSql(sql)
		}
	}
	<-chHandler
}

func (r *River) InsetGroupSql(e *canal.RowsEvent, chHandler chan int) {
	for i := 0; i < len(e.Rows); i++ {
		var sets = ""
		var where = ""
		pkValue, err := e.Table.GetPKValues(e.Rows[i])
		if err != nil {
			log.Errorf("InsetGroupSql GetPKValues rows:%v, err:%s\n", e.Rows, err.Error())
			break
		}

		if len(pkValue) == 1 {
			where += "`trade_account_id`" + "=" + ToStrings(pkValue[0])
		}

		for _, v := range e.Table.Columns {
			if strings.ToUpper(v.Name) != "GROUP" {
				continue
			}

			str, err := e.Table.GetColumnValue(v.Name, e.Rows[i])
			if err != nil {
				log.Errorf("InsetGroupSql GetColumnValue rows:%v, err:%s\n", e.Rows, err.Error())
				break
			}

			s := ToStrings(str)
			sets += "`" + "group" + "`" + "=" + "'" + s + "'"
		}
		if sets != "" && where != "" {
			sql := "UPDATE " + r.c.DbGroupName + "." + r.c.GroupTbName + " SET " + sets + " WHERE " + where
			QueryGroupSql(sql)
		}
	}
	<-chHandler
}

func (r *River) UpdateSql(e *canal.RowsEvent, chHandler chan int) {
	var n = len(e.Rows)
	for i := 0; i < len(e.Rows); i++ {
		if i%2 != 0 {
			continue
		}

		if n == i+1 {
			break
		}

		pkValue, err := e.Table.GetPKValues(e.Rows[i+1])
		if err != nil {
			log.Errorf("UpdateSql GetPKValues rows:%v, err %s\n", e.Rows, err.Error())
			continue
		}

		pkLen := e.Table.PKColumns

		var where = ""
		var mr = make(map[string]string)
		var ret = make([]map[string]string, 0)
		for j := 0; j < len(pkLen); j++ {
			pk := e.Table.GetPKColumn(j).Name
			if where != "" {
				where += " and "
			}

			v := ToStrings(pkValue[j])
			mr[pk] = pk
			ret = append(ret, mr)
			where += "`" + pk + "`" + "=" + "'" + v + "'"
		}

		var sets = ""
		for _, v := range e.Table.Columns {
			_, ok := mr[v.Name]
			if ok {
				continue
			}
			var err error
			oldStr, err := e.Table.GetColumnValue(v.Name, e.Rows[i])
			str, err := e.Table.GetColumnValue(v.Name, e.Rows[i+1])
			if err != nil {
				log.Errorf("UpdateSql GetColumnValue rows:%v, err %s\n", e.Rows, err.Error())
				continue
			}

			if ToStrings(oldStr) == ToStrings(str) {
				continue
			}

			if sets != "" {
				sets += ","
			}

			var s = ""
			if str == nil {
				s = "NULL"
				sets += "`" + v.Name + "`" + "=" + s
			} else {
				if v.RawType == "json" {
					s = fmt.Sprintf("%s", str)
				} else {
					s = ToStrings(str)
				}
				sets += "`" + v.Name + "`" + "=" + "'" + s + "'"
			}
		}
		sql := "UPDATE " + r.c.DbName + "." + e.Table.Name + " SET " + sets + " WHERE " + where
		QuerySql(sql)
	}
	<-chHandler
}

func (r *River) InsetSql(e *canal.RowsEvent, chHandler chan int) {
	for i := 0; i < len(e.Rows); i++ {
		var fields = ""
		var values = ""
		for _, v := range e.Table.Columns {
			str, err := e.Table.GetColumnValue(v.Name, e.Rows[i])
			if err != nil {
				log.Errorf("InsetSql GetColumnValue rows:%v, err:%s\n", e.Rows, err.Error())
				continue
			}

			if fields != "" {
				fields += ","
			}

			fields += "`" + v.Name + "`"
			if values != "" {
				values += ","
			}

			var s = ""
			if str == nil {
				s = "NULL"
				values += s
			} else {
				if v.RawType == "json" {
					s = fmt.Sprintf("%s", str)
				} else {
					s = ToStrings(str)
				}
				values += "'" + s + "'"
			}
		}

		sql := "INSERT INTO " + r.c.DbName + "." + e.Table.Name + " (" + fields + ")" + " VALUES " + "(" + values + ")"
		QuerySql(sql)
	}
	<-chHandler
}

func QueryGroupSql(sql string) {
	q := db.Group()
	r, err := q.Exec(sql)
	if err == nil {
		res, _ := r.RowsAffected()
		log.Infof("QueryGroupSql Succ:%s, res:%d\n", sql, res)
	} else {
		log.Infof("QueryGroupSql Fail:%s, res:%d\n", sql, 0)
	}
	q.Close()
}

func QuerySql(sql string) {
	q := db.Init()
	r, err := q.Exec(sql)
	if err == nil {
		res, _ := r.RowsAffected()
		log.Infof("querySql Succ:%s, res:%d\n", sql, res)
	} else {
		log.Infof("querySql Fail:%s, res:%d\n", sql, 0)
	}
	q.Close()
}

func ToStrings(args interface{}) string {
	return fmt.Sprintf("%v", args)
}

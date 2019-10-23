package src

import (
	"fmt"
	"github.com/siddontang/go-log/log"
	"github.com/siddontang/go-mysql/canal"
	"gobinlog/src/conf"
	"gobinlog/src/db"
	"runtime/debug"
)

type BinlogHandler struct {
	canal.DummyEventHandler
}

func (h *BinlogHandler) OnRow(e *canal.RowsEvent) error {
	defer func() {
		if r := recover(); r != nil {
			fmt.Print(r, " ", string(debug.Stack()))
		}
	}()

	var mapTB = conf.Config().Database.BinlogTbs
	var fTb = e.Table.Schema + "." + e.Table.Name
	_, ok := mapTB[fTb]
	if !ok {
		return nil
	}
	switch e.Action {
	case canal.UpdateAction:
		UpdateSql(e)
	case canal.InsertAction:
		InsetSql(e)
	case canal.DeleteAction:
		DeleteSql(e)
	default:
		fmt.Printf("Unknown action")
	}

	return nil
}

func DeleteSql(e *canal.RowsEvent) {
	pv, _ := e.Table.GetPKValues(e.Rows[0])
	pkLen := e.Table.PKColumns
	var where = ""
	for i := 0; i < len(pkLen); i++ {
		pk := e.Table.GetPKColumn(i).Name
		if where != "" {
			where += " and "
		}
		var r []interface{} = make([]interface{}, 1)
		r[0] = pv[i]
		s := ToStrings(r)
		where += "`" + pk + "`" + "=" + "'" + s + "'"
	}
	sql := "DELETE FROM " + conf.Config().Database.DbName + "." + e.Table.Name + " WHERE " + where
	QuerySql(sql)
}

func UpdateSql(e *canal.RowsEvent) {
	pv, _ := e.Table.GetPKValues(e.Rows[1])
	pkLen := e.Table.PKColumns
	var where = ""
	var mr = make(map[string]string)
	var ret = make([]map[string]string, 0)
	for i := 0; i < len(pkLen); i++ {
		pk := e.Table.GetPKColumn(i).Name
		if where != "" {
			where += " and "
		}
		var r []interface{} = make([]interface{}, 1)
		r[0] = pv[i]
		s := InterfaceToStrings(r)
		mr[pk] = pk
		ret = append(ret, mr)
		where += "`" + pk + "`" + "=" + "'" + string(s[0]) + "'"
	}

	var sets = ""
	for _, v := range e.Table.Columns {
		_, ok := mr[v.Name]
		if ok {
			continue
		}
		oldStr, _ := e.Table.GetColumnValue(v.Name, e.Rows[0])
		str, _ := e.Table.GetColumnValue(v.Name, e.Rows[1])
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
			if string(v.RawType) == "json" {
				s = fmt.Sprintf("%s", str)
			} else {
				s = ToStrings(str)
			}
			sets += "`" + v.Name + "`" + "=" + "'" + s + "'"
		}

	}
	sql := "UPDATE " + conf.Config().Database.DbName + "." + e.Table.Name + " SET " + sets + " WHERE " + where
	QuerySql(sql)
}

func InsetSql(e *canal.RowsEvent) {
	var fields = ""
	var values = ""
	for _, v := range e.Table.Columns {
		str, _ := e.Table.GetColumnValue(v.Name, e.Rows[0])
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
		}else {
			if string(v.RawType) == "json" {
				s = fmt.Sprintf("%s", str)
			} else {
				s = ToStrings(str)
			}
			values += "'" + s + "'"
		}
	}
	sql := "INSERT INTO " + conf.Config().Database.DbName + "." + e.Table.Name + " (" + fields + ")" + " VALUES " + "(" + values + ")"
	QuerySql(sql)
}
func ToStrings(args interface{}) string {
	return fmt.Sprintf("%v", args)
}

func InterfaceToStrings(args []interface{}) []string {
	sArgs := []string{}
	for _, arg := range args {
		sArgs = append(sArgs, fmt.Sprintf("%v", arg))
	}
	return sArgs
}

func (h *BinlogHandler) String() string {
	return "BinlogHandler"
}

func (h *BinlogHandler) OnTableChanged(schema string, table string) error {
	log.Println("OnTableChanged")
	return nil
}

func BinlogListener() {
	c, err := GetDefaultCanal()
	if err != nil {
		log.Error("GetDefaultCanal Error:", err.Error())
	}
	coords, err := c.GetMasterPos()
	if err != nil {
		fmt.Println("%s\n", err.Error())
	}
	c.SetEventHandler(&BinlogHandler{})
	err = c.RunFrom(coords)
	if err != nil {
		log.Error("BinlogListener Error:", err.Error())
	}
}

func GetDefaultCanal() (*canal.Canal, error) {
	cfg := canal.NewDefaultConfig()
	cfg.Addr = fmt.Sprintf("%s:%d", conf.Config().Database.BinlogDbHost, 3306)
	cfg.User = conf.Config().Database.BinlogDbUser
	cfg.Password = conf.Config().Database.BinlogDbPass
	cfg.Flavor = "mysql"
	cfg.Dump.ExecutionPath = ""

	c, err := canal.NewCanal(cfg)
	if err != nil {
		log.Errorln("GetDefaultCanal:%s\n", err.Error())
	}
	return c, nil
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

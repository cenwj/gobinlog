package src

import (
	"bytes"
	"fmt"
	"os"
	"path"
	"sync"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/juju/errors"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go/ioutil2"
	log "github.com/sirupsen/logrus"
)

type masterInfo struct {
	sync.RWMutex

	Name string
	Pos  uint32

	filePath     string
	lastSaveTime time.Time
}

func loadMasterInfo(dataDir string) (*masterInfo, error) {
	var m masterInfo

	if len(dataDir) == 0 {
		return &m, nil
	}

	m.filePath = path.Join(dataDir, "master.info")
	m.lastSaveTime = time.Now()

	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, errors.Trace(err)
	}

	f, err := os.Open(m.filePath)
	if err != nil && !os.IsNotExist(errors.Cause(err)) {
		return nil, errors.Trace(err)
	} else if os.IsNotExist(errors.Cause(err)) {
		return &m, nil
	}
	defer f.Close()

	_, err = toml.DecodeReader(f, &m)
	return &m, errors.Trace(err)
}

func GetNewestPos() (*masterInfo, error) {
	var m masterInfo
	r := new(River)
	result, err := r.canal.Execute("SHOW MASTER STATUS")
	if err != nil {
		return nil, fmt.Errorf("show master status error - %s", err)
	}

	if result.Resultset.RowNumber() != 1 {
		return nil, errors.New("select master info error")
	}

	binlogName, _ := result.GetStringByName(0, "File")
	binlogPos, _ := result.GetIntByName(0, "Position")
	m.Name = binlogName
	m.Pos = uint32(binlogPos)
	return &m, nil
	//return &mysql.Position{binlogName, uint32(binlogPos)}, nil
}

func (m *masterInfo) Save(pos mysql.Position) error {
	log.Infof("save table position %s", pos)

	m.Lock()
	defer m.Unlock()

	m.Name = pos.Name
	m.Pos = pos.Pos

	if len(m.filePath) == 0 {
		return nil
	}

	n := time.Now()
	if n.Sub(m.lastSaveTime) < time.Second {
		return nil
	}

	m.lastSaveTime = n
	var buf bytes.Buffer
	e := toml.NewEncoder(&buf)

	e.Encode(m)

	var err error
	if err = ioutil2.WriteFileAtomic(m.filePath, buf.Bytes(), 0644); err != nil {
		log.Error("save master file%s err %v", m.filePath, err)
	}

	return errors.Trace(err)
}

func (m *masterInfo) Position() mysql.Position {
	m.RLock()
	defer m.RUnlock()

	return mysql.Position{
		m.Name,
		m.Pos,
	}
}

func (m *masterInfo) Close() error {
	pos := m.Position()

	return m.Save(pos)
}

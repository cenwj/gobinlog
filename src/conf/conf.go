package conf

import (
	"flag"
	"github.com/BurntSushi/toml"
	"sync"
)

type tomlConfig struct {
	Database database
}

type database struct {
	DbHost       string
	DbPort       string
	DbUser       string
	DbPass       string
	DbName       string
	DbType       string
	BinlogDbHost string
	BinlogDbUser string
	BinlogDbPass string
	BinlogTbs    map[string]string
	BinlogDbs    []string
}

//读取环境变量
var conf *tomlConfig
var once sync.Once
var confLock = new(sync.RWMutex)
var confPath string

/** 平滑的替换掉配置，不需要重启项目,起一个协程，监视我们定义好的信号，
如果接收到信号就重新加载配置。
在Config方法中获取Conf的时候加了读锁，防止在读的时候，也在写入，导致配置错乱。
*/
func Config() *tomlConfig {
	once.Do(Init)
	confLock.RLock()
	defer confLock.RUnlock()
	return conf
}

func init(){
	flag.StringVar(&confPath, "conf", "./src/conf/conf_test.toml", "default config path")
}

func Init() {

	//filePath, err := filepath.Abs("./src/conf/conf.toml")
	//if err != nil {
	//	panic(err)
	//}

	config := new(tomlConfig)
	if _, err := toml.DecodeFile(confPath, config); err != nil {
		panic(err)
	}

	confLock.Lock()
	defer confLock.Unlock()
	conf = config
}


package src

import (
	"fmt"
)


func Start() {
	go BinlogListener()

	//time.Sleep(2 * time.Minute)
	fmt.Print("start binlog service")
}
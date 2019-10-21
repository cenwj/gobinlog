package db

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"gobinlog/src/conf"
	"log"
)

var db *sql.DB

func Init() *sql.DB {
	confStr := fmt.Sprintf(
		"%s:%s@tcp(%s:%s)/%s?charset=utf8&parseTime=True&loc=Local",
		conf.Config().Database.DbUser,
		conf.Config().Database.DbPass,
		conf.Config().Database.DbHost,
		conf.Config().Database.DbPort,
		conf.Config().Database.DbName)
	db, err := sql.Open("mysql", confStr)

	if err != nil {
		log.Fatalln("db:" + err.Error())
	}
	fmt.Println(db)
	return db
}

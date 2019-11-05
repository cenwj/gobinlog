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
		conf.Conf().C.DbUser,
		conf.Conf().C.DbPass,
		conf.Conf().C.DbHost,
		conf.Conf().C.DbPort,
		conf.Conf().C.DbName)
	db, err := sql.Open("mysql", confStr)

	if err != nil {
		log.Fatalln("db:" + err.Error())
	}
	return db
}

func Group() * sql.DB  {
	confStr := fmt.Sprintf(
		"%s:%s@tcp(%s:%s)/%s?charset=utf8&parseTime=True&loc=Local",
		conf.Conf().C.DbGroupUser,
		conf.Conf().C.DbGroupPass,
		conf.Conf().C.DbGroupHost,
		conf.Conf().C.DbGroupPort,
		conf.Conf().C.DbGroupName)
	db, err := sql.Open("mysql", confStr)

	if err != nil {
		log.Fatalln("db:" + err.Error())
	}
	return db
}

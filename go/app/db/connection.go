package db

import (
	"database/sql"
	"fmt"
	"github.com/BurntSushi/toml"
	"go-practice/app/error"
	"os"
	"path/filepath"
)

type Config struct {
	Host   string `toml:"host"`
	Port   string `toml:"port"`
	User   string `toml:"user"`
	Pass   string `toml:"pass"`
	DbName string `toml:"db_name"`
}

func ConnectToDB() *sql.DB {
	// 設定ファイルを開く
	currentDir, err := os.Getwd()
	error.ErrorAndExit(err)
	env := os.Getenv("ENVIRONMENT")
	path := filepath.Join(currentDir, "/db_"+env+".tml")

	// 設定ファイルをパースしてDSNを指定する
	var config Config
	_, err = toml.DecodeFile(path, &config)
	error.ErrorAndExit(err)
	dsn := fmt.Sprintf(
		"%s:%s@tcp(%s:%s)/%s",
		config.User,
		config.Pass,
		config.Host,
		config.Port,
		config.DbName)

	// DB接続
	db, err := sql.Open("mysql", dsn)
	error.ErrorAndExit(err)

	return db
}

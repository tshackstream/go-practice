package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"github.com/gocarina/gocsv"
	"go-practice/app"
	"go-practice/app/error"
	"io"
	"log"
	"os"
	"path/filepath"
)

func main() {
	// コマンドラインオプションによって処理を分ける
	action := flag.String("a", "", "")
	// 並行処理オプションがあったら並行処理
	// Goは並行処理(Concurrent)らしいが、Parallelの方がなじみがあるのでオプションpにする
	isConcurrent := flag.Bool("p", false, "")
	// 一度に処理する件数
	bulkNum := flag.Int("b", 10000, "")
	flag.Parse()

	// ログファイルの設定
	currentDir, err := os.Getwd()
	error.ErrorAndExit(err)
	path := filepath.Join(currentDir, "../data/logs.log")
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0775)
	error.ErrorAndExit(err)
	log.SetOutput(io.MultiWriter(file, os.Stdout))

	switch *action {
	// アップロード
	case "upload":
		file := app.OpenUploadFile()
		defer file.Close()
		reader := csv.NewReader(file)

		// 各行とカラムの構造体を紐づけた形で読み込む
		cols := app.CsvColumns{}
		goCsvReader, err := gocsv.NewUnmarshaller(reader, cols)
		error.ErrorAndExit(err)

		var execSucceeded bool

		if *isConcurrent {
			execSucceeded = app.UploadConcurrently(goCsvReader, *bulkNum)
		} else {
			execSucceeded = app.Upload(goCsvReader, *bulkNum)
		}

		if execSucceeded {
			fmt.Println("upload succeeded")
		} else {
			fmt.Println("upload failed")
		}
	// DBからCSVを作成
	case "make_csv":
		if *isConcurrent {
			app.MakeCSVConcurrently(*bulkNum)
		} else {
			app.MakeCSV(*bulkNum)
		}
		log.Print("make csv finished")
	default:
		fmt.Println("不正なアクションです")
	}
}

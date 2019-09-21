package main

import (
	"flag"
	"fmt"
	"go-practice/app"
	"runtime"
)

func main() {
	var mem runtime.MemStats

	runtime.ReadMemStats(&mem)
	fmt.Println(mem.Alloc, mem.TotalAlloc, mem.HeapAlloc, mem.HeapSys)

	// コマンドライン引数を受け取る
	flag.Parse()

	// コマンドライン引数によって処理を分ける
	switch flag.Arg(0) {
	// アップロード
	case "upload":
		// TODO 並列処理パラメータがあったら並列処理
		succeeded, errorMessages := app.Upload()
		if !succeeded {
			for row, message := range errorMessages {
				fmt.Println(string(row) + "行目: " + message)
			}
		}
	// DBからCSVを作成
	case "make_csv":
		app.MakeCSV()
	}

	runtime.ReadMemStats(&mem)
	fmt.Println(mem.Alloc, mem.TotalAlloc, mem.HeapAlloc, mem.HeapSys)
}

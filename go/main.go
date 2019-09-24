package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"go-practice/app"
	"math"
	"os"
	"sync"
)

func main() {
	// コマンドラインオプション「によって処理を分ける
	action := flag.String("a", "", "")
	isConcurrent := flag.Bool("p", false, "")
	flag.Parse()

	switch *action {
	// アップロード
	case "upload":
		// TODO 並行処理パラメータがあったら並列処理
		succeeded, errorMessages := app.Upload()
		if !succeeded {
			for row, message := range errorMessages {
				fmt.Println(string(row) + "行目: " + message)
			}
		}
	// DBからCSVを作成
	case "make_csv":
		// 並行処理パラメータがあったら並行処理
		// Goは並行処理(Concurrent)のようだが、Parallelの方がなじみがあるのでオプションpにする
		if *isConcurrent {
			totalCount := app.GetTotalCount()
			if totalCount == 0 {
				fmt.Println("データがありません")
				os.Exit(0)
			}
			limit := 10000
			chunk := int(math.Ceil(float64(totalCount / limit)))
			wg := sync.WaitGroup{}
			for i := 0; i <= chunk; i++ {
				wg.Add(1)
				offset := i * limit
				go func() {
					file := app.OpenNewFile("addresses_from_db_go_concurrent.csv")
					defer file.Close()
					app.MakeCSV(csv.NewWriter(file), app.SetLimitOffset(&limit, &offset))
					wg.Done()
				}()
			}
			wg.Wait()
			fmt.Println("end")
		} else {
			file := app.OpenNewFile("addresses_from_db_go.csv")
			defer file.Close()
			app.MakeCSV(csv.NewWriter(file))
		}
	default:
		fmt.Println("不正なアクションです")
	}
}

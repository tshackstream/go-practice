package app

import (
	"encoding/csv"
	"fmt"
	"github.com/gocarina/gocsv"
	goPracticeDb "go-practice/app/db"
	"go-practice/app/error"
	"math"
	"os"
	"path/filepath"
	"sync"
)

// writeContent関数のオプション引数を設定
// nullableにしたいのでポインタ
type selectOptions struct {
	limit  *int
	offset *int
}

type SelectOption func(*selectOptions)

// オプション引数に値を設定する関数
func SetLimitOffset(limit *int, offset *int) SelectOption {
	return func(opt *selectOptions) {
		opt.limit = limit
		opt.offset = offset
	}
}

// 新規CSVファイルを開く
func OpenNewFile(fileName string) *os.File {
	currentDir, err := os.Getwd()
	file, err := os.OpenFile(filepath.Join(currentDir, "../data/"+fileName), os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0775)
	error.ErrorAndExit(err)
	return file
}

// データ総数を取得
func GetTotalCount() int {
	var count int
	db := goPracticeDb.ConnectToDB()
	defer db.Close()
	err := db.QueryRow("SELECT count(*) FROM addresses").Scan(&count)
	error.ErrorAndExit(err)

	return count
}

// CSV作成
func MakeCSV(writer *csv.Writer, limitOffset ...SelectOption) {
	// 引数limitOffsetのデフォルト値
	selectOptions := selectOptions{}
	for _, opt := range limitOffset {
		opt(&selectOptions)
	}

	goCSVWriter := gocsv.NewSafeCSVWriter(writer)

	// 書き込み関数
	writeContent(goCSVWriter, selectOptions)
}

// 並行処理で作成
func MakeCSVConcurrently() {
	totalCount := GetTotalCount()
	if totalCount == 0 {
		fmt.Println("データがありません")
		os.Exit(0)
	}
	limit := 10000
	chunk := int(math.Ceil(float64(totalCount / limit)))
	wg := sync.WaitGroup{}
	// goroutineの数を制限する
	ch := make(chan int, 2)
	for i := 0; i <= chunk; i++ {
		ch <- 1
		wg.Add(1)
		offset := i * limit
		go func() {
			file := OpenNewFile("addresses_from_db_go_concurrent.csv")
			defer file.Close()
			MakeCSV(csv.NewWriter(file), SetLimitOffset(&limit, &offset))
			<-ch
			wg.Done()
		}()
	}
	wg.Wait()
}

// データ行書き込み
func writeContent(writer *gocsv.SafeCSVWriter, limitOffset selectOptions) {
	// DB接続
	db := goPracticeDb.ConnectToDB()
	defer db.Close()

	// DBからデータ取得しつつ、CSV書き出し
	sql := "SELECT * FROM addresses"
	if limitOffset.limit != nil && limitOffset.offset != nil {
		sql = sql + fmt.Sprintf(" LIMIT %d OFFSET %d", *limitOffset.limit, *limitOffset.offset)
	}
	query, err := db.Query(sql)
	error.ErrorAndExit(err)
	defer query.Close()

	// CSVに書き込むデータ
	var csvRow []*AddressesCols

	// 1行ずつ書き出し
	for query.Next() {
		row := AddressesCols{}
		err := query.Scan(
			&row.TodofukenCode,
			&row.ShikuchosonCode,
			&row.OoazaCode,
			&row.ChomeCode,
			&row.TodofukenName,
			&row.ShikuchosonName,
			&row.OoazachomeName,
			&row.Lat,
			&row.Lon)
		error.ErrorAndExit(err)
		csvRow = append(csvRow, &row)

		// 5000件ごとに書き出し
		if len(csvRow) == 5000 {
			err = gocsv.MarshalCSVWithoutHeaders(csvRow, writer)
			error.ErrorAndExit(err)
			csvRow = nil
		}
	}
}

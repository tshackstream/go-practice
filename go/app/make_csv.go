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

// CSV作成
func MakeCSV(limit int) {
	fileName := "addresses_from_db_go.csv"
	WriteHeader(fileName)
	totalCount := GetTotalCount()
	if totalCount == 0 {
		fmt.Println("データがありません")
		os.Exit(0)
	}

	bulkNum := int(math.Ceil(float64(totalCount / limit)))

	file := OpenNewFile(fileName)
	defer file.Close()
	goCSVWriter := gocsv.NewSafeCSVWriter(csv.NewWriter(file))

	for i := 0; i <= bulkNum; i++ {
		offset := i * limit
		// 書き込み関数
		WriteContent(goCSVWriter, limit, offset)
	}

}

// 並行処理で作成
func MakeCSVConcurrently(limit int) {
	fileName := "addresses_from_db_go_concurrent.csv"

	WriteHeader(fileName)

	totalCount := GetTotalCount()
	if totalCount == 0 {
		fmt.Println("データがありません")
		os.Exit(0)
	}

	bulkNum := int(math.Ceil(float64(totalCount / limit)))
	wg := sync.WaitGroup{}
	// goroutineの数を制限する
	ch := make(chan int, 2)

	for i := 0; i <= bulkNum; i++ {
		ch <- 1
		wg.Add(1)
		offset := i * limit
		go func() {
			file := OpenNewFile(fileName)
			defer file.Close()
			goCSVWriter := gocsv.NewSafeCSVWriter(csv.NewWriter(file))
			// 書き込み関数
			WriteContent(goCSVWriter, limit, offset)
			<-ch
			wg.Done()
		}()
	}
	wg.Wait()
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

// ヘッダ行書き込み
func WriteHeader(fileName string) {
	file := OpenNewFile(fileName)
	defer file.Close()
	writer := csv.NewWriter(file)
	// 横着してべた書きしたが本来はうまいことやりたい
	err := writer.Write(
		[]string{
			"todofuken_code",
			"shikuchoson_code",
			"ooaza_code",
			"chome_code",
			"todofuken_name",
			"shikuchoson_name",
			"ooazachome_name",
			"lat",
			"lon",
			"newdata_flag"})
	error.ErrorAndExit(err)
	writer.Flush()
}

// データ行書き込み
func WriteContent(writer *gocsv.SafeCSVWriter, limit int, offset int) {
	// DB接続
	db := goPracticeDb.ConnectToDB()
	defer db.Close()

	// DBからデータ取得しつつ、CSV書き出し
	sql := "SELECT * FROM addresses LIMIT %d OFFSET %d"
	sql = fmt.Sprintf(sql, limit, offset)

	query, err := db.Query(sql)
	error.ErrorAndExit(err)
	defer query.Close()

	// CSVに書き込むデータ
	var csvRow []*CsvColumns

	// 1行ずつ書き出し
	for query.Next() {
		row := CsvColumns{}
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
		row.NewDataFlag = "0"
		csvRow = append(csvRow, &row)
	}

	err = gocsv.MarshalCSVWithoutHeaders(csvRow, writer)
	error.ErrorAndExit(err)
}

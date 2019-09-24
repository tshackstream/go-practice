package app

import (
	"encoding/csv"
	"fmt"
	"github.com/gocarina/gocsv"
	goPracticeDb "go-practice/app/db"
	"go-practice/app/error"
	"os"
	"path/filepath"
)

// DBカラムの構造体
type AddressesCol struct {
	TodofukenCode   string `csv:"todofuken_code"`
	ShikuchosonCode string `csv:"shikuchoson_code"`
	OoazaCode       string `csv:"ooaza_code"`
	ChomeCode       string `csv:"chome_code"`
	TodofukenName   string `csv:"todofuken_name"`
	ShikuchosonName string `csv:"shikuchoson_name"`
	OoazachomeName  string `csv:"ooazachome_name"`
	Lat             string `csv:"lat"`
	Lon             string `csv:"lon"`
}

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

func OpenNewFile(fileName string) *os.File {
	// 新規CSVファイルを開く
	currentDir, err := os.Getwd()
	file, err := os.OpenFile(filepath.Join(currentDir, "../data/"+fileName), os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0775)
	error.ErrorAndExit(err)
	return file
}

func GetTotalCount() int {
	// データ総数を取得
	var count int
	db := goPracticeDb.ConnectToDB()
	defer db.Close()
	err := db.QueryRow("SELECT count(*) FROM addresses").Scan(&count)
	error.ErrorAndExit(err)

	return count
}

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
	var csvRow []*AddressesCol

	// 1行ずつ書き出し
	for query.Next() {
		row := AddressesCol{}
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

package app

import (
	"database/sql"
	"encoding/csv"
	"github.com/gocarina/gocsv"
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

func MakeCSV() {
	// 新規CSVファイルを開く
	currentDir, err := os.Getwd()
	file, err := os.OpenFile(filepath.Join(currentDir, "../data/addresses_from_db_go.csv"), os.O_WRONLY|os.O_CREATE, 0775)
	error.ErrorAndExit(err)
	defer file.Close()
	writer := csv.NewWriter(file)
	goCSVWriter := gocsv.NewSafeCSVWriter(writer)

	// 書き込み関数
	writeFile(goCSVWriter)
}

// ファイル書き込み
func writeFile(writer *gocsv.SafeCSVWriter) {
	// DB接続
	db, err := sql.Open("mysql", "root:root@tcp(127.0.0.1:13306)/go_practice")
	defer db.Close()
	error.ErrorAndExit(err)

	// DBからデータ取得しつつ、CSV書き出し
	query, err := db.Query("SELECT * FROM addresses")
	error.ErrorAndExit(err)
	defer query.Close()

	// CSVに書き込むデータ
	var csvRow []*AddressesCol

	// 書き込み回数
	i := 1
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

		// 1万行読み込んだらCSVに書き込み
		if len(csvRow) == 10000 {
			// 1回目の書き込みだけヘッダ行付き
			if i == 1 {
				err := gocsv.MarshalCSV(csvRow, writer)
				error.ErrorAndExit(err)
			} else {
				err := gocsv.MarshalCSVWithoutHeaders(csvRow, writer)
				error.ErrorAndExit(err)
			}
			csvRow = nil
			i += 1
		}
	}
}

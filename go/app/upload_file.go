package app

import (
	"encoding/csv"
	"errors"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/gocarina/gocsv"
	goPracticeDb "go-practice/app/db"
	"go-practice/app/error"
	"io"
	"log"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// CSVファイルを開く
func OpenUploadFile() *os.File {
	currentDir, err := os.Getwd()
	error.ErrorAndExit(err)
	path := filepath.Join(currentDir, "../data/addresses.csv")
	file, err := os.Open(path)
	error.ErrorAndExit(err)

	return file
}

// アップロード
func Upload(reader *gocsv.Unmarshaller, bulkNum int) bool {
	execSucceeded := true
	for {
		insertValues, checkConditions, isLast := ReadLines(reader, bulkNum)
		succeeded, errorMessages := CheckAndImport(insertValues, checkConditions)
		if !succeeded {
			log.Println(strings.Join(errorMessages, "\n"))
			execSucceeded = false
		}

		if isLast {
			break
		}
	}

	return execSucceeded
}

// 並行処理でアップロード
func UploadConcurrently(reader *gocsv.Unmarshaller, bulkNum int) bool {
	execSucceeded := true

	// ファイルの行数を取得
	rowCountFile := OpenUploadFile()
	rowCountReader := csv.NewReader(rowCountFile)
	lineNum := 0
	for {
		_, err := rowCountReader.Read()
		if err == io.EOF {
			break
		}
		error.ErrorAndExit(err)
		lineNum++
	}

	// 1回に処理する量で分ける
	routineNum := int(math.Ceil(float64(lineNum / bulkNum)))

	// 同時に行うgoroutineの上限
	goroutineNumCh := make(chan int, 2)

	wg := sync.WaitGroup{}
	m := sync.Mutex{}

	// 処理する分だけgoroutineを作成
	for i := 0; i < routineNum; i++ {
		goroutineNumCh <- 1
		wg.Add(1)
		go func() {
			m.Lock()
			insertValues, checkConditions, _ := ReadLines(reader, bulkNum)
			m.Unlock()
			succeeded, errorMessages := CheckAndImport(insertValues, checkConditions)
			if !succeeded {
				log.Println(strings.Join(errorMessages, "\n"))
				execSucceeded = false
			}

			<-goroutineNumCh
			wg.Done()
		}()
	}
	wg.Wait()

	return execSucceeded
}

// 指定した行数読み込む
func ReadLines(reader *gocsv.Unmarshaller, bulkNum int) ([]string, []string, bool) {
	// WHERE句とVALUESのフォーマット
	// 今回はベタで書いたが、自動で生成しても良いかも
	conditionFormat := "('%s', '%s', '%s', '%s')"
	valuesFormat := "('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')"

	// 指定された行数分各行を読み込みつつ、UPSERT用のVALUES,重複チェック用のWHERE句を準備する
	var insertValues []string
	var checkConditions []string
	for i := 0; i < bulkNum; i++ {
		row, err := reader.Read()
		if err == io.EOF {
			return insertValues, checkConditions, true
		}
		error.ErrorAndExit(err)

		datum, ok := row.(CsvColumns)
		if !ok {
			// メッセージが曖昧だが割愛
			error.ErrorAndExit(errors.New("不正なデータが検出されました。"))
		}

		if datum.NewDataFlag == "1" {
			condition := fmt.Sprintf(conditionFormat, datum.TodofukenCode, datum.ShikuchosonCode, datum.OoazaCode, datum.ChomeCode)
			checkConditions = append(checkConditions, condition)
		}

		values := fmt.Sprintf(
			valuesFormat,
			datum.TodofukenCode,
			datum.ShikuchosonCode,
			datum.OoazaCode,
			datum.ChomeCode,
			datum.TodofukenName,
			datum.ShikuchosonName,
			datum.OoazachomeName,
			datum.Lat,
			datum.Lon)
		insertValues = append(insertValues, values)
	}

	return insertValues, checkConditions, false
}

// 重複チェックとUPSERT
func CheckAndImport(insertValues []string, checkConditions []string) (bool, []string) {
	// エラーメッセージ
	var errorMessages []string

	// 投入データがなければ終了
	if len(insertValues) == 0 {
		errorMessages = append(errorMessages, "投入データがありません。")
		return false, errorMessages
	}

	// DB接続
	db := goPracticeDb.ConnectToDB()
	tx, err := db.Begin()
	error.ErrorAndExit(err)
	defer db.Close()

	// 重複チェック
	if len(checkConditions) > 0 {
		errorMessageFormat := "都道府県コード: %s 市区町村コード: %s 大字コード: %s 丁目コード: %s は既に登録されています。"

		baseCheckSql := "SELECT todofuken_code, shikuchoson_code, ooaza_code, chome_code FROM addresses " +
			"WHERE (todofuken_code, shikuchoson_code, ooaza_code, chome_code) IN (%s)"
		checkSql := fmt.Sprintf(
			baseCheckSql,
			strings.Join(checkConditions, ","))
		checkStmt, err := db.Prepare(checkSql)
		defer checkStmt.Close()
		error.ErrorAndExit(err)

		// それぞれのコードの組み合わせを検索
		checkQuery, err := checkStmt.Query()
		error.ErrorAndExit(err)
		for checkQuery.Next() {
			checkResultRow := AddressesCols{}
			err := checkQuery.Scan(
				&checkResultRow.TodofukenCode,
				&checkResultRow.ShikuchosonCode,
				&checkResultRow.OoazaCode,
				&checkResultRow.ChomeCode)
			error.ErrorAndExit(err)

			errorMessage := fmt.Sprintf(
				errorMessageFormat,
				checkResultRow.TodofukenCode,
				checkResultRow.ShikuchosonCode,
				checkResultRow.OoazaCode,
				checkResultRow.ChomeCode)
			errorMessages = append(errorMessages, errorMessage)
		}
	}

	if len(errorMessages) > 0 {
		tx.Rollback()
		return false, errorMessages
	}

	// データ投入(UPSERT)
	baseSQL := "INSERT INTO addresses VALUES %s " +
		"ON DUPLICATE KEY UPDATE " +
		"todofuken_name = VALUES(todofuken_name), " +
		"shikuchoson_name = VALUES(shikuchoson_name), " +
		"ooazachome_name = VALUES(ooazachome_name), " +
		"lat = VALUES(lat), " +
		"lon = VALUES(lon)"

	upsertStmt, err := db.Prepare(fmt.Sprintf(baseSQL, strings.Join(insertValues, ",")))
	defer upsertStmt.Close()
	error.ErrorAndExit(err)

	_, err = upsertStmt.Exec()
	error.ErrorAndExit(err)
	tx.Commit()

	return true, nil
}

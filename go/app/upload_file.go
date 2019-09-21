package app

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"go-practice/app/error"
	"io"
	"os"
	"path/filepath"
	"strings"
)

func Upload() (bool, map[int]string) {
	// CSVファイルを開く
	currentDir, err := os.Getwd()
	error.ErrorAndExit(err)
	path := filepath.Join(currentDir, "../data/addresses.csv")
	file, err := os.Open(path)
	error.ErrorAndExit(err)

	// インポート関数
	return doImport(file)
}

func doImport(file io.Reader) (bool, map[int]string) {
	// DB接続
	db, err := sql.Open("mysql", "root:root@tcp(127.0.0.1:13306)/go_practice")
	defer db.Close()
	error.ErrorAndExit(err)

	// 行数
	i := 1
	var colNames []string
	// エラーメッセージ　[行数: メッセージ]
	errorMessages := make(map[int]string)

	// 投入データ
	// SQL文分割のため、1万行ずつの配列を格納する
	/*
		 	[
				[
					[データ行], [データ行]... * 10000
				],
				[
					[データ行], [データ行]... * 10000
				],
			]
	*/
	var uploadData [][]string
	var row []string
	var line []string

	reader := csv.NewReader(file)
	// 1行ずつ処理
	for {
		line, err = reader.Read()
		if err == io.EOF {
			break
		}
		error.ErrorAndExit(err)

		// ヘッダ行はスキップ
		if i == 1 {
			i += 1
			continue
		}

		// 列名と値を対応させる
		// mapにしておくことで記述しやすいように
		namedColLine := make(map[string]string)
		for idx, colName := range colNames {
			namedColLine[colName] = line[idx]
		}

		// データが1万行入っていたら、投入データに格納して空で再度初期化
		if len(row) == 10000 {
			uploadData = append(uploadData, row)
			row = nil
		}

		// 新規か
		if namedColLine["newdata_flag"] == "1" {
			// 新規の場合DBと重複していないかチェック
			// 結局UPSERTなので意味はないがバッチ処理っぽい挙動にするため入れた
			var count string

			stmt, err := db.Prepare(
				"SELECT count(*) FROM addresses WHERE " +
					"todofuken_code = ? " +
					"AND shikuchoson_code = ? " +
					"AND ooaza_code = ? " +
					"AND chome_code = ?")
			error.ErrorAndExit(err)
			_ = stmt.QueryRow(
				namedColLine["todofuken_code"],
				namedColLine["shikuchoson_code"],
				namedColLine["ooaza_code"],
				namedColLine["chome_code"]).Scan(&count)

			// 重複していたらエラーメッセージ配列に入れる
			if count != "0" {
				errorMessages[i] = "既にデータが存在します。"
				continue
			}

			stmt.Close()
		}

		// この段階まで来たらnewdata_flagはいらないので削除]
		// mapより配列の方が都合がいいので変数lineを使う
		// SQLのVALUES句 に入れるため"val1", "val2"... のようにダブルクォートで囲んでカンマで区切った文字列にする
		// ちょっと強引か…
		formattedLine := fmt.Sprintf("%#v", line[:len(line)-1])
		formattedLine = strings.TrimRight(strings.TrimPrefix(formattedLine, "[]string{"), "}")
		values := fmt.Sprintf("(%s)", formattedLine)
		row = append(row, values)

		i += 1
	}

	// エラーメッセージ配列が空でない場合エラーを返して終了
	if len(errorMessages) != 0 {
		return false, errorMessages
	}

	// データ投入(UPSERT)
	// 10万件ずつのINSERT文に分けて実行
	baseSQL := "INSERT INTO addresses VALUES %s " +
		"ON DUPLICATE KEY UPDATE " +
		"todofuken_name = VALUES(todofuken_name), " +
		"shikuchoson_name = VALUES(shikuchoson_name), " +
		"ooazachome_name = VALUES(ooazachome_name), " +
		"lat = VALUES(lat), " +
		"lon = VALUES(lon)"

	for _, data := range uploadData {
		upsertStmt, err := db.Prepare(fmt.Sprintf(baseSQL, strings.Join(data, ",")))
		error.ErrorAndExit(err)

		_, err = upsertStmt.Exec()
		upsertStmt.Close()
	}

	return true, nil
}

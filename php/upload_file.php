<?php

require_once 'db_dsn_config.php';
require_once  'db.php';

ini_set('memory_limit', -1);

$file = new SplFileObject(__DIR__ . '/../data/addresses.csv');
$file->setFlags(
    SplFileObject::READ_CSV |
    SplFileObject::SKIP_EMPTY |
    SplFileObject::READ_AHEAD
);

$i = 1;
$columns = [];
$checkConditions = [];
$insertValues = [];
$conditionFormat = "('%s', '%s', '%s', '%s')";
$valuesFormat = "('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')";
$errorMassegeFormat = '都道府県コード: %s 市区町村コード: %s 大字コード: %s 丁目コード: %s は既に登録されています。';
$errorMasseges = [];

$bulkNum = $argv[1];

$pdo = connectToDb($config);
$pdo->beginTransaction();

foreach ($file as $line) {
    if ($i === 1) {
        $columns = $line;
        $i++;
        continue;
    }

    $namedLine = array_combine($columns, $line);
    if ($namedLine['newdata_flag'] === '1') {
        $checkConditions[] = sprintf(
            $conditionFormat,
            $namedLine['todofuken_code'],
            $namedLine['shikuchoson_code'],
            $namedLine['ooaza_code'],
            $namedLine['chome_code']
        );
    }
    
    $insertValues[] = sprintf(
        $valuesFormat,
        $namedLine['todofuken_code'],
        $namedLine['shikuchoson_code'],
        $namedLine['ooaza_code'],
        $namedLine['chome_code'],
        $namedLine['todofuken_name'],
        $namedLine['shikuchoson_name'],
        $namedLine['ooazachome_name'],
        $namedLine['lat'],
        $namedLine['lon']
    );

    if ($i % $bulkNum === 0) {
        if (empty($insertValues)) {
            echo "投入データがありません。";
            exit();
        }

        if (!empty($checkConditions)) {
            $checkSql = sprintf(
                'SELECT todofuken_code, shikuchoson_code, ooaza_code, chome_code FROM addresses '
                . 'WHERE (todofuken_code, shikuchoson_code, ooaza_code, chome_code) IN (%s)',
                implode(',', $checkConditions)
            );

            $checkStmt = $pdo->prepare($checkSql);
            $checkStmt->execute();
            $results = $checkStmt->fetchAll();
            if (!empty($results)) {
                foreach ($results as $result) {
                    $errorMassege = sprintf(
                        $errorMassegeFormat,
                        $result['todofuken_code'],
                        $result['shikuchoson_code'],
                        $result['ooaza_code'],
                        $result['chome_code']
                    );
                    $errorMasseges[] = $errorMassege;
                }
            }
        }

        $upsertSql = "INSERT INTO addresses VALUES %s 
            ON DUPLICATE KEY UPDATE 
            todofuken_name = VALUES(todofuken_name), 
            shikuchoson_name = VALUES(shikuchoson_name), 
            ooazachome_name = VALUES(ooazachome_name), 
            lat = VALUES(lat), 
            lon = VALUES(lon)";
        $upsertStmt = $pdo->prepare(sprintf($upsertSql, implode(',', $insertValues)));
        $upsertStmt->execute();
        $checkConditions = [];
        $insertValues = [];
    }
    $i++;
}

if (!empty($errorMasseges)) {
    $pdo->rollBack();
    file_put_contents(__DIR__ . '/../data/log_php.log', implode("\n", $errorMasseges));
    echo 'upload failed';
} else {
    $pdo->commit();
    echo 'upload succeeded';
}
<?php

require_once 'db_dsn_config.php';
require_once  'db.php';

ini_set('memory_limit', -1);

$file = new SplFileObject(__DIR__ . '/../data/addresses_from_db_php.csv', 'a');

$pdo = connectToDb($config);

$totalCount = $pdo->query("SELECT count(*) AS count FROM addresses");
$count = ($totalCount->fetch())['count'];

$limit = 10000;
$bulkNum = ceil($count / $limit);

$file->fputcsv(["todofuken_code",
    "shikuchoson_code",
    "ooaza_code",
    "chome_code",
    "todofuken_name",
    "shikuchoson_name",
    "ooazachome_name",
    "lat",
    "lon",
    "newdata_flag"]);

foreach (range(0, $bulkNum) as $num) {
    $offset = $limit * $num;

    $sql = $pdo->prepare(sprintf("SELECT * FROM addresses LIMIT %d OFFSET %d", $limit, $offset));
    $sql->execute();

    foreach ($sql->fetchAll(PDO::FETCH_ASSOC) as $row) {
        $row['newdata_flag'] = '0';
        $file->fputcsv($row);
    }
}
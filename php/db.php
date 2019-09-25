<?php

function connectToDb($config) {
    $dsn = "mysql:dbname={$config['db_name']};host={$config['host']};port={$config['port']}";
    return new PDO($dsn, $config['user'], $config['pass']);
}


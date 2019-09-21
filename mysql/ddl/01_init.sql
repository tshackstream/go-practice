CREATE TABLE IF NOT EXISTS `addresses` (
    `todofuken_code` VARCHAR(10) NOT NULL COMMENT '都道府県コード',
    `shikuchoson_code` VARCHAR(10) NOT NULL COMMENT '市区町村コード',
    `ooaza_code` VARCHAR(10) NOT NULL COMMENT '大字コード',
    `chome_code` VARCHAR(10) NOT NULL COMMENT '丁目コード',
    `todofuken_name` VARCHAR(20) NOT NULL COMMENT '都道府県名',
    `shikuchoson_name` VARCHAR(20) NOT NULL COMMENT '市区町村名',
    `ooazachome_name` VARCHAR(20) NOT NULL COMMENT '大字丁目名',
    `lat` VARCHAR(10) NOT NULL COMMENT '緯度',
    `lon` VARCHAR(10) NOT NULL COMMENT '経度',
    PRIMARY KEY (`todofuken_code`, `shikuchoson_code`, `ooaza_code`, `chome_code`)
);
package app

// DBカラムの構造体
type AddressesCols struct {
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

// CSV列の構造体
type CsvColumns struct {
	TodofukenCode   string `csv:"todofuken_code"`
	ShikuchosonCode string `csv:"shikuchoson_code"`
	OoazaCode       string `csv:"ooaza_code"`
	ChomeCode       string `csv:"chome_code"`
	TodofukenName   string `csv:"todofuken_name"`
	ShikuchosonName string `csv:"shikuchoson_name"`
	OoazachomeName  string `csv:"ooazachome_name"`
	Lat             string `csv:"lat"`
	Lon             string `csv:"lon"`
	NewDataFlag     string `csv:"newdata_flag"`
}

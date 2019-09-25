package error

import "log"

func ErrorAndExit(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

package models

import (
	"encoding/json"
	"log"
	"os"
)

type Directories struct {
	Directory []string `json:"directories"`
}

func ReadDirectoriesConfigFile(filepath string) *Directories {
	file, err := os.ReadFile(filepath)
	if err != nil {
		log.Fatal("Error when opening file: ", err)
	}

	var payload Directories
	err = json.Unmarshal(file, &payload)
	if err != nil {
		log.Fatal("Error during Unmarshal(): ", err)
	}
	return &payload
}

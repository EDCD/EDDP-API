package main

import (
	"bufio"
	"bytes"
	"database/sql"
	"encoding/json"
	"log"
	"os"
	"strconv"

	_ "github.com/mattn/go-sqlite3" // SQLite driver
)

// Database connections
var eddpDb *sql.DB

type Systems struct {
	System []struct {
		data map[string]interface{}
	}
}

func assertNil(e error) {
	if e != nil {
		log.Print(e)
		panic(e)
	}
}

func main() {
	var err error
	eddpDb, err = sql.Open("sqlite3", "/data/sqlite/eddp-new.sqlite")
	assertNil(err)
	defer eddpDb.Close()

	SetupTables()
	_, err = eddpDb.Exec("PRAGMA synchronous = OFF")
	assertNil(err)
	_, err = eddpDb.Exec("PRAGMA journal_mode = OFF")
	assertNil(err)

	ImportBodies()

	SetupIndices()
}

func SetupTables() {
	_, err := eddpDb.Exec("CREATE TABLE IF NOT EXISTS bodies(id INT NOT NULL, system_id INT NOT NULL, name TEXT COLLATE NOCASE NOT NULL, data TEXT NOT NULL)")
	assertNil(err)
}

func SetupIndices() {
	_, err := eddpDb.Exec("CREATE INDEX IF NOT EXISTS bodies_idx1 ON bodies(id)")
	assertNil(err)
	_, err = eddpDb.Exec("CREATE INDEX IF NOT EXISTS bodies_idx2 ON bodies(system_id)")
	assertNil(err)
	_, err = eddpDb.Exec("CREATE INDEX IF NOT EXISTS bodies_idx3 ON bodies(name)")
	assertNil(err)
}

func ImportBodies() {
	file, err := os.Open("/data/eddb/bodies.jsonl")
	assertNil(err)
	defer file.Close()

	_, err = eddpDb.Exec("BEGIN")
	assertNil(err)

	// Work through the file one line at a time
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		data := scanner.Text()
		rawData := []byte(data)
		var body map[string]interface{}

		// Set up a decoder that leaves numbers alone
		d := json.NewDecoder(bytes.NewBuffer(rawData))
		d.UseNumber()

		err := d.Decode(&body)
		if err != nil {
			log.Fatal(err)
		}
		for k, v := range body {
			if (v == nil) {
				delete(body, k)
			} else {
				switch vv := v.(type) {
					case []interface{}:
						if (len(vv) == 0) {
							delete(body, k)
						}
				}
			}

		}
		munged, err := json.Marshal(body)

		bodyId, err := strconv.ParseUint(string(body["id"].(json.Number)), 10, 64);
		assertNil(err)
		systemId, err := strconv.ParseUint(string(body["system_id"].(json.Number)), 10, 64);
		assertNil(err)
		_, err = eddpDb.Exec("INSERT INTO bodies(id, system_id, name, data) VALUES(?, ?, ?, ?)", bodyId, systemId, body["name"].(string), string(munged))
		assertNil(err)
	}

	_, err = eddpDb.Exec("COMMIT")
	assertNil(err)
}

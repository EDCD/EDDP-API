package main

import (
	"bytes"
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"

	_ "github.com/mattn/go-sqlite3" // SQLite driver
)

// Database connections
var eddpDb *sql.DB

type Systems struct {
	System []struct {
		data map[string]interface{}
	}
}

func assertNotNil(e error) {
	if e != nil {
		log.Print(e)
		panic(e)
	}
}

func main() {
	var err error
	eddpDb, err = sql.Open("sqlite3", "/data/sqlite/eddp-new.sqlite")
	assertNotNil(err)
	defer eddpDb.Close()

	SetupTables()
	_, err = eddpDb.Exec("PRAGMA synchronous = OFF")
	assertNotNil(err)
	_, err = eddpDb.Exec("PRAGMA journal_mode = OFF")
	assertNotNil(err)

	ImportSystems()

	SetupIndices()
}

func SetupTables() {
	_, err := eddpDb.Exec("CREATE TABLE IF NOT EXISTS systems(id INT NOT NULL, x DECIMAL(10, 5) NOT NULL, y DECIMAL(10, 5) NOT NULL, z DECIMAL(10, 5) NOT NULL, name TEXT COLLATE NOCASE NOT NULL, data TEXT NOT NULL)")
	assertNotNil(err)
}

func SetupIndices() {
	_, err := eddpDb.Exec("CREATE INDEX IF NOT EXISTS systems_idx1 ON systems(id)")
	assertNotNil(err)
	_, err = eddpDb.Exec("CREATE INDEX IF NOT EXISTS systems_idx2 ON systems(name)")
	assertNotNil(err)
}

func ImportSystems() {
	file, err := os.Open("/data/eddb/systems.csv")
	assertNotNil(err)
	defer file.Close()

	_, err = eddpDb.Exec("BEGIN")
	assertNotNil(err)

	reader := csv.NewReader(file)
	// Read header
	_, err = reader.Read()
	// Work through the file one line at a time
	for {
		line, err := reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			fmt.Println("Error:", err)
			return
		}
		var buffer bytes.Buffer

		buffer.WriteString("{")

		if (line[0] == "") {
			fmt.Println("Line without ID: ")
			continue
		}
		id := line[0]
		buffer.WriteString("\"id\":")
		buffer.WriteString(id)

		if (line[2] == "") {
			fmt.Println("Line without name: ")
			continue
		}
		name := line[2]
		buffer.WriteString(",\"name\":\"")
		buffer.WriteString(name)
		buffer.WriteString("\"")

		if (line[3] == "") {
			fmt.Println("Line without X co-ordinate: ")
			continue
		}
		buffer.WriteString(",\"x\":")
		buffer.WriteString(line[3])
		x := line[3]

		if (line[4] == "") {
			fmt.Println("Line without Y co-ordinate: ")
			continue
		}
		buffer.WriteString(",\"y\":")
		buffer.WriteString(line[4])
		y := line[4]

		if (line[5] == "") {
			fmt.Println("Line without Z co-ordinate: ")
			continue
		}
		buffer.WriteString(",\"z\":")
		buffer.WriteString(line[5])
		z := line[5]

		if (line[6] != "") {
			buffer.WriteString(",\"population\":")
			buffer.WriteString(line[6])
		}

		if (line[7] == "1") {
			buffer.WriteString(",\"is_populated\":true")
		} else {
			buffer.WriteString(",\"is_populated\":false")
		}

		if (line[9] != "") {
			buffer.WriteString(",\"government\":\"")
			buffer.WriteString(line[9])
			buffer.WriteString("\"")
		}

		if (line[11] != "") {
			buffer.WriteString(",\"allegiance\":\"")
			buffer.WriteString(line[11])
			buffer.WriteString("\"")
		}

		if (line[13] != "") {
			buffer.WriteString(",\"state\":\"")
			buffer.WriteString(line[13])
			buffer.WriteString("\"")
		}

		if (line[15] != "") {
			buffer.WriteString(",\"security\":\"")
			buffer.WriteString(line[15])
			buffer.WriteString("\"")
		}

		if (line[17] != "") {
			buffer.WriteString(",\"primary_economy\":\"")
			buffer.WriteString(line[17])
			buffer.WriteString("\"")
		}

		if (line[18] != "") {
			buffer.WriteString(",\"power\":\"")
			buffer.WriteString(line[18])
			buffer.WriteString("\"")
		}

		if (line[19] != "") {
			buffer.WriteString(",\"power_state\":\"")
			buffer.WriteString(line[19])
			buffer.WriteString("\"")
		}

		if (line[22] != "") {
			buffer.WriteString(",\"updated_at\":")
			buffer.WriteString(line[22])
		}

		if (line[25] != "") {
			buffer.WriteString(",\"faction\":\"")
			buffer.WriteString(line[25])
			buffer.WriteString("\"")
		}

		if (line[27] != "") {
			buffer.WriteString(",\"reserve_type\":\"")
			buffer.WriteString(line[27])
			buffer.WriteString("\"")
		}

		buffer.WriteString("}")

		_, err = eddpDb.Exec("INSERT INTO systems(id, x, y, z, name, data) VALUES(?, ?, ?, ?, ?, ?)", id, x, y, z, name, buffer.String())
		assertNotNil(err)
	}


	_, err = eddpDb.Exec("COMMIT")
	assertNotNil(err)
}

package main

import (
        "bufio"
	"bytes"
        "database/sql"
	"encoding/csv"
        "encoding/json"
        "fmt"
        "io"
        "io/ioutil"
        "log"
        "os"
        "strconv"
        "strings"

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

        ImportStations()

        SetupIndices()
}

func SetupTables() {
        _, err := eddpDb.Exec("CREATE TABLE IF NOT EXISTS stations(id INT NOT NULL, system_id INT NOT NULL, name TEXT COLLATE NOCASE NOT NULL, data TEXT NOT NULL)")
        assertNil(err)
}

func SetupIndices() {
        _, err := eddpDb.Exec("CREATE INDEX IF NOT EXISTS stations_idx1 ON stations(id)")
        assertNil(err)
        _, err = eddpDb.Exec("CREATE INDEX IF NOT EXISTS stations_idx2 ON stations(system_id)")
        assertNil(err)
        _, err = eddpDb.Exec("CREATE INDEX IF NOT EXISTS stations_idx3 ON stations(name)")
        assertNil(err)
}

func ImportStations() {
        // Import the commodities locally
        commoditiesFile, err := ioutil.ReadFile("/data/eddb/commodities.json")
        assertNil(err)
        var commoditiesDefinitions []map[string]interface{}
        err = json.Unmarshal(commoditiesFile, &commoditiesDefinitions)
        var commodities map[int]string
        commodities =  make(map[int]string)
        for _,element := range commoditiesDefinitions {
                commodities[int(element["id"].(float64))] = element["name"].(string)
        }

        // Fetch the market listings
        listingsFile, err := os.Open("/data/eddb/listings.csv")
        assertNil(err)
        defer listingsFile.Close()

        var m map[string]bytes.Buffer
        m = make(map[string]bytes.Buffer)

        reader := csv.NewReader(listingsFile)
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

                if (line[1] == "") {
                        fmt.Println("Line without station ID")
                        continue
                }
                stationid := line[1]

                if (line[2] == "") {
                        fmt.Println("Line without commodity ID")
                        continue
                }
                commodityid, err := strconv.Atoi(line[2])

                if (line[3] == "") {
                        fmt.Println("Line without supply")
                        continue
                }
                supply, err := strconv.Atoi(line[3])

                if (line[4] == "") {
                        fmt.Println("Line without buy price")
                        continue
                }
                buyprice := line[4]

                if (line[5] == "") {
                        fmt.Println("Line without sell price")
                        continue
                }
                sellprice := line[5]

                if (line[6] == "") {
                        fmt.Println("Line without demand")
                        continue
                }
                demand, err := strconv.Atoi(line[6])

                buffer, continuation := m[stationid]
                if (continuation) {
                        // Continuation
                        buffer.WriteString(",")
                } else {
                        // New entry
                        buffer.WriteString("[")
                }

                buffer.WriteString("{\"id\":")
                buffer.WriteString(strconv.Itoa(commodityid));
                buffer.WriteString(",")

                buffer.WriteString("\"name\":\"")
                buffer.WriteString(commodities[commodityid]);
                buffer.WriteString("\"")

                if (supply > 0) {
                        buffer.WriteString(",\"supply\":")
                        buffer.WriteString(strconv.Itoa(supply));
                        buffer.WriteString(",\"buy_price\":")
                        buffer.WriteString(buyprice);
                }

                if (demand > 0) {
                        buffer.WriteString(",\"demand\":")
                        buffer.WriteString(strconv.Itoa(demand));
                        buffer.WriteString(",\"sell_price\":")
                        buffer.WriteString(sellprice);
                }

                buffer.WriteString("}")
                m[stationid] = buffer
        }

        // Import the factions locally
        factionsFile, err := os.Open("/data/eddb/factions.csv")
        assertNil(err)
        defer factionsFile.Close()

        var factions map[int]string
        factions = make(map[int]string)

        reader = csv.NewReader(factionsFile)
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

                if (line[0] == "") {
                        fmt.Println("Line without faction ID")
                        continue
                }
                factionid, err := strconv.Atoi(line[0])
		assertNil(err)

                if (line[1] == "") {
                        fmt.Println("Line without faction name")
                        continue
                }
                factionname := line[1]
		factions[factionid] = factionname
	}

	// Work through the stations file
        file, err := os.Open("/data/eddb/stations.jsonl")
        assertNil(err)
        defer file.Close()

        _, err = eddpDb.Exec("BEGIN")
        assertNil(err)

        // Work through the file one line at a time
        scanner := bufio.NewScanner(file)
        for scanner.Scan() {
                data := scanner.Text()
                //bytes := []byte(data)

                d := json.NewDecoder(strings.NewReader(data))
                d.UseNumber()
                var station map[string]interface{}
                err = d.Decode(&station)
                assertNil(err)

		systemid, err := station["system_id"].(json.Number).Int64()
		stationid, err := station["id"].(json.Number).Int64()

		// Remove stuff we don't want.
		// In general these are either foreign IDs or items that we can't keep up-to-date automatically
		delete(station, "government_id")
		delete(station, "allegiance_id")
		delete(station, "state_id")
		delete(station, "type_id")
		delete(station, "import_commodities")
		delete(station, "export_commodities")
		delete(station, "prohibited_commodities")
		delete(station, "settlement_size_id")
		delete(station, "settlement_security_id")
		delete(station, "body_id")
		if (station["controlling_minor_faction_id"] != nil) {
			factionid, err := station["controlling_minor_faction_id"].(json.Number).Int64()
			assertNil(err)
			station["controlling_faction"] = factions[int(factionid)]
			delete(station, "controlling_minor_faction_id")
		}
		if (station["economies"] != nil && len(station["economies"].([]interface{})) > 0) {
			station["primary_economy"] = station["economies"].([]interface{})[0].(string)
		}
		delete(station, "economies")


		updatedData, err := json.Marshal(station)
		assertNil(err)
		data = string(updatedData)

		stationCommodities, stationCommoditiesExists := m[strconv.Itoa(int(stationid))]
		if (stationCommoditiesExists) {
			// Patch in the commodities
			data = data[:len(data)-1]
			data = data + ",\"commodities\":"
			data = data + stationCommodities.String()
			data = data + "]}"
		}

                _, err = eddpDb.Exec("INSERT INTO stations(id, system_id, name, data) VALUES(?, ?, ?, ?)", stationid, systemid, station["name"].(string), data)
                assertNil(err)
        }

        _, err = eddpDb.Exec("COMMIT")
        assertNil(err)
}

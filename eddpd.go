package main

import (
	"database/sql"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"runtime"
	"strings"
	"time"

	"github.com/ajays20078/go-http-logger" // Log HTTP requests
	"github.com/gorilla/mux"               // URL-based routing
	_ "github.com/mattn/go-sqlite3"        // SQLite driver
	"github.com/nytimes/gziphandler"       // GZip handler
)

// Database connections
var eddpDb *sql.DB
var profileDb *sql.DB
var errorDb *sql.DB

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	var err error
	eddpDb, err = sql.Open("sqlite3", "/data/sqlite/eddp.sqlite")
	if err != nil {
		log.Print(err)
	}
	errorDb, err = sql.Open("sqlite3", "/data/sqlite/error.sqlite")
	if err != nil {
		log.Print(err)
	}
	profileDb, err = sql.Open("sqlite3", "/data/sqlite/profile.sqlite")
	if err != nil {
		log.Print(err)
	}

	r := mux.NewRouter()
	r.HandleFunc("/version", VersionHandler).Methods("GET")
	r.HandleFunc("/eddi/version", VersionHandler).Methods("GET")
	r.HandleFunc("/error", ErrorHandler).Methods("POST")
	r.HandleFunc("/log", LogHandler).Methods("POST")
	r.HandleFunc("/profile", ProfileHandler).Methods("POST")
	// Static JSON files
	r.PathPrefix("/_").Handler(http.StripPrefix("/_", http.FileServer(http.Dir("/home/eddp/files"))))
	r.PathPrefix("/.").Handler(http.StripPrefix("/", http.FileServer(http.Dir("/home/eddp/files"))))
	// Generic database handler
	r.HandleFunc("/{category}/{item}", DatabaseHandler).Methods("GET")

	http.Handle("/", gziphandler.GzipHandler(r))
	err = http.ListenAndServe(":80", JsonContent(httpLogger.WriteLog(http.DefaultServeMux, os.Stdout)))
	if err != nil {
		log.Print("ListenAndServe: ", err)
	}
}

func VersionHandler(w http.ResponseWriter, r *http.Request) {
	io.WriteString(w, "2.2.0")
}

func ErrorHandler(w http.ResponseWriter, r *http.Request) {
	body, err := ioutil.ReadAll(io.LimitReader(r.Body, 1048576))
	if err != nil {
		log.Print(err)
	}
	if err := r.Body.Close(); err != nil {
		log.Print(err)
	}

	_, err = errorDb.Exec("INSERT INTO errors(ipaddr, error) VALUES(?, ?)", r.RemoteAddr, string(body))
	if err != nil {
		log.Print(err)
	}
}

func LogHandler(w http.ResponseWriter, r *http.Request) {
	body, err := ioutil.ReadAll(io.LimitReader(r.Body, 1048576*1024))
	if err != nil {
		log.Print(err)
	}
	if err := r.Body.Close(); err != nil {
		log.Print(err)
	}

	regex, _ := regexp.Compile("^[^:]*")
	ipaddr := regex.FindString(r.RemoteAddr)

	err = ioutil.WriteFile("logs/"+ipaddr+"-"+time.Now().Format("20060102T150405"), body, 0644)
	if err != nil {
		log.Print(err)
	}
}

func ProfileHandler(w http.ResponseWriter, r *http.Request) {
	body, err := ioutil.ReadAll(io.LimitReader(r.Body, 1048576))
	if err != nil {
		log.Print(err)
	}
	if err := r.Body.Close(); err != nil {
		log.Print(err)
	}

	_, err = profileDb.Exec("INSERT INTO profiles(ipaddr, profile) VALUES(?, ?)", r.RemoteAddr, string(body))
	if err != nil {
		log.Print(err)
	}
}

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHJKLMNPQRSTUVWXYZ123456789"

func RandStringBytes(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

func DatabaseHandler(w http.ResponseWriter, r *http.Request) {
	regex := regexp.MustCompile("[^A-Za-z]")
	vars := mux.Vars(r)
	category, err := url.QueryUnescape(regex.ReplaceAllString(vars["category"], ""))
	if err != nil {
		log.Print(err)
		w.WriteHeader(500)
		return
	}
	item, err := url.QueryUnescape(vars["item"])
	if err != nil {
		log.Print(err)
		w.WriteHeader(500)
		return
	}

	var dataId int
	var data string
	err = eddpDb.QueryRow(fmt.Sprintf("SELECT id, data FROM %s WHERE name = ?", category), item).Scan(&dataId, &data)
	if err != nil {
		log.Print(err)
		w.WriteHeader(404)
		return
	}
	if category == "systems" {
		var bodies []string
		rows, err := eddpDb.Query("SELECT data FROM bodies WHERE system_id = ?", dataId)
		if err == nil {
			defer rows.Close()
			for rows.Next() {
				var bodyData string
				err = rows.Scan(&bodyData)
				bodies = append(bodies, bodyData)
			}
		}
		var stations []string
		rows, err = eddpDb.Query("SELECT data FROM stations WHERE system_id = ?", dataId)
		if err == nil {
			defer rows.Close()
			for rows.Next() {
				var stationData string
				err = rows.Scan(&stationData)
				stations = append(stations, stationData)
			}
		}
		// Hack the system string to remove the final close bracket and add in the data we have gathered
		data = strings.TrimSuffix(data, "}")
		data = data + ",\"bodies\":[" + strings.Join(bodies, ",") + "],\"stations\":[" + strings.Join(stations, ",") + "]}"
	}
	io.WriteString(w, data)
}

// Set content-type for JSON
func JsonContent(next http.Handler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		next.ServeHTTP(w, r)
	}
}

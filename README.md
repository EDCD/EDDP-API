# EDDP-API
Caching server for use with EDDI and EDDN.

## Dependencies

1. [Go, aka Golang](https://golang.org/dl/).
1. sqlite3.
1. [ZeroMQ](http://zeromq.org/intro:get-the-software). 
  * On MacOs, `brew install zmq`.
  * On Debian `apt-get install libzmq5` (?)
1. `./getDependencies` will get any Go dependencies.

## Configuration

Environment variable          | Default value               | Meaning
----------------------------- | --------------------------- | -------
`EDDP_API_DATA_DIR`           | `./data`                    | Directory for downloaded data and SQL database
`EDDP_API_HTTP_ADDR`          | `":8080"`                   | TCP address for the HTTP server
`EDDP_API_EDDN_LISTENER_URL`  | `"tcp://eddn.edcd.io:9500"` | URL for the EDDN listener
`EDDP_API_EDDN_PUBLISHER_URL` | `"tcp://*:5556"`            | URL for the EDDN publisher

## Setup and development

* `./getDependencies` to get all required Go packages.
* `./formatAll` to format all Go source according to standards (Go is rather prescriptive about that).
* `./buildAll` to build all Go source.
  * Currently each top-level `.go` file must be added manually to this script, and its build product added to `.gitignore`.

## Usage

* `refreshEDDB` to fetch the latest data from EDDB to `${dataDir}/eddb`. At the time of writing this totals about 3.5GB). This script calls ...
* `rebuild` to import this fetched data into SQLite. On a 2011 MacBook Air this can take around 12 min and the resulting SQLlite file is around 8.5GB. Once it completes, you need to
  * manually stop the servers (instructions to follow).
  * replace `${dataDir}/sqlite/eddp.sqlite` with `${dataDir}/sqlite/eddp-new.sqlite`
  * restart the servers (instructions to follow).
  * The raw data in `${dataDir}/eddb` can then be zipped or discarded.

## Server Deployment

To follow.

# EDDP-API
Caching server for use with EDDI and EDDN.

## Dependencies

1. [Golang](https://golang.org/dl/).
1. sqlite3.
1. [ZeroMQ](http://zeromq.org/intro:get-the-software). 
  * On MacOs, `brew install zmq`.
  * On Debian `apt-get install libzmq5` (?)
1. `./getDependencies` will get any go dependencies.

## Configuration

To follow.

## Setup

* `./getDependencies` to get all required go packages.
* `./buildAll` to build all scripts (currently each `.go` file must be added manually and its build product added ti `.gitignore`).

## Usage

* `refreshEDDB` to fetch the latest data from EDDB to `${DataDir}/eddb`. At the time of writing this totals about 3.5GB). This script calls ...
* `rebuild` to import this fetched data into SQLite. On a MacBook Ai this can take around 12 min and the resulting SQLlite file is around 8.5GB. once it completes, you need to
  *  manually stop the servers (instructions to follow)
  * replace `${DataDir}/sqlite/eddp.sqlite` with `${DataDir}/sqlite/eddp-new.sqlite`
  * restart the servers.
  * the raw data in `${DataDir}/eddb` can then be zipped or discarded.

## Server Deployment

To follow.

# GoStore

GoStore is a lightweight key/value database written in Go.

I chose to build GoStore to learn how databases like Redis and Sqlite work internally.


### Features 
- B+Tree index with splitting, merging, borrowing and rebalancing
- Pager for fixed-size page IO + free-list management
- Write-Ahead Log for crash recovery
- Authenticated TCP server with a simple text protocol
- Admin CLI for creating / deleting databases and managing users

### Install
```bash
git clone https://github.com/taran1sdev/GoStore.git
cd gostore
go build ./cmd/gostore
```

### Directory Layout
Application data is stored in `~/.local/share/gostore` by default
```
gostore/
├── config.yaml
├── data
│   └── example
│       ├── example.db
│       └── example.db.wal
├── log
│   └── example.log
└── users.json
```

### CLI
The CLI was built with cobra and provides an administrative interface
```
GoStore CLI

Usage:
  gostore [command]

Available Commands:
  create      Create a new database
  create-user Create a new GoStore user
  delete      Delete an existing database
  delete-user Delete a GoStore user
  grant       Grant user access to db
  help        Help about any command
  revoke      Revoke user access to a database
  start       Start GoStore server

Flags:
      --config string   Path to config.yaml
  -h, --help            help for gostore
      --home string     GoStore home directory

Use "gostore [command] --help" for more information about a command.
```

### User Roles
Users must be granted access to databases through the CLI

- User - Read/Write 
- Guest - Read Only

### Server
By default the server will start on `localhost:57083` - you can change this in `config.yaml`

To start the server with default settings run
```
gostore start
```

To connect via nc
```bash
nc localhost 57083
```

Clients communicate with simple text commands:
```
AUTH username password
OPEN dbname
SET key value
GET key
DEL key
QUIT
```

### TODO
- TLS encryption 
- Go client library for embedding GoStore directly in Go projects
- Binary protocol for faster clients
- Compression for large database files
- Snapshots / backups
- Support for Windows


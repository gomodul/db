package db

// Engine represents a supported database engine.
type Engine string

const (
	// SQL databases
	EngineMySQL       Engine = "mysql"
	EnginePostgreSQL  Engine = "postgresql"
	EngineSQLite      Engine = "sqlite"
	EngineMSSQL       Engine = "mssql"
	EngineOracle      Engine = "oracle"
	EngineMariaDB     Engine = "mariadb"
	EngineCockroachDB Engine = "cockroachdb"
	EngineTiDB        Engine = "tidb"
	EngineYugabyteDB  Engine = "yugabytedb"
	EngineVoltDB      Engine = "voltdb"

	// Document databases
	EngineMongoDB  Engine = "mongodb"
	EngineCouchDB  Engine = "couchdb"
	EngineFirebase Engine = "firebase"
	EngineFaunaDB  Engine = "faunadb"

	// Key-value databases
	EngineRedis    Engine = "redis"
	EngineDynamoDB Engine = "dynamodb"

	// Column-family databases
	EngineCassandra Engine = "cassandra"
	EngineScyllaDB  Engine = "scylladb"
	EngineHBase     Engine = "hbase"

	// Graph databases
	EngineNeo4j    Engine = "neo4j"
	EngineDGraph   Engine = "dgraph"
	EngineArangoDB Engine = "arangodb"

	// Search engines
	EngineElasticSearch Engine = "elasticsearch"

	// Time-series databases
	EngineInfluxDB Engine = "influxdb"

	// Other
	EngineRethinkDB Engine = "rethinkdb"
)

// String returns the engine name as a string.
func (e Engine) String() string {
	return string(e)
}

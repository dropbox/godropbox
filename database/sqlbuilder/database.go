package sqlbuilder

type Dialect interface {
	EscapeCharacter() rune
	InsertReturningClause() string
	Kind() string
	Name() *string
}

type genericDialect struct {
	escapeChar      rune
	returningClause string
	kind            string
	name            *string
}

func (db *genericDialect) EscapeCharacter() rune {
	return db.escapeChar
}

func (db *genericDialect) InsertReturningClause() string {
	return db.returningClause
}

func (db *genericDialect) Kind() string {
	return db.kind
}

func (db *genericDialect) Name() *string {
	return db.name
}

func NewMySQLDialect(dbName *string) Dialect {
	return &genericDialect{
		escapeChar: '`',
		kind:       "mysql",
		name:       dbName,
	}
}

func NewPostgresDialect(dbName *string) Dialect {
	return &genericDialect{
		escapeChar:      '"',
		returningClause: " RETURNING *",
		kind:            "postgres",
		name:            dbName,
	}
}

func NewSQLiteDialect() Dialect {
	defaultName := "main"
	return &genericDialect{
		escapeChar: '"',
		kind:       "sqlite",
		name:       &defaultName,
	}
}

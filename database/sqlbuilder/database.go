package sqlbuilder

type Database interface {
	EscapeCharacter() rune
	InsertReturningClause() string
	Kind() string
	Name() *string
}

type genericDatabase struct {
	escapeChar      rune
	returningClause string
	kind            string
	name            *string
}

func (db *genericDatabase) EscapeCharacter() rune {
	return db.escapeChar
}

func (db *genericDatabase) InsertReturningClause() string {
	return db.returningClause
}

func (db *genericDatabase) Kind() string {
	return db.kind
}

func (db *genericDatabase) Name() *string {
	return db.name
}

func NewMySQLDatabase(dbName *string) Database {
	return &genericDatabase{
		escapeChar: '`',
		kind:       "mysql",
		name:       dbName,
	}
}

func NewPostgresDatabase(dbName *string) Database {
	return &genericDatabase{
		escapeChar:      '"',
		returningClause: " RETURNING *",
		kind:            "postgres",
		name:            dbName,
	}
}

func NewSQLiteDatabase() Database {
	defaultName := "main"
	return &genericDatabase{
		escapeChar: '"',
		kind:       "sqlite",
		name:       &defaultName,
	}
}

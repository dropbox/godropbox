package sqlbuilder

type Database interface {
	EscapeCharacter() rune
	InsertReturningClause() string
	Name() *string
}

type genericDatabase struct {
	escapeChar      rune
	returningClause string
	name            *string
}

func (db *genericDatabase) EscapeCharacter() rune {
	return db.escapeChar
}

func (db *genericDatabase) InsertReturningClause() string {
	return db.returningClause
}

func (db *genericDatabase) Name() *string {
	return db.name
}

func NewMySQLDatabase(dbName *string) Database {
	return &genericDatabase{
		escapeChar: '`',
		name:       dbName,
	}
}

func NewPostgresDatabase(dbName *string) Database {
	return &genericDatabase{
		escapeChar:      '"',
		returningClause: " RETURNING *",
		name:            dbName,
	}
}

func NewSQLiteDatabase() Database {
	defaultName := "main"
	return &genericDatabase{
		escapeChar: '"',
		name:       &defaultName,
	}
}

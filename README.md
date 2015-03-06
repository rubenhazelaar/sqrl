# Lite Squirrel - fat-free version of fluent SQL generator for Go

**Non thread safe** fork of [squirrel](http://github.com/lann/squirrel)

```go
import "github.com/elgris/squirrel"
```

[![GoDoc](https://godoc.org/github.com/elgris/squirrel?status.png)](https://godoc.org/github.com/elgris/squirrel)
[![Build Status](https://travis-ci.org/elgris/squirrel.png?branch=master)](https://travis-ci.org/elgris/squirrel)

## Inspired by

- [squirrel](https://github.com/lann/squirrel)
- [dbr](https://github.com/gocraft/dbr)

## Why to make good squirrel lighter?

Ask [benchmarks](github.com/elgris/golang-sql-builder-benchmark) about that ;). Squirrel is good, reliable and thread-safe with it's immutable query builder. Although immutability is nice, it's resource consuming and sometimes redundant. As authors of `dbr` say: "100% of our application code was written without the need for this".

## Why not to use dbr then?

Although, [dbr](https://github.com/gocraft/dbr) is proven to be much [faster than squirrel](https://github.com/tyler-smith/golang-sql-benchmark) and even faster than [lite squirrel](https://github.com/elgris/golang-sql-builder-benchmark), it doesn't have all syntax sugar. Especially I miss support of JOINs, subqueries and aliases.
Second reason is `dbr`'s sweet query builder requires `Session` which requires established database connection. I don't want to connect to database when I need to generate SQL string.

## Usage

**Squirrel is not an ORM.**, it helps you build SQL queries from composable parts.
**Squirrel lite is non thread safe**. SQL builder change their state, so using the same builder in parallel is dangerous.

It's very easy to switch between original squirrel and light one, because there is no change in interface:

```go
import sq "github.com/elgris/squirrel" // you can easily use github.com/lann/squirrel here

users := sq.Select("*").From("users").Join("emails USING (email_id)")

active := users.Where(sq.Eq{"deleted_at": nil})

sql, args, err := active.ToSql()

sql == "SELECT * FROM users JOIN emails USING (email_id) WHERE deleted_at IS NULL"
```

```go
sql, args, err := sq.
    Insert("users").Columns("name", "age").
    Values("moe", 13).Values("larry", sq.Expr("? + 5", 12)).
    ToSql()

sql == "INSERT INTO users (name,age) VALUES (?,?),(?,? + 5)"
```

Squirrel can also execute queries directly:

```go
stooges := users.Where(sq.Eq{"username": []string{"moe", "larry", "curly", "shemp"}})
three_stooges := stooges.Limit(3)
rows, err := three_stooges.RunWith(db).Query()

// Behaves like:
rows, err := db.Query("SELECT * FROM users WHERE username IN (?,?,?,?) LIMIT 3",
                      "moe", "larry", "curly", "shemp")
```

Squirrel makes conditional query building a breeze:

```go
if len(q) > 0 {
    users = users.Where("name LIKE ?", q)
}
```

## License

Squirrel is released under the
[MIT License](http://www.opensource.org/licenses/MIT).

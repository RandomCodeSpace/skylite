module github.com/RandomCodeSpace/skylite/driver/acceptance

go 1.26.0

require (
	github.com/RandomCodeSpace/skylite v0.0.0
	github.com/go-jose/go-jose/v4 v4.1.9
	gorm.io/driver/sqlite v1.6.0
	gorm.io/gorm v1.31.2
)

require (
	github.com/go-json-experiment/json v1.27.2 // indirect
	github.com/jinzhu/inflection v1.0.0 // indirect
	github.com/jinzhu/now v1.1.5 // indirect
	github.com/mattn/go-sqlite3 v1.14.22 // indirect
	golang.org/x/text v0.43.0 // indirect
)

replace github.com/RandomCodeSpace/skylite => ../..

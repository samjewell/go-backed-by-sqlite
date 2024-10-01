package main

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/mattn/go-sqlite3"
)

func main() {
	db, err := sql.Open("sqlite3", "./example.db")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Optimize performance
	_, err = db.Exec("PRAGMA synchronous = OFF")
	if err != nil {
		log.Fatal(err)
	}
	_, err = db.Exec("PRAGMA journal_mode = MEMORY")
	if err != nil {
		log.Fatal(err)
	}

	// Destroy table if exists
	_, err = db.Exec("DROP TABLE IF EXISTS full_names")
	if err != nil {
		log.Fatal(err)
	}

	// Create table
	createTableSQL := `CREATE TABLE IF NOT EXISTS full_names (
	      column1 INTEGER PRIMARY KEY AUTOINCREMENT,
	      column2 TEXT NOT NULL,
	      column3 TEXT NOT NULL
	  );`
	_, err = db.Exec(createTableSQL)
	if err != nil {
		log.Fatal(err)
	}

	// // Insert data from memory
	// _, err = db.Exec(`INSERT INTO example VALUES (1, 'Alex'), (2, 'Anna')`)
	// if err != nil {
	// 	log.Fatal(err)
	// }

	// Begin transaction
	tx, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}

	// Prepare statement for inserting data
	stmt, err := tx.Prepare("INSERT INTO full_names (column1, column2, column3) VALUES (?, ?, ?)")
	if err != nil {
		log.Fatal(err)
	}
	defer stmt.Close()

	// create dummy data
	type Row struct {
		Column1 int
		Column2 string
		Column3 string
	}
	yourData := []Row{
		{1, "Alex", "Smith"},
		{2, "Anna", "Johnson"},
		{3, "John", "Doe"},
		{4, "Mike", "Smith"},
		{5, "Tom", "Johnson"},
		{6, "Jim", "Doe"},
		{7, "Alex", "Smith"},
		{8, "Anna", "Johnson"},
		{9, "John", "Doe"},
		{10, "Mike", "Smith"},
		{11, "Tom", "Johnson"},
		{12, "Jim", "Doe"},
	}

	// Insert data in batches
	batchSize := 10
	for i := 0; i < len(yourData); i += batchSize {
		end := i + batchSize
		if end > len(yourData) {
			end = len(yourData)
		}

		for _, row := range yourData[i:end] {
			_, err = stmt.Exec(row.Column1, row.Column2, row.Column3)
			if err != nil {
				tx.Rollback()
				log.Fatal(err)
			}
		}
	}

	// Commit transaction
	err = tx.Commit()
	if err != nil {
		log.Fatal(err)
	}

	// Query data
	rows, err := db.Query(`SELECT * FROM full_names LIMIT 2`)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		// print the values of the current row
		var id int
		var name1 string
		var name2 string
		err = rows.Scan(&id, &name1, &name2)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(id, name1, name2)
	}
}

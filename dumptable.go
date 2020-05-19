package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"os"
)

func main() {
	args := os.Args
	if len(args) != 4 {
		fmt.Fprintf(os.Stderr, "usage: %s <database_type> <connect_string> <table> [>filename.csv]\n", args[0])
		return
	}
	db, err := sql.Open(args[1], args[2])
	if err != nil {
		fmt.Fprintf(os.Stderr, " ** ERROR: %s\n", err.Error())
		return
	}
	defer db.Close()
	query := "select * from " + args[3]
	rows, err := db.Query(query)
	if err != nil {
		fmt.Fprintf(os.Stderr, " ** ERROR: %s on %s\n", err.Error(), query)
		return
	}
	defer rows.Close()
	var cols []string
	var rec []string
	var values []interface{}
	cols, _ = rows.Columns()
	count := len(cols)
	rec = make([]string, count)
	values = make([]interface{}, count)
	for i := 0; i < count; i++ {
		values[i] = new(sql.RawBytes)
	}
	w := csv.NewWriter(os.Stdout)
	err = w.Write(cols)
	for rows.Next() {
		err := rows.Scan(values...)
		if err == nil {
			for i, _ := range cols {
				rec[i] = string(*values[i].(*sql.RawBytes))
			}
		} else {
			fmt.Fprintf(os.Stderr, " ** ERROR: %s\n", err.Error())
		}
		err = w.Write(rec)
	}
	w.Flush()
}

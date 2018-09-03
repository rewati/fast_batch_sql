package main

import (
	"bufio"
	"database/sql"
	"fmt"
	"log"
	"net/url"
	"os"
	"strconv"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	"github.com/rmulley/go-fast-sql"
)

func main() {
	// writeTestData("testdata.txt")
	readFromFileAndPrint()
	// var name = "db15"
	// // create(name, name)
	// start := time.Now().UnixNano() / int64(time.Millisecond)
	// batchTestInsert(name, name)
	// end := time.Now().UnixNano() / int64(time.Millisecond)
	// fmt.Printf("Time to execute: %v\n", (end-start)/1000)
	// println(countRows(name))
}

func batchTestInsert(dbName string, tableName string) {
	var (
		err error
		i   uint = 1
		dbh *fastsql.DB
	)

	// Create new FastSQL DB object with a flush-interval of 100 rows
	if dbh, err = fastsql.Open("mysql", "root:happy@tcp(localhost:3306)/"+tableName+"?"+url.QueryEscape("charset=utf8mb4,utf8&loc=America/New_York"), 1000); err != nil {
		log.Fatalln(err)
	}
	defer dbh.Close()

	// Some loop performing SQL INSERTs
	for i <= 5000000 {
		if err = dbh.BatchInsert("INSERT INTO "+tableName+"(id, id2, id3) VALUES(?, ?, ?);", i, i+1, i+2); err != nil {
			log.Fatalln(err)
		}

		i++
	}

}

func create(dbName string, tableName string) {

	db, err := sql.Open("mysql", "root:happy@tcp(127.0.0.1:3306)/")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE DATABASE " + dbName)
	if err != nil {
		panic(err)
	}

	_, err = db.Exec("USE " + dbName)
	if err != nil {
		panic(err)
	}

	_, err = db.Exec("CREATE TABLE " + tableName + " ( id integer, id2 integer,id3 integer )")
	if err != nil {
		panic(err)
	}
}

func countRows(tableName string) int {
	db, err := sql.Open("mysql", "root:happy@tcp(127.0.0.1:3306)/"+tableName)
	if err != nil {
		panic(err.Error()) // Just for example purpose. You should use proper error handling instead of panic
	}
	defer db.Close()

	rows, err := db.Query("SELECT COUNT(*) as count FROM  " + tableName)
	checkErr(err)
	return checkCount(rows)
}

func checkCount(rows *sql.Rows) (count int) {
	for rows.Next() {
		err := rows.Scan(&count)
		checkErr(err)
	}
	return count
}

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}

func startReadingFileLineByLine(filePath string, ch chan lineOutPut) {
	file, err := os.Open(filePath)
	if err != nil {
		ch <- lineOutPut{Err: err}
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	split := func(data []byte, atEOF bool) (advance int, token []byte, err error) {
		if atEOF == true {
			ch <- lineOutPut{EOF: true}
		} else {
			ch <- lineOutPut{Line: string(data)}

		}
		return 0, nil, nil
	}
	scanner.Split(split)
	buf := make([]byte, 0, 1024*1024)
	scanner.Buffer(buf, 100024*100024)
	for scanner.Scan() {
		ch <- lineOutPut{Line: scanner.Text()}
	}

	if err := scanner.Err(); err != nil {
		ch <- lineOutPut{Err: err}
	}
}

type lineOutPut struct {
	Line string
	Err  error
	EOF  bool
}

func readFromFileAndPrint() {
	var (
		err error
		dbh *fastsql.DB
	)
	// Create new FastSQL DB object with a flush-interval of 100 rows
	if dbh, err = fastsql.Open("mysql", "root:happy@tcp(localhost:3306)/db15?"+url.QueryEscape("charset=utf8mb4,utf8&loc=America/New_York"), 1000); err != nil {
		log.Fatalln(err)
	}
	defer dbh.Close()
	ch := make(chan lineOutPut)
	go startReadingFileLineByLine("testdata.txt", ch)
	for msg := range ch {
		if msg.EOF {
			break
		}
		if msg.Err != nil {
			log.Printf("Error: %v", msg.Err)
		} else {

			s := strings.Split(msg.Line, ",")
			a, _ := strconv.Atoi(s[0])
			b, _ := strconv.Atoi(s[1])
			c, _ := strconv.Atoi(s[2])
			if err = dbh.BatchInsert("INSERT INTO db15(id, id2, id3) VALUES(?, ?, ?);", a, b, c); err != nil {
				log.Fatalln(err)
			}
			println(msg.Line)
		}

	}
}

func writeTestData(path string) {
	file, err := os.Create(path)
	if err != nil {
		log.Panicf("Error while writing test data %v", err)
	}
	defer file.Close()

	w := bufio.NewWriter(file)
	var i uint = 1
	for i <= 15000000 {
		fmt.Fprintln(w, fmt.Sprintf("%v,%v,%v", i, i+1, i+2))
		i++
	}
	w.Flush()
}

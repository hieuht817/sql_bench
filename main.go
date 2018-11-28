package main

import (
	"bitbucket.org/alanmbc/traxclix/utils/log"
	"flag"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/lann/squirrel"
	"math/rand"
	"sync"
	"time"
)

var query *string
var row_count *int
var concr *int
var connection_string *string
var dbms *string

//var db = testing_db.NewDevDbx()
//var db, _ = sql.Open("mysql", fmt.Sprintf("alan:12345678@tcp(%s)/traxclix?charset=utf8&parseTime=True", host))
var db *sqlx.DB

func main() {


	row_count = flag.Int("r", 1000, "row count")
	concr = flag.Int("c", 1, "concurrent")
	connection_string = flag.String("h",
			"traxclixdev:123456@tcp(moco-dev-db.czb1d4ixderm.ap-southeast-1.rds.amazonaws.com:3306)/traxclix?charset=utf8&parseTime=True",
		"db connection string")

	dbms = flag.String("db", "mysql", "dbms name")
	query = flag.String("query", "insert into test_bench(s1, s2, s3) value(?, ?, ?);", "dbms name")

	flag.Parse()

	db = sqlx.MustOpen(*dbms, *connection_string)

	fmt.Println(db)
	fmt.Println(db.DB)
	queue := make(chan []interface{}, *concr)

	wg := &sync.WaitGroup{}
	wg.Add(*concr + 1)

	go func() {
		GenRowValues(*row_count, queue)
		wg.Done()
	}()


	start := time.Now()
	for i := 0; i < *concr; i++ {
		go func() {
			InsertWorker(queue)
			wg.Done()
		}()
	}

	wg.Wait()

	length := time.Since(start)

	log.Debug(start.String(), length.String())

}

func queryAndArgs(queue chan []interface{}) (string, []interface{}) {
	// insert into test_bench(s1, s2, s3) value(?, ?, ?);
	query_builder := squirrel.Insert("test_bench").Columns("s1", "s2", "s3")


	for i := 0; i < 1000; i++ {
		v := <- queue

		query_builder.Values(v[0], v[1], v[2])
	}

	q, args, _ := query_builder.ToSql()

	return q, args
}

func InsertWorker(queue chan []interface{}) {
	for {
		tx, err := db.Begin()
		if err != nil {
			log.Error(err)
			continue
		}

		q, args := queryAndArgs(queue)


		_, err = tx.Exec(q, args...)

		if err != nil {
			log.Error(err)
			tx.Rollback()
		} else {
			tx.Commit()
		}
	}
}

func GenRowValues(row_count int, c chan []interface{}) {
	for i := 0; i < row_count/1000; i++ {
		v := randomValues(100, 150, 250)
		for j:=0; j<1000; j++ {
			c <- v
		}
	}
	close(c)
}

func randomValues(l1, l2, l3 int) []interface{} {
	return []interface{}{
		randomString(l1),
		randomString(l2),
		randomString(l3),
	}
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randomString(length int) string {
	b := make([]rune, length)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

func init() {
	rand.Seed(time.Now().UnixNano())
}

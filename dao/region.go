//package dao
package main

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"log"
	"time"
)

func getRegionNumberByTableName(tableName *string) int {
	var num int = -1
	// Open connection of mysql
	db, err := sql.Open("mysql", "hbase:hbase@/hbase?charset=utf8")
	if err != nil {
		log.Fatalf("Open database error: %s\n", err)
		panic(err.Error())
	}
	defer db.Close()

	// Prepare statement for reading data
	stmt, err := db.Prepare("SELECT count(*) FROM hbase.regions where region_name like '" + *tableName + "%'")
	fmt.Println(db.Stats())

	if err != nil {
		panic(err.Error()) // proper error handling instead of panic in your app
		//log.Fatalf("Select error: %s\n", err)
	}
	defer stmt.Close()

	// Query another number.. 1 maybe?
	//err = stmt.QueryRow(1).Scan(&num) // WHERE number = 1
	rows, _ := stmt.Query()
	for rows.Next() {
		rows.Scan(&num)
	}

	if err != nil {
		panic(err.Error()) // proper error handling instead of panic in your app
		//log.Fatalf("Select error: %s\n", err)
	}
	fmt.Printf("The number is: %d\n", num)

	return num
}

func getTotalNumber() int {
	var num int = -1
	db, err := sql.Open("mysql", "hbase:hbase@/hbase?charset=utf8")
	if nil != err {
		panic(err.Error())
	}
	defer db.Close()

	stmt, err := db.Prepare("SELECT COUNT(*) FROM hbase.regions")
	if nil != err {
		panic(err.Error())
	}
	defer stmt.Close()

	rows, err := stmt.Query()
	for rows.Next() {
		rows.Scan(&num) //使用指针，直接修改在内存中对象的值

	}
	fmt.Printf("Total num is: %d\n", num)
	return num
}

//list()

func list() []Region {
	db, err := sql.Open("mysql", "hbase:hbase@/hbase?charset=utf8")
	if nil != err {
		panic(err.Error())
	}
	stmt, err := db.Prepare("select * from hbase.regions")
	if nil != err {
		panic(err.Error())
	}

	rows, err := stmt.Query()
	if nil != err {
		panic(err.Error())
	}

	var regions []Region

	for rows.Next() {
		var id int
		var regionName string
		var updateTime string

		rows.Scan(&id, &regionName, &updateTime)

		var region Region
		region.id = id
		region.regionName = regionName

		region.updateTime, err = time.Parse("2006-01-02 15:04:05", updateTime)
		if nil != err {
			panic(err.Error())
		}

		regions = append(regions, region)

		fmt.Println(region)
		fmt.Println(id, regionName, updateTime)

	}

	return regions

}

func truncate() {
	db, err := sql.Open("mysql", "hbase:hbase@/hbase?charset=utf8")
	if nil != err {
		panic(err.Error())
	}
	defer db.Close()

	stmt, err := db.Prepare("TRUNCATE TABLE hbase.regions")
	if nil != err {
		panic(err.Error())
	}
	defer stmt.Close()

	_, err = stmt.Exec()
	if nil != err {
		panic(err.Error())
	}

}

type Region struct {
	id         int       `db:"id"`
	regionName string    `db:"region_name"`
	updateTime time.Time `db:"update_time"`
}

//needUpdate()




//batchUpdate()

func main() {
	var tableName string = "t1"
	var tn *string
	tn = &tableName
	getRegionNumberByTableName(tn)
	getTotalNumber()
	fmt.Println(list())

	

}

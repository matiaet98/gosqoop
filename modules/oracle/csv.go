package oracle

import (
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	log "github.com/sirupsen/logrus"
	goracle "gopkg.in/goracle.v2" //se abstrae su uso con la libreria sql
	"gosqoop/global"
	"os"
	"strconv"
	"strings"
	"sync"
)

var CsvFile *os.File

var Db *sql.DB
var Tx *sql.Tx

// DataToCsv - Obtiene los datos a grabar
func DataToCsv() {
	CsvFile, err := os.Create(global.Output)
	if err != nil {
		log.Fatalln(err.Error())
	}
	defer CsvFile.Close()
	Db, err = sql.Open("goracle", global.User+"/"+global.Password+"@//"+global.Connstring+"?poolWaitTimeout=0")
	if err != nil {
		log.Fatalln(err.Error())
	}
	defer Db.Close()
	if err != nil {
		log.Fatalln(err.Error())
	}
	rows, err := Db.Query(global.Query, goracle.FetchRowCount(global.FetchSize), goracle.ArraySize(global.FetchSize))
	if err != nil {
		log.Fatalln(err.Error())
	}
	defer rows.Close()
	columns, _ := rows.ColumnTypes()
	var types []string
	var colNames []string
	for x := range columns {
		colNames = append(colNames, columns[x].Name())
		types = append(types, columns[x].DatabaseTypeName())
	}
	writer := csv.NewWriter(CsvFile)
	writer.Write(colNames)
	defer writer.Flush()
	count := len(columns)
	values := make([]interface{}, count)
	valuePtrs := make([]interface{}, count)
	var finalValues [][]string
	var j int
	for j = 0; rows.Next(); j++ {
		for i := range columns {
			valuePtrs[i] = &values[i]
		}
		rows.Scan(valuePtrs...)
		finalValues = append(finalValues, convert(values, types))
		if j%global.FetchSize == 0 {
			writer.WriteAll(finalValues)
			finalValues = nil
		}
		writer.WriteAll(finalValues)
	}
}

// DataToCsvParallel - Obtiene los datos a grabar
func DataToCsvParallel() {
	var wg sync.WaitGroup
	CsvFile, err := os.Create(global.Output)
	if err != nil {
		log.Fatalln(err.Error())
	}
	defer CsvFile.Close()
	Db, err = sql.Open("goracle", global.User+"/"+global.Password+"@//"+global.Connstring+"?poolWaitTimeout=0")
	if err != nil {
		log.Fatalln(err.Error())
	}
	defer Db.Close()
	if err != nil {
		log.Fatalln(err.Error())
	}
	Tx, _ = Db.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true})
	defer Tx.Commit()
	var max interface{}
	var min interface{}
	min, max = getMaxAndMin()
	colType, nullable := getColType()
	var queries []string
	switch colType {
	case "NUMBER":
		rmin, _ := strconv.Atoi(string(min.(goracle.Number)))
		rmax, _ := strconv.Atoi(string(max.(goracle.Number)))
		ct := getCotasFromInts(rmin, rmax)
		for x := range ct {
			var st string
			if nullable && x == 0 {
				st = fmt.Sprintf(" ((%v >= %v AND %v <= %v) OR %v is NULL) ", global.Pcolumn, ct[x].min, global.Pcolumn, ct[x].max, global.Pcolumn)
			} else {
				st = fmt.Sprintf(" (%v >= %v AND %v <= %v) ", global.Pcolumn, ct[x].min, global.Pcolumn, ct[x].max)
			}
			q := strings.ReplaceAll(global.OriginalQuery, "##CONDITIONS", st)
			queries = append(queries, q)
		}
		log.Infoln("Queries a realizar:")
		cols, types := getCols(global.Query)
		writer := csv.NewWriter(CsvFile)
		writer.Write(cols)
		writer.Flush()
		for x := range queries {
			log.Infof("%v\n", queries[x])
			wg.Add(1)
			go getValues(queries[x], &wg, &types)
		}
		break
	case "DATE", "TIMESTAMP":
		break
	default:
		log.Fatalln("No se puede paralelizar por esa columna por no ser del tipo correcto (solo se puede por numerico o fecha)")
	}
	wg.Wait()
}

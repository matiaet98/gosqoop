package oracle

import (
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
	"time"
)

var Db *sql.DB

// DataToCsv - Obtiene los datos a grabar
func DataToCsv() {
	file, err := os.Create(global.Output)
	if err != nil {
		log.Fatalln(err.Error())
	}
	defer file.Close()
	writer := csv.NewWriter(file)
	defer writer.Flush()
	Db, err = sql.Open("goracle", global.User+"/"+global.Password+"@//"+global.Connstring+"?poolWaitTimeout=0&poolIncrement=5&poolSessionTimeout=0&poolSessionMaxLifetime=36000")
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
	log.Println("Columnas:")
	var types []string
	var colNames []string
	for x := range columns {
		fmt.Println(columns[x].Name())
		colNames = append(colNames, columns[x].Name())
		types = append(types, columns[x].DatabaseTypeName())
	}
	count := len(columns)
	values := make([]interface{}, count)
	valuePtrs := make([]interface{}, count)
	var finalValues [][]string
	for rows.Next() {
		for i := range columns {
			valuePtrs[i] = &values[i]
		}
		rows.Scan(valuePtrs...)
		finalValues = append(finalValues, convert(values, types))
	}
	writer.Write(colNames)
	writer.WriteAll(finalValues)
}

// DataToCsvParallel - Obtiene los datos a grabar
func DataToCsvParallel() {
	var wg sync.WaitGroup
	file, err := os.Create(global.Output)
	if err != nil {
		log.Fatalln(err.Error())
	}
	defer file.Close()
	writer := csv.NewWriter(file)
	defer writer.Flush()
	Db, err = sql.Open("goracle", global.User+"/"+global.Password+"@//"+global.Connstring+"?poolWaitTimeout=0")
	if err != nil {
		log.Fatalln(err.Error())
	}
	defer Db.Close()
	if err != nil {
		log.Fatalln(err.Error())
	}
	var max interface{}
	var min interface{}
	min, max = getMaxAndMin()
	colType, nullable := getColType()
	var queries []string
	var cols []string
	var vals [][]string
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
		for x := range queries {
			log.Infof("%v\n", queries[x])
			wg.Add(1)
			go getValues(queries[x], &wg, &cols, &vals)
		}
		break
	case "DATE", "TIMESTAMP":
		break
	default:
		log.Fatalln("No se puede paralelizar por esa columna por no ser del tipo correcto (solo se puede por numerico o fecha)")
	}
	wg.Wait()
	writer.Write(cols)
	writer.WriteAll(vals)
}

func getValues(query string, wg *sync.WaitGroup, cols *[]string, vals *[][]string) {
	defer wg.Done()
	rows, err := Db.Query(query, goracle.FetchRowCount(global.FetchSize), goracle.ArraySize(global.FetchSize))
	if err != nil {
		log.Fatalln(err.Error())
	}
	defer rows.Close()
	columns, _ := rows.ColumnTypes()
	var types []string
	if len(*cols) == 0 {
		for x := range columns {
			*cols = append(*cols, columns[x].Name())
			types = append(types, columns[x].DatabaseTypeName())
		}
	} else {
		for x := range columns {
			types = append(types, columns[x].DatabaseTypeName())
		}
	}
	count := len(columns)
	values := make([]interface{}, count)
	valuePtrs := make([]interface{}, count)
	for rows.Next() {
		for i := range columns {
			valuePtrs[i] = &values[i]
		}
		rows.Scan(valuePtrs...)
		*vals = append(*vals, convert(values, types))
	}
}

func convert(dest []interface{}, types []string) []string {
	var arr []string
	for x := range dest {
		if dest[x] == nil {
			arr = append(arr, "NULL")
			continue
		}
		switch types[x] {
		case "DATE":
			arr = append(arr, dest[x].(time.Time).Format("2006-01-02"))
			break
		case "TIMESTAMP":
			arr = append(arr, dest[x].(time.Time).Format("2006-01-02 00:01:02"))
			break
		case "NUMBER", "BINARY_FLOAT", "BINARY_DOUBLE":
			arr = append(arr, string(dest[x].(goracle.Number)))
			break
		case "CHAR", "VARCHAR2", "NCHAR", "NVARCHAR2", "CLOB", "NCLOB", "LONG":
			arr = append(arr, dest[x].(string))
			break
		default:
			log.Fatalln("Se recibio un tipo de dato no reconocido: " + types[x])
			break
		}
	}
	return arr
}

func getMaxAndMin() (min interface{}, max interface{}) {
	log.Infoln("Obteniendo cotas para columna " + global.Pcolumn)
	q1 := fmt.Sprintf("select min(t.%s) from (%s) t", global.Pcolumn, global.Query)
	q2 := fmt.Sprintf("select max(t.%s) from (%s) t", global.Pcolumn, global.Query)
	_ = Db.QueryRow(q1, goracle.FetchRowCount(1), goracle.ArraySize(1)).Scan(&min)
	_ = Db.QueryRow(q2, goracle.FetchRowCount(1), goracle.ArraySize(1)).Scan(&max)
	log.Infof("Minimo: %v Maximo: %v", min, max)
	return
}

func getColType() (string, bool) {
	log.Infoln("Obteniendo tipo de la columna")
	q1 := fmt.Sprintf("select t.%s from (%s) t where rownum = 1", global.Pcolumn, global.Query)
	rows, err := Db.Query(q1, goracle.FetchRowCount(1), goracle.ArraySize(1))
	if err != nil {
		log.Fatalln(err)
	}
	coltype, err := rows.ColumnTypes()
	if err != nil {
		log.Fatalln(err)
	}
	log.Infof("Tipo de la columna: %v", coltype[0].DatabaseTypeName())
	nu, ok := coltype[0].Nullable()
	if !ok {
		nu = false
	}
	return coltype[0].DatabaseTypeName(), nu
}

type cotas struct {
	min int
	max int
}

func getCotasFromInts(min int, max int) []cotas {
	cant := max - min
	part := cant / global.Plevel
	var ct []cotas
	var i int = 0
	for x := min; x <= max; x++ {
		lmin := x
		x = x + part
		lmax := x
		ct = append(ct, cotas{min: lmin, max: lmax})
		i++
	}
	ct[len(ct)-1].max = max
	return ct
}

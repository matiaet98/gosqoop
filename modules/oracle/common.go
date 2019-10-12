package oracle

import (
	"encoding/csv"
	"fmt"
	log "github.com/sirupsen/logrus"
	goracle "gopkg.in/goracle.v2" //se abstrae su uso con la libreria sql
	"gosqoop/global"
	"sync"
	"time"
)

func getCols(query string) (cols []string, types []string) {
	rows, err := Tx.Query(query, goracle.FetchRowCount(1), goracle.ArraySize(1))
	if err != nil {
		log.Fatalln(err.Error())
	}
	defer rows.Close()
	columns, _ := rows.ColumnTypes()
	for x := range columns {
		cols = append(cols, columns[x].Name())
		types = append(types, columns[x].DatabaseTypeName())
	}
	return
}

func getValues(query string, wg *sync.WaitGroup, types *[]string) {
	defer wg.Done()
	writer := csv.NewWriter(CsvFile)
	defer writer.Flush()
	var vals [][]string
	rows, err := Tx.Query(query, goracle.FetchRowCount(global.FetchSize), goracle.ArraySize(global.FetchSize))
	if err != nil {
		log.Fatalln(err.Error())
	}
	defer rows.Close()
	columns, _ := rows.ColumnTypes()
	count := len(columns)
	values := make([]interface{}, count)
	valuePtrs := make([]interface{}, count)
	var j int
	for j = 0; rows.Next(); j++ {
		for i := range columns {
			valuePtrs[i] = &values[i]
		}
		rows.Scan(valuePtrs...)
		vals = append(vals, convert(values, *types))
		if j%global.FetchSize == 0 {
			writer.WriteAll(vals)
			vals = nil
		}
	}
	writer.WriteAll(vals)
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
	_ = Tx.QueryRow(q1, goracle.FetchRowCount(1), goracle.ArraySize(1)).Scan(&min)
	_ = Tx.QueryRow(q2, goracle.FetchRowCount(1), goracle.ArraySize(1)).Scan(&max)
	log.Infof("Minimo: %v Maximo: %v", min, max)
	return
}

func getColType() (string, bool) {
	log.Infoln("Obteniendo tipo de la columna")
	q1 := fmt.Sprintf("select t.%s from (%s) t where rownum = 1", global.Pcolumn, global.Query)
	rows, err := Tx.Query(q1, goracle.FetchRowCount(1), goracle.ArraySize(1))
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

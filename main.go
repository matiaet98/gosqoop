package main

import (
	"flag"
	log "github.com/sirupsen/logrus"
	"gosqoop/global"
	"gosqoop/readers/oracle"
	"gosqoop/validations"
	"os"
	"strconv"
	"strings"
)

func init() {
	log.SetOutput(os.Stdout)
	log.SetLevel(log.InfoLevel)
	log.SetFormatter(&log.TextFormatter{
		FullTimestamp:          true,
		ForceColors:            true,
		DisableLevelTruncation: true,
		TimestampFormat:        "2006-01-02 15:04:05",
	})
}

func main() {
	getOpts()
	printOpts()
	validations.Common()
	defer log.Infof("All jobs finished")
	switch global.Source {
	case "oracle":
		switch global.Format {
		case "csv":
			if !global.Parallel {
				log.Info("Se corre sin paralelo")
				oracle.DataToCsv()
			} else {
				log.Info("Se corre en paralelo")
				global.Query = strings.ReplaceAll(global.Query, "##CONDITIONS", " 1 = 1 ")
				oracle.DataToCsvParallel()
			}
			break
		}
		break
	}
}

func getOpts() {
	flag.StringVar(&global.Source, "source", "oracle", "tipo base de datos de origen")
	flag.StringVar(&global.Dest, "dest", "local", "plataforma destino. Actualmente se soporta local y hdfs")
	flag.StringVar(&global.Connstring, "connstring", "", "connection string de la base de datos de origen")
	flag.StringVar(&global.User, "user", "", "usuario de la base de datos de origen")
	flag.StringVar(&global.Password, "password", "", "password para el usuario proporcionado")
	flag.StringVar(&global.Format, "format", "csv", "formato del archivo destino")
	flag.StringVar(&global.Query, "query", "", "query a realizar en la base de origen. Si se paraleliza debe tener la variable ##CONDITIONS en el where")
	flag.IntVar(&global.FetchSize, "fetchsize", 300, "Cantidad de Rows por fetch. En caso de paralelizar, se multiplica por la cantidad de plevel")
	flag.StringVar(&global.Output, "output", "", "path y nombre del archivo/tabla de salida")
	flag.BoolVar(&global.Parallel, "parallel", false, "si se paraleliza o no el fetch")
	flag.StringVar(&global.Pcolumn, "pcolumn", "", "columna por la cual paralelizar")
	flag.IntVar(&global.Plevel, "plevel", 0, "cantidad de hilos en que se paraleliza")
	flag.Parse()
	global.OriginalQuery = global.Query
}

func printOpts() {
	log.Infoln("Parametros recibidos:")
	log.Infoln("Source: " + global.Source)
	log.Infoln("Dest: " + global.Dest)
	log.Infoln("Connstring: " + global.Connstring)
	log.Infoln("User: " + global.User)
	log.Infoln("Password: " + global.Password)
	log.Infoln("Format: " + global.Format)
	log.Infoln("Query: " + global.Query)
	log.Infoln("FetchSize: " + strconv.Itoa(global.FetchSize))
	log.Infoln("Output: " + global.Output)
	log.Infoln("Parallel: " + strconv.FormatBool(global.Parallel))
	log.Infoln("Pcolumn: " + global.Pcolumn)
	log.Infoln("Plevel: " + strconv.Itoa(global.Plevel))
}

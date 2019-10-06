package validations

import (
	log "github.com/sirupsen/logrus"
	"gosqoop/global"
	"strings"
)

func Common() {
	if global.Connstring == "" {
		log.Fatalln("connstring no puede ser nulo")
	}
	if global.User == "" {
		log.Fatalln("Usuario no puede ser nulo")
	}
	if global.Password == "" {
		log.Fatalln("Password no puede ser nulo")
	}
	if global.Query == "" {
		log.Fatalln("query no puede ser nula")
	}
	if global.Output == "" {
		log.Fatalln("output no puede ser nulo")
	}
	checkParallel()
}

func checkParallel() {
	if global.Parallel {
		if global.Pcolumn == "" || global.Plevel == 0 {
			log.Fatalln("En modo paralelo deben pasarse las variables Pcolumn y Plevel")
		}
		if !strings.Contains(global.Query, "##CONDITIONS") {
			log.Fatalf(`La query especificada debe tener la condicion ##CONDITIONS cuando se utiliza el paralelo. ej:
"select * from mytable where param = val ##CONDITIONS and param2 = val2"
			`)
		}
	}
}

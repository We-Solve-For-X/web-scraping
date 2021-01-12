package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"sync"
	"time"

	_ "github.com/lib/pq"
	uuid "github.com/satori/go.uuid"
	"golang.org/x/sync/semaphore"
)

// var maxAll uint64
// var m runtime.MemStats

// func PrintMemUsage() {

// 	runtime.ReadMemStats(&m)
// 	// For info on each, see: https://golang.org/pkg/runtime/#MemStats
// 	// fmt.Printf("Alloc = %v KiB", bToMb(m.Alloc))
// 	// fmt.Printf("\tSys = %v KiB", bToMb(m.Sys))
// 	fmt.Printf("\tNumGC = %v\n", m.NumGC)

// 	if m.Alloc > maxAll {
// 		maxAll = m.Alloc
// 	}
// 	log.Printf("\tMaxAl = %v\n", maxAll)

// }

type User struct {
	Name          string
	City          string
	Province      string
	Postcode      string
	Registrations []Registration
	Id            int
}

type Registration struct {
	Number         string
	Status         string
	IsACtive       bool
	Register       string
	Board          string
	Practice       []Practice
	Qualifications []Qualification
}

type Practice struct {
	Type          string
	Field         string
	Speciality    string
	SubSpeciality string
	Date          string
	Origin        string
}

type Qualification struct {
	Name string
	Date string
}

func userToDB(user *User, db *sql.DB) {
	_, err := db.Exec(`INSERT INTO users("name","city","province","postcode","user_id") VALUES	($1,$2,$3,$4,$5);`, (*user).Name, (*user).City, (*user).Province, (*user).Postcode, (*user).Id)
	if err != nil {
		log.Println("SQL Error: Failure writing user to DB with id - " + string((*user).Id))
	}
}

func registrationToDB(reg *Registration, regId *string, usrId *int, db *sql.DB) {
	_, err := db.Exec(`INSERT INTO registrations("number","status","isActive","register","board","user_id","reg_id") VALUES ($1,$2,$3,$4,$5,$6,$7);`, (*reg).Number, (*reg).Status, (*reg).IsACtive, (*reg).Register, (*reg).Board, *usrId, *regId)
	if err != nil {
		log.Println(err)
		log.Println("SQL Error: Failure writing user to DB with id - " + *regId)
	}
}

func practiceToDB(practice *Practice, regId *string, db *sql.DB) {
	_, err := db.Exec(`INSERT INTO practices("type","field","speciality","sub_speciality","date","origin","reg_id") VALUES ($1,$2,$3,$4,$5,$6,$7);`, (*practice).Type, (*practice).Field, (*practice).Speciality, (*practice).SubSpeciality, (*practice).Date, (*practice).Origin, *regId)
	if err != nil {
		log.Println(err)
		log.Println("SQL Error: Failure writing practice to DB with reg id - " + *regId)
	}
}

func qualificationToDB(qualification *Qualification, regId *string, db *sql.DB) {
	_, err := db.Exec(`INSERT INTO qualifications("name","date","reg_id") VALUES ($1,$2,$3);`, (*qualification).Name, (*qualification).Date, *regId)
	if err != nil {
		log.Println(err)
		log.Println("SQL Error: Failure writing qualification to DB with reg id - " + *regId)
	}
}

func writeQualifications(qualifs *[]Qualification, regId *string, db *sql.DB) {
	for _, qualification := range *qualifs {
		qualificationToDB(&qualification, regId, db)
		qualification = Qualification{}
	}
	qualifs = nil
	return
}

func writeRegistrations(regs *[]Registration, usrId int, db *sql.DB) {
	for _, registration := range *regs {
		var regId = uuid.Must(uuid.NewV4()).String()

		registrationToDB(&registration, &regId, &usrId, db)
		for _, prac := range registration.Practice {
			practiceToDB(&prac, &regId, db)
			prac = Practice{}
		}
		writeQualifications(&registration.Qualifications, &regId, db)
		regId = ""
	}

	regs = nil
	return
}

func decodeUsers(userBytes *[]byte, users *[]User) {
	json.Unmarshal(*userBytes, users)
}

func getFromFile(fileName string, bytes *[]byte) {
	file, err := os.Open(fileName)

	if err != nil {
		log.Println(err)
	}
	defer file.Close()

	tempBytes, err := ioutil.ReadAll(file)
	(*bytes) = tempBytes
	tempBytes = nil
}

func threadProcessUser(userIn *User, db *sql.DB, wg *sync.WaitGroup, sem *semaphore.Weighted) {
	defer wg.Done()

	userToDB(userIn, db)
	writeRegistrations(&(*userIn).Registrations, (*userIn).Id, db)

	userIn = nil
	sem.Release(1)
}

func loopUsers(users *[]User, db *sql.DB, maxThreads *int) {

	start := time.Now()
	ctx := context.Background()
	var sem = semaphore.NewWeighted(int64(*maxThreads))
	var wg sync.WaitGroup
	wg.Add(len(*users))

	log.Println("Starting users-loop")
	for index, _ := range *users {
		if err := sem.Acquire(ctx, 1); err != nil {
			log.Println("Semaphore error ccoured")
		}
		go threadProcessUser(&(*users)[index], db, &wg, sem)
	}

	wg.Wait()
	fmt.Println("time: " + time.Since(start).String())
}

func main() {

	//extract cmd flags
	var fileName string
	flag.StringVar(&fileName, "fileName", "medProfiles.txt", "filename from which to extract users")
	var dbUser string
	flag.StringVar(&dbUser, "dbUser", "grafana", "PG user to be used")
	var dbName string
	flag.StringVar(&dbName, "dbName", "grafana", "PG database name to connect to")
	var dbPasword string
	flag.StringVar(&dbPasword, "dbPasword", "grafana", "PG database pasword to use")
	var maxThreads int
	flag.IntVar(&maxThreads, "maxThreads", 15, "Maximum goroutines to use during concurency")
	flag.Parse()

	//start DB connection
	connStr := "user=" + dbUser + " dbname=" + dbName + " password=" + dbPasword + " host=localhost port=5432 sslmode=disable"
	log.Println("Starting with flags: " + connStr)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Println("Error: Problem with database connection string")
	}

	err = db.Ping()
	if err != nil {
		log.Println("Error: Could not establish a connection with the database")
	}

	db.Begin()
	log.Println("DB Connected")

	//fetch data from file as bytes
	var usersText []byte
	getFromFile(fileName, &usersText)
	log.Println("Data fetched from file")

	//unmarshal to users
	var users = []User{}
	decodeUsers(&usersText, &users)
	usersText = []byte{}
	log.Println("Decoded users")

	//process users
	loopUsers(&users, db, &maxThreads)

	log.Println("Proccess success")
}

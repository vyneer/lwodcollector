package config

import (
	"context"
	"database/sql"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"

	"github.com/joho/godotenv"
	log "github.com/vyneer/lwodcollector/logger"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/drive/v3"
	"google.golang.org/api/option"
	"gopkg.in/Iwark/spreadsheet.v2"
)

type DBConfig struct {
	DB         *sql.DB
	Statements Statements
}

type Statements struct {
	SelectYTHashStmt       *sql.Stmt
	SelectYTByHashStmt     *sql.Stmt
	DeleteYTStmt           *sql.Stmt
	SelectTwitchHashStmt   *sql.Stmt
	SelectTwitchByHashStmt *sql.Stmt
	DeleteTwitchStmt       *sql.Stmt
	InsertYTStmt           *sql.Stmt
	InsertTwitchStmt       *sql.Stmt
	InsertURLStmt          *sql.Stmt
}

type GoogleConfig struct {
	Drive  *drive.Service
	Sheets *spreadsheet.Service
}

type Flags struct {
	Verbose    bool
	AllSheets  bool
	Continuous int
}

type Config struct {
	GoogleCred   string
	DBFile       string
	Folder       string
	HealthCheck  string
	Delay        int
	Refresh      int
	Flags        Flags
	DBConfig     DBConfig
	GoogleConfig GoogleConfig
}

const sqlCreateMain string = `CREATE TABLE IF NOT EXISTS lwod (
	dateadded text,
	datestreamed text,
	vodid text, 
	vidid text, 
	starttime text, 
	endtime text, 
	yttime text,
	game text, 
	subject text, 
	topic text,
	FOREIGN KEY (vodid)
		REFERENCES twitch(id)
		ON DELETE CASCADE,
	FOREIGN KEY (vidid)
		REFERENCES youtube(id)
		ON DELETE CASCADE
);`

const sqlCreateTwitch string = `CREATE TABLE IF NOT EXISTS twitch (
	id text, 
	hash text,
	PRIMARY KEY (id)
);`

const sqlCreateYouTube string = `CREATE TABLE IF NOT EXISTS youtube (
	id text, 
	hash text,
	PRIMARY KEY (id)
);`

const sqlCreateLink string = `CREATE TABLE IF NOT EXISTS lwodUrl (
	date text primary key, 
	sheetId text
);`

var ctx context.Context

func LoadDotEnv() Config {
	var err error
	var cfg Config

	log.Debugf("Loading environment variables")
	godotenv.Load()

	cfg.GoogleCred = os.Getenv("GOOGLE_CRED")
	if cfg.GoogleCred == "" {
		log.Fatalf("Please set the GOOGLE_CRED environment variable and restart the app")
	}
	cfg.DBFile = os.Getenv("DB_FILE")
	if cfg.DBFile == "" {
		log.Fatalf("Please set the DB_FILE environment variable and restart the app")
	}
	cfg.Folder = os.Getenv("LWOD_FOLDER")
	if cfg.Folder == "" {
		log.Fatalf("Please set the LWOD_FOLDER environment variable and restart the app")
	}
	cfg.HealthCheck = os.Getenv("HEALTHCHECK")
	delayStr := os.Getenv("DELAY")
	if delayStr == "" {
		log.Fatalf("Please set the DELAY environment variable and restart the app")
	}
	cfg.Delay, err = strconv.Atoi(delayStr)
	if err != nil {
		log.Fatalf("strconv error: %s", err)
	}
	refreshStr := os.Getenv("REFRESH")
	if refreshStr == "" {
		refreshStr = "0"
	}
	cfg.Refresh, err = strconv.Atoi(refreshStr)
	if err != nil {
		log.Fatalf("strconv error: %s", err)
	}

	log.Debugf("Environment variables loaded successfully")
	return cfg
}

func LoadDatabase(config *Config) {
	log.Debugf("Connecting to databases")
	dbpath := filepath.Join(".", "db")
	err := os.MkdirAll(dbpath, os.ModePerm)
	if err != nil {
		log.Fatalf("Error creating a db directory: %s", err)
	}

	dbpath = filepath.Join(".", "db", config.DBFile)
	config.DBConfig.DB, err = sql.Open("sqlite3", fmt.Sprintf("file:%s?_fk=true", dbpath))
	if err != nil {
		log.Fatalf("Error opening/creating lwoddb: %s", err)
	}

	if _, err := config.DBConfig.DB.Exec(sqlCreateYouTube); err != nil {
		log.Fatalf("Error creating the YouTube table: %s", err)
	}

	if _, err := config.DBConfig.DB.Exec(sqlCreateTwitch); err != nil {
		log.Fatalf("Error creating the Twitch table: %s", err)
	}

	if _, err := config.DBConfig.DB.Exec(sqlCreateMain); err != nil {
		log.Fatalf("Error creating the lwod table: %s", err)
	}

	if _, err := config.DBConfig.DB.Exec(sqlCreateLink); err != nil {
		log.Fatalf("Error creating the link table: %s", err)
	}

	config.DBConfig.Statements.SelectYTHashStmt, err = config.DBConfig.DB.Prepare("SELECT hash FROM youtube WHERE id = ? LIMIT 1")
	if err != nil {
		log.Fatalf("Error preparing a db statement: %s", err)
	}

	config.DBConfig.Statements.SelectYTByHashStmt, err = config.DBConfig.DB.Prepare("SELECT id FROM youtube WHERE hash = ? LIMIT 1")
	if err != nil {
		log.Fatalf("Error preparing a db statement: %s", err)
	}

	config.DBConfig.Statements.DeleteYTStmt, err = config.DBConfig.DB.Prepare("DELETE FROM youtube WHERE id = ?")
	if err != nil {
		log.Fatalf("Error preparing a db statement: %s", err)
	}

	config.DBConfig.Statements.SelectTwitchHashStmt, err = config.DBConfig.DB.Prepare("SELECT hash FROM twitch WHERE id = ? LIMIT 1")
	if err != nil {
		log.Fatalf("Error preparing a db statement: %s", err)
	}

	config.DBConfig.Statements.SelectTwitchByHashStmt, err = config.DBConfig.DB.Prepare("SELECT id FROM youtube WHERE hash = ? LIMIT 1")
	if err != nil {
		log.Fatalf("Error preparing a db statement: %s", err)
	}

	config.DBConfig.Statements.DeleteTwitchStmt, err = config.DBConfig.DB.Prepare("DELETE FROM twitch WHERE id = ?")
	if err != nil {
		log.Fatalf("Error preparing a db statement: %s", err)
	}

	config.DBConfig.Statements.InsertYTStmt, err = config.DBConfig.DB.Prepare("INSERT INTO youtube (id, hash) VALUES (?, ?)")
	if err != nil {
		log.Fatalf("Error preparing a db statement: %s", err)
	}

	config.DBConfig.Statements.InsertTwitchStmt, err = config.DBConfig.DB.Prepare("INSERT INTO twitch (id, hash) VALUES (?, ?)")
	if err != nil {
		log.Fatalf("Error preparing a db statement: %s", err)
	}

	config.DBConfig.Statements.InsertURLStmt, err = config.DBConfig.DB.Prepare("INSERT INTO lwodUrl (date, sheetId) VALUES (?, ?) ON CONFLICT DO NOTHING")
	if err != nil {
		log.Fatalf("Error preparing a db statement: %s", err)
	}

	log.Debugf("Connected to the database successfully")
}

func CreateGoogleClients(config *Config) {
	log.Debugf("Creating Google API clients")

	ctx := context.Background()

	credpath := filepath.Join(".", config.GoogleCred)
	b, err := ioutil.ReadFile(credpath)
	if err != nil {
		log.Fatalf("Unable to read client secret file: %v", err)
	}

	cfg, err := google.JWTConfigFromJSON(b, "https://spreadsheets.google.com/feeds https://www.googleapis.com/auth/drive")
	if err != nil {
		log.Fatalf("Unable to parse client secret file to config: %v", err)
	}
	client := cfg.Client(ctx)

	config.GoogleConfig.Sheets = spreadsheet.NewServiceWithClient(client)
	if err != nil {
		log.Fatalf("Unable to retrieve Sheets client: %v", err)
	}

	config.GoogleConfig.Drive, err = drive.NewService(ctx, option.WithHTTPClient(client))
	if err != nil {
		log.Fatalf("Unable to retrieve Drive client: %v", err)
	}

	log.Debugf("Created Google API clients successfully")
}

package config

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"github.com/joho/godotenv"
	log "github.com/vyneer/lwodcollector/logger"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/drive/v3"
	"google.golang.org/api/option"
	"google.golang.org/api/sheets/v4"
	"google.golang.org/api/youtube/v3"
)

type LWODDBConfig struct {
	DB         *sql.DB
	Statements LWODStatements
}

type YTDBConfig struct {
	DB         *sql.DB
	Statements YTStatements
}

type LWODStatements struct {
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

type YTStatements struct {
	SelectVods              *sql.Stmt
	GetPlaylistEtag         *sql.Stmt
	AddPlaylistEtag         *sql.Stmt
	GetLivestreamSearchEtag *sql.Stmt
	AddLivestreamSearchEtag *sql.Stmt
	ReplaceVod              *sql.Stmt
}

type GoogleConfig struct {
	Drive   *drive.Service
	Sheets  *sheets.Service
	YouTube *youtube.Service
}

type Flags struct {
	Verbose   bool
	AllSheets bool
	AllVideos bool
}

type Config struct {
	GoogleCred      string
	LWODDBFile      string
	YTDBFile        string
	LWODFolder      string
	YTChannel       string
	YTPlaylist      string
	LWODHealthCheck string
	YTHealthCheck   string
	MainPlatform    string
	LWODDelay       int
	LWODRefresh     int
	YTDelay         int
	YTRefresh       int
	YTAPIRefresh    int
	Continuous      bool
	Flags           Flags
	LWODDBConfig    LWODDBConfig
	YTDBConfig      YTDBConfig
	GoogleConfig    GoogleConfig
}

const sqlCreateMain string = `CREATE TABLE IF NOT EXISTS lwod (
	dateadded text,
	datestreamed text,
	vodid text, 
	vidid text, 
	starttime text, 
	endtime text, 
	yttime integer,
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

const sqlCreateVods string = `CREATE TABLE IF NOT EXISTS ytvods (vodid text, pubtime text, title text, starttime text, endtime text, thumbnail text, livestreamEtag text, hash text);`

const sqlCreateLivestreamEtag string = `CREATE TABLE IF NOT EXISTS livestreamSearchEtag (time text, etag text);`

const sqlCreatePlaylistEtag string = `CREATE TABLE IF NOT EXISTS playlistEtag (time text, etag text);`

const sqlCreateVodsIndex string = `CREATE UNIQUE INDEX IF NOT EXISTS vodids ON ytvods(vodid);`

const sqlCreateLEtagIndex string = `CREATE UNIQUE INDEX IF NOT EXISTS letags ON livestreamSearchEtag(etag);`

const sqlCreatePEtagIndex string = `CREATE UNIQUE INDEX IF NOT EXISTS petags ON playlistEtag(etag);`

func LoadDotEnv() Config {
	var err error
	var cfg Config

	log.Debugf("Loading environment variables")
	godotenv.Load()

	cfg.GoogleCred = os.Getenv("GOOGLE_CRED")
	if cfg.GoogleCred == "" {
		log.Fatalf("Please set the GOOGLE_CRED environment variable and restart the app")
	}
	cfg.LWODDBFile = os.Getenv("LWOD_DB_FILE")
	if cfg.LWODDBFile == "" {
		log.Fatalf("Please set the LWOD_DB_FILE environment variable and restart the app")
	}
	cfg.YTDBFile = os.Getenv("YT_DB_FILE")
	if cfg.YTDBFile == "" {
		log.Fatalf("Please set the YT_DB_FILE environment variable and restart the app")
	}
	cfg.LWODFolder = os.Getenv("LWOD_FOLDER")
	if cfg.LWODFolder == "" {
		log.Fatalf("Please set the LWOD_FOLDER environment variable and restart the app")
	}
	cfg.YTChannel = os.Getenv("YT_CHANNEL")
	if cfg.YTChannel == "" {
		log.Fatalf("Please set the YT_CHANNEL environment variable and restart the app")
	}
	cfg.YTPlaylist = os.Getenv("YT_PLAYLIST")
	if cfg.YTChannel == "" {
		log.Fatalf("Please set the YT_PLAYLIST environment variable and restart the app")
	}
	cfg.LWODHealthCheck = os.Getenv("LWOD_HEALTHCHECK")
	cfg.YTHealthCheck = os.Getenv("YT_HEALTHCHECK")
	lwoddelayStr := os.Getenv("LWOD_DELAY")
	if lwoddelayStr == "" {
		log.Fatalf("Please set the LWOD_DELAY environment variable and restart the app")
	}
	cfg.MainPlatform = os.Getenv("MAIN_PLATFORM")
	if cfg.MainPlatform != "youtube" && cfg.MainPlatform != "twitch" {
		log.Fatalf("Please set the MAIN_PLATFORM environment variable to either \"youtube\" or \"twitch\" and restart the app")
	}
	cfg.LWODDelay, err = strconv.Atoi(lwoddelayStr)
	if err != nil {
		log.Fatalf("strconv error: %s", err)
	}
	ytdelayStr := os.Getenv("YT_DELAY")
	if ytdelayStr == "" {
		log.Fatalf("Please set the YT_DELAY environment variable and restart the app")
	}
	cfg.YTDelay, err = strconv.Atoi(ytdelayStr)
	if err != nil {
		log.Fatalf("strconv error: %s", err)
	}
	lwodrefreshStr := os.Getenv("LWOD_REFRESH")
	if lwodrefreshStr == "" {
		lwodrefreshStr = "0"
	}
	cfg.LWODRefresh, err = strconv.Atoi(lwodrefreshStr)
	if err != nil {
		log.Fatalf("strconv error: %s", err)
	}
	ytrefreshStr := os.Getenv("YT_REFRESH")
	if ytrefreshStr == "" {
		ytrefreshStr = "0"
	}
	cfg.YTRefresh, err = strconv.Atoi(ytrefreshStr)
	if err != nil {
		log.Fatalf("strconv error: %s", err)
	}
	ytapirefreshStr := os.Getenv("YT_API_REFRESH")
	if ytapirefreshStr == "" {
		ytapirefreshStr = "0"
	}
	cfg.YTAPIRefresh, err = strconv.Atoi(ytapirefreshStr)
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

	dbpath = filepath.Join(".", "db", config.LWODDBFile)
	config.LWODDBConfig.DB, err = sql.Open("sqlite3", fmt.Sprintf("file:%s?_fk=true", dbpath))
	if err != nil {
		log.Fatalf("Error opening/creating lwoddb: %s", err)
	}

	if _, err := config.LWODDBConfig.DB.Exec(sqlCreateYouTube); err != nil {
		log.Fatalf("Error creating the YouTube table: %s", err)
	}

	if _, err := config.LWODDBConfig.DB.Exec(sqlCreateTwitch); err != nil {
		log.Fatalf("Error creating the Twitch table: %s", err)
	}

	if _, err := config.LWODDBConfig.DB.Exec(sqlCreateMain); err != nil {
		log.Fatalf("Error creating the lwod table: %s", err)
	}

	if _, err := config.LWODDBConfig.DB.Exec(sqlCreateLink); err != nil {
		log.Fatalf("Error creating the link table: %s", err)
	}

	config.LWODDBConfig.Statements.SelectYTHashStmt, err = config.LWODDBConfig.DB.Prepare("SELECT hash FROM youtube WHERE id = ? LIMIT 1")
	if err != nil {
		log.Fatalf("Error preparing a db statement: %s", err)
	}

	config.LWODDBConfig.Statements.SelectYTByHashStmt, err = config.LWODDBConfig.DB.Prepare("SELECT id FROM youtube WHERE hash = ? LIMIT 1")
	if err != nil {
		log.Fatalf("Error preparing a db statement: %s", err)
	}

	config.LWODDBConfig.Statements.DeleteYTStmt, err = config.LWODDBConfig.DB.Prepare("DELETE FROM youtube WHERE id = ?")
	if err != nil {
		log.Fatalf("Error preparing a db statement: %s", err)
	}

	config.LWODDBConfig.Statements.SelectTwitchHashStmt, err = config.LWODDBConfig.DB.Prepare("SELECT hash FROM twitch WHERE id = ? LIMIT 1")
	if err != nil {
		log.Fatalf("Error preparing a db statement: %s", err)
	}

	config.LWODDBConfig.Statements.SelectTwitchByHashStmt, err = config.LWODDBConfig.DB.Prepare("SELECT id FROM youtube WHERE hash = ? LIMIT 1")
	if err != nil {
		log.Fatalf("Error preparing a db statement: %s", err)
	}

	config.LWODDBConfig.Statements.DeleteTwitchStmt, err = config.LWODDBConfig.DB.Prepare("DELETE FROM twitch WHERE id = ?")
	if err != nil {
		log.Fatalf("Error preparing a db statement: %s", err)
	}

	config.LWODDBConfig.Statements.InsertYTStmt, err = config.LWODDBConfig.DB.Prepare("INSERT INTO youtube (id, hash) VALUES (?, ?)")
	if err != nil {
		log.Fatalf("Error preparing a db statement: %s", err)
	}

	config.LWODDBConfig.Statements.InsertTwitchStmt, err = config.LWODDBConfig.DB.Prepare("INSERT INTO twitch (id, hash) VALUES (?, ?)")
	if err != nil {
		log.Fatalf("Error preparing a db statement: %s", err)
	}

	config.LWODDBConfig.Statements.InsertURLStmt, err = config.LWODDBConfig.DB.Prepare("INSERT INTO lwodUrl (date, sheetId) VALUES (?, ?) ON CONFLICT DO NOTHING")
	if err != nil {
		log.Fatalf("Error preparing a db statement: %s", err)
	}

	dbpath = filepath.Join(".", "db", config.YTDBFile)
	config.YTDBConfig.DB, err = sql.Open("sqlite3", fmt.Sprintf("file:%s?_fk=true", dbpath))
	if err != nil {
		log.Fatalf("Error opening/creating ytvoddb: %s", err)
	}

	if _, err := config.YTDBConfig.DB.Exec(sqlCreateVods); err != nil {
		log.Fatalf("Error creating the ytvods table: %s", err)
	}

	if _, err := config.YTDBConfig.DB.Exec(sqlCreateLivestreamEtag); err != nil {
		log.Fatalf("Error creating the etag table: %s", err)
	}

	if _, err := config.YTDBConfig.DB.Exec(sqlCreatePlaylistEtag); err != nil {
		log.Fatalf("Error creating the etag table: %s", err)
	}

	if _, err := config.YTDBConfig.DB.Exec(sqlCreateVodsIndex); err != nil {
		log.Fatalf("Error creating the ytvods index: %s", err)
	}

	if _, err := config.YTDBConfig.DB.Exec(sqlCreateLEtagIndex); err != nil {
		log.Fatalf("Error creating the letag index: %s", err)
	}

	if _, err := config.YTDBConfig.DB.Exec(sqlCreatePEtagIndex); err != nil {
		log.Fatalf("Error creating the petag index: %s", err)
	}

	config.YTDBConfig.Statements.SelectVods, err = config.YTDBConfig.DB.Prepare("SELECT * FROM ytvods")
	if err != nil {
		log.Fatalf("Error preparing a db statement: %s", err)
	}

	config.YTDBConfig.Statements.GetPlaylistEtag, err = config.YTDBConfig.DB.Prepare("SELECT etag FROM playlistEtag ORDER BY time DESC LIMIT 1")
	if err != nil {
		log.Fatalf("Error preparing a db statement: %s", err)
	}

	config.YTDBConfig.Statements.AddPlaylistEtag, err = config.YTDBConfig.DB.Prepare("REPLACE INTO playlistEtag (time, etag) VALUES (?, ?)")
	if err != nil {
		log.Fatalf("Error preparing a db statement: %s", err)
	}

	config.YTDBConfig.Statements.GetLivestreamSearchEtag, err = config.YTDBConfig.DB.Prepare("SELECT etag FROM livestreamSearchEtag ORDER BY time DESC LIMIT 1")
	if err != nil {
		log.Fatalf("Error preparing a db statement: %s", err)
	}

	config.YTDBConfig.Statements.AddLivestreamSearchEtag, err = config.YTDBConfig.DB.Prepare("REPLACE INTO livestreamSearchEtag (time, etag) VALUES (?, ?)")
	if err != nil {
		log.Fatalf("Error preparing a db statement: %s", err)
	}

	config.YTDBConfig.Statements.ReplaceVod, err = config.YTDBConfig.DB.Prepare("REPLACE INTO ytvods (vodid, pubtime, title, starttime, endtime, thumbnail, livestreamEtag, hash) VALUES (?, ?, ?, ?, ?, ?, ?, ?)")
	if err != nil {
		log.Fatalf("Error preparing a db statement: %s", err)
	}

	log.Debugf("Connected to the databases successfully")
}

func CreateGoogleClients(config *Config) {
	log.Debugf("Creating Google API clients")

	ctx := context.Background()

	credpath := filepath.Join(".", config.GoogleCred)
	b, err := os.ReadFile(credpath)
	if err != nil {
		log.Fatalf("Unable to read client secret file: %v", err)
	}

	cfg, err := google.JWTConfigFromJSON(b, "https://spreadsheets.google.com/feeds https://www.googleapis.com/auth/drive https://www.googleapis.com/auth/youtube.readonly")
	if err != nil {
		log.Fatalf("Unable to parse client secret file to config: %v", err)
	}
	client := cfg.Client(ctx)

	config.GoogleConfig.Sheets, err = sheets.NewService(ctx, option.WithHTTPClient(client))
	if err != nil {
		log.Fatalf("Unable to retrieve Sheets client: %v", err)
	}

	config.GoogleConfig.Drive, err = drive.NewService(ctx, option.WithHTTPClient(client))
	if err != nil {
		log.Fatalf("Unable to retrieve Drive client: %v", err)
	}

	config.GoogleConfig.YouTube, err = youtube.NewService(ctx, option.WithHTTPClient(client))
	if err != nil {
		log.Fatalf("Unable to retrieve YouTube client: %v", err)
	}

	log.Debugf("Created Google API clients successfully")
}

func Initialize() Config {
	cfg := LoadDotEnv()
	CreateGoogleClients(&cfg)
	LoadDatabase(&cfg)
	return cfg
}

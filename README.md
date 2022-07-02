# lwodcollector

a tool that parses [Scoot's wonderful Last Week on Destiny spreadsheets](https://drive.google.com/drive/folders/1aRv251i5bZIk223SDssmdvksKvrEYHdK) and saves the data to an SQLite DB

## Usage

1. Create a [Google Cloud Platform project](https://console.developers.google.com/), enable both the Drive and Sheets APIs, [setup a service account](https://console.cloud.google.com/apis/credentials) and get the .json creds file. Used [this video](https://www.youtube.com/watch?v=vISRn5qFrkM) a couple of years ago to guide me through the process, though it might be outdated.
2. ```cp .env.example .env```
3. Change the ```.env``` how you see fit.
4. ```go build```
5. ```lwodcollector```

## .env

### GOOGLE_CRED

Sets the client secret file name.

### DB_FILE

Sets the SQLite DB file name.

### LWOD_FOLDER

Sets the LWOD folder ID.

### DELAY

Sets the delay between parsing spreadsheets.

### REFRESH (optional)

Sets the app to continuous mode and refreshes every set amount of minutes.

### HEALTHCHECK (optional)

If the app is in continuous mode will send an HTTP request to the specified address every refresh.

## Flags

### -c, --continuous

Run the parser every set amount of minutes (can also be set with the REFRESH env var).

### -a, --all

Process every single sheet.

### -h, --help

Print help information.

### -v, --verbose

Show debug messages.
package main

import (
	"bufio"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type dbentry struct {
	id         int
	titleText  string
	titleUrl   string
	authorName string
	authorUrl  string
	date       time.Time
}

type dbkey map[string]int

type dbvalue struct {
	keyId int
	data  string
}

type entryGen struct {
	nextID int
}

func newEntryGen() *entryGen {
	return &entryGen{nextID: 1}
}

func (g *entryGen) generate(data map[string]interface{}) *dbentry {
	e := g.parse(data, g.nextID)
	g.nextID++
	return e
}

func (g *entryGen) parse(data map[string]interface{}, id int) *dbentry {
	e := &dbentry{id: id}
	author, ok := data["_author"].(map[string]interface{})
	if ok {
		e.authorName = author["name"].(string)
		e.authorUrl = author["url"].(string)
	}
	title, ok := data["_title"].(map[string]interface{})
	if ok {
		e.titleText = title["text"].(string)
		e.titleUrl = title["url"].(string)
	}
	if d, ok := data["_date"].(string); ok {
		// Silently ignore invalid dates
		if date, err := time.Parse(time.RFC3339, d); err == nil {
			e.date = date
		}
	}
	return e
}

func (ks dbkey) addKeys(data map[string]interface{}) dbvalues {
	vals := dbvalues(make([]*dbvalue, 0, len(data)))
	for k := range data {
		if k == "_author" || k == "_title" || k == "_date" {
			continue
		}
		key := strings.TrimSpace(k)
		id, ok := ks[key]
		if !ok {
			id = len(ks) + 1
			ks[key] = id
		}
		d, ok := data[k].(string)
		if !ok {
			d = ""
		}
		vals = append(vals, &dbvalue{
			keyId: id,
			data:  d,
		})
	}
	return vals
}

type dbvalues []*dbvalue

type stmts struct {
	entry *sql.Stmt
	value *sql.Stmt
	key   *sql.Stmt
}

func (e *dbentry) store(s *stmts) error {
	_, err := s.entry.Exec(e.id, e.titleText, e.titleUrl, e.authorName, e.authorUrl, e.date)
	if err != nil {
		return fmt.Errorf("cannot store entry: %s", err)
	}
	return nil
}

func (vs dbvalues) store(s *stmts) error {
	for i := range vs {
		if vs[i] == nil {
			log.Fatal("value stmt is nil")
		}
		_, err := s.value.Exec(vs[i].keyId, vs[i].data)
		if err != nil {
			return fmt.Errorf("cannot store value: %s", err)
		}
	}
	return nil
}

func (ks dbkey) store(s *stmts) error {
	for k, id := range ks {
		_, err := s.key.Exec(id, k)
		if err != nil {
			return fmt.Errorf("cannot store key: %s", err)
		}
	}
	return nil
}

type storer interface {
	store(s *stmts) error
}

type dbconn struct {
	db *sql.DB
	s  *stmts
}

func (c *dbconn) start(dsn string) error {
	var err error
	c.db, err = sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("cannot connect to mysql: %s", err)
	}
	c.s = &stmts{}
	c.s.entry, err = c.db.Prepare("INSERT INTO `entries` (id, title_text, title_url, author_name, author_url, date) VALUES (?, ?, ?, ?, ?, ?)")
	if err != nil {
		return fmt.Errorf("cannot prepare entries statement: %s", err)
	}
	c.s.value, err = c.db.Prepare("INSERT INTO `values` (key_id, data) VALUES (?, ?)")
	if err != nil {
		return fmt.Errorf("cannot prepare values statement: %s", err)
	}
	c.s.key, err = c.db.Prepare("INSERT INTO `keys` (id, name) VALUES (?, ?)")
	if err != nil {
		return fmt.Errorf("cannot prepare keys statement: %s", err)
	}
	return nil
}

func (c *dbconn) store(in <-chan storer, done chan<- struct{}) {
	for s := range in {
		if err := s.store(c.s); err != nil {
			log.Fatal("Cannot store: ", err)
		}
	}
	c.db.Close()
	close(done)
}

func main() {
	dsn := "opi:zGRUYmDbASCydFXt@/opi"
	filename := "/data/www/tmp/OPI.json"
	file, err := os.Open(filename)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	conn := &dbconn{}
	if err := conn.start(dsn); err != nil {
		log.Fatal("cannot start DB: ", err)
	}
	db := make(chan storer, 100)
	done := make(chan struct{})
	go conn.store(db, done)

	eg := newEntryGen()
	keys := dbkey(make(map[string]int))

	r := bufio.NewReader(file)
	for {
		line, err := r.ReadBytes('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Fatal(err)
		}
		var data map[string]interface{}
		if err = json.Unmarshal(line, &data); err != nil {
			log.Printf("error: cannot unmarshal JSON: %s", err)
			continue
		}
		entry := eg.generate(data)
		db <- entry
		vals := keys.addKeys(data)
		db <- vals
	}
	db <- keys
	close(db)
	<-done
}

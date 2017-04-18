package main

import (
	"encoding/base64"
	"fmt"
	"io"
	"io/ioutil"
	"mime"
	"net/http"
	"sync"

	"github.com/dullgiulio/wiki-extract-mdata/lru"
)

type mimed struct {
	mime string
	data []byte
}

func newMimedFromUrl(url string) (*mimed, error) {
	m := &mimed{}
	resp, err := http.Get(url)
	if err != nil {
		return nil, fmt.Errorf("cannot GET: %s", err)
	}
	defer resp.Body.Close()
	m.data, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("cannot read body: %s", err)
	}
	hdr := resp.Header.Get("Content-Type")
	m.mime, _, err = mime.ParseMediaType(hdr)
	if err != nil {
		return nil, fmt.Errorf("cannot get mime type: %s", err)
	}
	return m, nil
}

func (i *mimed) WriteTo(w io.Writer) (int64, error) {
	m, err := w.Write([]byte("data:" + i.mime + ";base64,"))
	if err != nil {
		return int64(m), err
	}
	n := int64(m)
	enc := base64.NewEncoder(base64.StdEncoding, w)
	if m, err = enc.Write(i.data); err != nil {
		return n + int64(m), err
	}
	enc.Close()
	return n + int64(m), nil
}

type imgproc struct {
	proc chan func()
	mux  sync.Mutex
	lru  *lru.Cache
}

func newImgproc(nworkers, max int) *imgproc {
	i := &imgproc{
		proc: make(chan func()),
		lru:  lru.New(max),
	}
	for n := 0; n < nworkers; n++ {
		go i.run()
	}
	return i
}

func (i imgproc) get(url string) (*mimed, error) {
	var (
		err error
		m   *mimed
	)
	done := make(chan struct{})
	i.proc <- func() {
		m, err = i.fetch(url)
		close(done)
	}
	<-done
	return m, err
}

func (i *imgproc) run() {
	for fn := range i.proc {
		fn()
	}
}

func (i *imgproc) fetch(url string) (*mimed, error) {
	var err error
	i.mux.Lock()
	m, ok := i.lru.Get(url)
	if ok {
		i.mux.Unlock()
		return m.(*mimed), nil
	}
	i.mux.Unlock()
	m, err = newMimedFromUrl(url)
	// TODO: implement anti-stampede system?
	if err != nil {
		i.mux.Lock()
		i.lru.Add(url, m)
		i.mux.Unlock()
	}
	return m.(*mimed), err
}

package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/PuerkitoBio/goquery"
	"golang.org/x/net/html"
)

func nodeGetAttr(node *html.Node, attr string) string {
	for n := range node.Attr {
		if node.Attr[n].Key == attr {
			return node.Attr[n].Val
		}
	}
	return ""
}

func emitSubpages(r io.Reader, domain string, out chan<- string) error {
	doc, err := goquery.NewDocumentFromReader(r)
	if err != nil {
		close(out)
		return fmt.Errorf("cannot query document: %s", err)
	}
	doc.Find("#page-children a").Each(func(i int, s *goquery.Selection) {
		node := s.Get(0)
		href := nodeGetAttr(node, "href")
		if href != "" {
			out <- domain + href
		}
	})
	close(out)
	return nil
}

type values map[string]interface{}

type processor struct {
	domain string
}

func (p *processor) renderText(w io.Writer, node *html.Node) error {
	if node == nil {
		return nil
	}
	if node.Type == html.TextNode {
		data := strings.TrimSpace(node.Data)
		_, err := w.Write([]byte(data))
		return err
	}
	var writeAfter []byte
	if node.Type == html.ElementNode {
		var data []byte
		switch node.Data {
		case "li":
			data = []byte("\t* ")
			writeAfter = []byte("\n")
		case "br":
			data = []byte("\n")
		case "a":
			href := nodeGetAttr(node, "href")
			if href != "" {
				data = []byte(" <a href=\"" + href + "\">")
				writeAfter = []byte("</a>")
			}
			// case "img": // TODO: download and emit embedded img tag
		}
		if data != nil {
			if _, err := w.Write(data); err != nil {
				return err
			}
		}
	}
	for node = node.FirstChild; node != nil; node = node.NextSibling {
		if err := p.renderText(w, node); err != nil {
			return err
		}
	}
	if writeAfter != nil {
		if _, err := w.Write(writeAfter); err != nil {
			return err
		}
	}
	return nil
}

func (p *processor) metadata(doc *goquery.Document, vals map[string]interface{}) error {
	var err error
	doc.Find("#title-text a").Each(func(i int, s *goquery.Selection) {
		node := s.Get(0)
		href := nodeGetAttr(node, "href")
		vals["_title"] = map[string]string{
			"text": node.FirstChild.Data,
			"url":  p.domain + href,
		}
	})
	doc.Find(".page-metadata-modification-info .author a").Each(func(i int, s *goquery.Selection) {
		node := s.Get(0)
		href := nodeGetAttr(node, "href")
		vals["_author"] = map[string]string{
			"name": node.FirstChild.Data,
			"url":  p.domain + href,
		}
	})
	doc.Find(".page-metadata-modification-info .last-modified a").Each(func(i int, s *goquery.Selection) {
		node := s.Get(0)
		dateText := node.FirstChild.Data
		date, err := time.Parse("02 Jan 2006", dateText)
		if err != nil {
			err = fmt.Errorf("cannot parse modification date: %s", err)
			return
		}
		vals["_date"] = date.Format(time.RFC3339)
	})
	return err
}

func (p *processor) attributes(r io.Reader) (values, error) {
	doc, err := goquery.NewDocumentFromReader(r)
	if err != nil {
		return nil, fmt.Errorf("cannot query document: %s", err)
	}
	vals := make(map[string]interface{})
	if err := p.metadata(doc, vals); err != nil {
		return nil, fmt.Errorf("cannot query metadata: %s", err)
	}
	var key string
	var hasKey bool
	doc.Find("#main-content table.confluenceTable").Each(func(i int, s *goquery.Selection) {
		if err != nil {
			return
		}
		s.Find("tr").Each(func(i int, s *goquery.Selection) {
			if err != nil {
				return
			}
			s.Find("td").Each(func(i int, s *goquery.Selection) {
				if err != nil {
					return
				}
				node := s.Get(0)
				var buf bytes.Buffer
				if err = p.renderText(&buf, node); err != nil {
					err = fmt.Errorf("cannot render subitem: %s", err)
					return
				}
				data := buf.String()
				if hasKey {
					vals[key] = data
					hasKey = false
					return
				}
				key = data
				hasKey = true
			})
			if hasKey {
				vals[key] = ""
			}
			key = ""
			hasKey = false
		})
	})
	return values(vals), err
}

func (p *processor) processPage(r io.Reader) ([]byte, error) {
	vals, err := p.attributes(r)
	if err != nil {
		return nil, fmt.Errorf("cannot extract from supage: %s", err)
	}
	data, err := json.Marshal(vals)
	if err != nil {
		return nil, fmt.Errorf("cannot write JSON: %s", err)
	}
	return data, nil
}

func (p *processor) pageReader(url string) (io.Reader, error) {
	resp, err := http.Get(url)
	if err != nil {
		return nil, fmt.Errorf("cannot GET: %s", err)
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("cannot read body: %s", err)
	}
	return bytes.NewReader(body), nil
}

func (p *processor) process(in <-chan string, out chan<- []byte, wg *sync.WaitGroup) {
	for url := range in {
		log.Printf("debug: processing start: %s", url)
		r, err := p.pageReader(url)
		if err != nil {
			log.Printf("%s: cannot read page content: %s", url, err)
			continue
		}
		data, err := p.processPage(r)
		if err != nil {
			log.Fatal("cannot extract from supage: %s", err)
		}
		log.Printf("debug: processing done: %s", url)
		out <- data
	}
	wg.Done()
}

func printer(in <-chan []byte, w io.Writer) {
	for data := range in {
		if _, err := w.Write(data); err != nil {
			log.Fatal("cannot write to output: %s", err)
		}
		if _, err := w.Write([]byte("\n")); err != nil {
			log.Fatal("cannot write to output: %s", err)
		}
	}
}

func main() {
	nworkers := 6
	filename := "OPI.html"
	domain := "http://wiki.local"

	domains := make(chan string, 2048)
	go func() {
		// TODO: Could just use pageReader here, but lots of memory.
		r, err := os.Open(filename)
		if err != nil {
			log.Fatal("cannot open file: ", err)
		}
		if err := emitSubpages(r, domain, domains); err != nil {
			log.Fatal("cannot get subpages: %s", err)
		}
	}()
	out := make(chan []byte)
	wg := &sync.WaitGroup{}
	wg.Add(nworkers)
	p := &processor{domain}
	for i := 0; i < nworkers; i++ {
		go p.process(domains, out, wg)
	}
	go printer(out, os.Stdout)
	wg.Wait()
}

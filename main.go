package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
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
			return strings.TrimSpace(node.Attr[n].Val)
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

type byteTo []byte

func (b byteTo) WriteTo(w io.Writer) (int64, error) {
	n, err := w.Write([]byte(b))
	return int64(n), err
}

type imageTo struct {
	img *mimed
}

func (i *imageTo) WriteTo(w io.Writer) (int64, error) {
	n, err := w.Write([]byte("<img src=\""))
	if err != nil {
		return int64(n), err
	}
	m, err := i.img.WriteTo(w)
	m += int64(n)
	if err != nil {
		return m, err
	}
	n, err = w.Write([]byte("\" />"))
	return m + int64(n), err
}

type processor struct {
	domain  string
	imgproc *imgproc
}

func (p *processor) run(nworkers int, domains chan string, out chan []byte) {
	wg := &sync.WaitGroup{}
	wg.Add(nworkers)
	for i := 0; i < nworkers; i++ {
		go p.process(domains, out, wg)
	}
	wg.Wait()
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
	var after, before io.WriterTo
	if node.Type == html.ElementNode {
		switch node.Data {
		case "li":
			before = byteTo([]byte("\t* "))
			after = byteTo([]byte("\n"))
		case "br":
			before = byteTo([]byte("\n"))
		case "a":
			href := nodeGetAttr(node, "href")
			if href != "" {
				before = byteTo([]byte(" <a href=\"" + href + "\">"))
				after = byteTo([]byte("</a> "))
			}
		case "img":
			src := nodeGetAttr(node, "src")
			if src != "" {
				img, err := p.imgproc.get(p.domain + src)
				// Silently skip images we cannot get
				if err != nil {
					log.Printf("cannot include image %s: %s", p.domain+src, err)
					before = byteTo([]byte(" [image unavailable] "))
				} else {
					before = &imageTo{img: img}
				}
			}
		default:
			before = byteTo([]byte(" "))
			after = byteTo([]byte(" "))
		}
	}
	if before != nil {
		if _, err := before.WriteTo(w); err != nil {
			return err
		}
	}
	for node = node.FirstChild; node != nil; node = node.NextSibling {
		if err := p.renderText(w, node); err != nil {
			return err
		}
	}
	if after != nil {
		if _, err := after.WriteTo(w); err != nil {
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
	mimed, err := newMimedFromUrl(url)
	if err != nil {
		return nil, err
	}
	return bytes.NewReader(mimed.data), nil
}

func (p *processor) fileReader(url string) (io.Reader, error) {
	r, err := os.Open(url)
	if err != nil {
		return nil, fmt.Errorf("cannot read file: %s", err)
	}
	defer r.Close()
	data, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("cannot load file: %s", err)
	}
	return bytes.NewReader(data), nil
}

func (p *processor) process(in <-chan string, out chan<- []byte, wg *sync.WaitGroup) {
	for url := range in {
		log.Printf("debug: processing start: %s", url)
		var (
			r   io.Reader
			err error
		)
		if url[0:7] == "file://" {
			r, err = p.fileReader(url[7:])
		} else {
			r, err = p.pageReader(url)
		}
		if err != nil {
			log.Printf("%s: cannot read page content: %s", url, err)
			continue
		}
		data, err := p.processPage(r)
		if err != nil {
			log.Fatalf("cannot extract from supage: %s", err)
		}
		log.Printf("debug: processing done: %s", url)
		out <- data
	}
	wg.Done()
}

func printer(in <-chan []byte, w io.Writer) {
	for data := range in {
		if _, err := w.Write(data); err != nil {
			log.Fatalf("cannot write to output: %s", err)
		}
		if _, err := w.Write([]byte("\n")); err != nil {
			log.Fatalf("cannot write to output: %s", err)
		}
	}
}

func main() {
	nworkers := 6
	filename := "OPI.html"
	domain := "http://wiki.local"
	maxLru := 256

	domains := make(chan string, 2048)
	out := make(chan []byte)
	go func() {
		r, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open file: %s", err)
		}
		if err := emitSubpages(r, domain, domains); err != nil {
			log.Fatalf("cannot get subpages: %s", err)
		}
		r.Close()
		/*
			domains <- "file://./subpage.html"
			close(domains)
		*/
	}()
	processor := &processor{
		domain:  domain,
		imgproc: newImgproc(nworkers, maxLru),
	}
	go printer(out, os.Stdout)
	processor.run(nworkers, domains, out)
}

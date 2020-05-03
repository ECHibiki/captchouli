package gelbooru

import (
	"database/sql"
	"io"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"

	"github.com/bakape/boorufetch"
	"github.com/bakape/captchouli/common"
	"github.com/bakape/captchouli/db"
)

var (
	cache = make(map[string]*cacheEntry)
	mu    sync.Mutex

	// Tag deduplication map. Reused to reduce allocations.
	dedupMap = make(map[string]struct{})
)

type cacheEntry struct {
	pages    map[int]struct{}
	maxPages int // Estimate for maximum number of pages
}

// Fetch random matching file from Gelbooru.
// f can be nil, if no file is matched, even when err = nil.
// Caller must close and remove temporary file after use.
func Fetch(req common.FetchRequest) (f *os.File, image db.Image, err error) {
	mu.Lock()
	defer mu.Unlock()

	//removed
	//  
	tags :=
		"solo -multiple_girls -couple -multiple_boys -monochrome -photo -objectification -cosplay solo " +
			req.Tag

	err = tryFetchPage(req.Tag, tags)
	if err != nil {
		log.Printf("err1")
		return
	}
	img, err := db.PopRandomPendingImage(req.Tag)
	fmt.Println(img)
	fmt.Println(err)
	if err != nil {
		log.Printf("err2")
		if err == sql.ErrNoRows {
			log.Printf("-1")
			err = nil
		}
		return
	}

	image = db.Image{
		Rating: img.Rating,
		Source: req.Source,
		MD5:    img.MD5,
		Tags:   img.Tags,
	}

	r, err := http.Get(img.URL)
	if err != nil {
		log.Printf("err3")
		return
	}
	defer r.Body.Close()

	f, err = ioutil.TempFile("", "")
	if err != nil {
		log.Printf("err4")
		return
	}
	_, err = io.Copy(f, r.Body)
	if err != nil {
		log.Printf("err5")
		// Ignore any errors here. This cleanup need not succeed.
		f.Close()
		os.Remove(f.Name())
		f = nil
	}
	return
}

// Attempt to fetch a random page from gelbooru
// api has max of 200 as of date
func tryFetchPage(requested, tags string) (err error) {
	store := cache[tags]
	if store == nil {
		maxPages := 200
		if common.IsTest { // Reduce test duration
			maxPages = 10
		}
		store = &cacheEntry{
			pages:    make(map[int]struct{}),
			maxPages: maxPages,
		}
		cache[tags] = store
	}
	if store.maxPages == 0 {
		err = common.ErrNoMatch
		return
	}
	if len(store.pages) == store.maxPages {
		// Already fetched all pages
		return
	}

	// Always dowload first page on fresh fetch
	var page int
	if len(store.pages) != 0 {
		page = common.RandomInt(store.maxPages)
	} else {
		page = 0
	}

	_, ok := store.pages[page]
	if ok { // Cache hit
		return
	}

	posts, err := boorufetch.FromGelbooru(tags, uint(page), 100)
	if err != nil {
		log.Printf("errA")
		return
	}
	if len(posts) == 0 {
		if page == 0 {
			err = common.ErrNoMatch
			store.maxPages = 0 // Mark as invalid
			return
		}
		// Empty page. Don't check pages past this one. They will also be empty.
		store.maxPages = page
		// Retry with a new random page
		return tryFetchPage(requested, tags)
	}

	// Push applicable posts to pending image set.
	// Reuse allocated resources, where possible.
	var (
		booruTags            []boorufetch.Tag
		img                  = db.PendingImage{TargetTag: requested}
		hasChar, valid, inDB bool
	)
	for i, p := range posts {
		if common.IsTest && i >= 10 {
			break // Shorten tests
		}
		img.MD5, err = p.MD5()
		if err != nil {
			log.Printf("err6")
			return
		}

		// Check, if not already in DB
		inDB, err = db.IsInDatabase(img.MD5)
		if err != nil {
			log.Printf("err7")
			return
		}
		if inDB {
			continue
		}
		inDB, err = db.IsPendingImage(img.MD5)
		if err != nil {
			log.Printf("err8")
			return
		}
		if inDB {
			continue
		}

		// File must be a still image
		valid = false
		img.URL = p.FileURL()
		if img.URL != "" {
			for _, s := range [...]string{"jpg", "jpeg", "png"} {
				if strings.HasSuffix(img.URL, s) {
					valid = true
					break
				}
			}
		}
		if !valid {
			err = db.BlacklistImage(img.MD5)
			if err != nil {
				log.Printf("err9")
				return
			}
			continue
		}

		// Rating and tag fetches might need a network fetch, so do these later
		img.Rating, err = p.Rating()
		if err != nil {
			log.Printf("err10")
			return
		}

		hasChar = false
		booruTags, err = p.Tags()
		if err != nil {
			log.Printf("err11")
			return
		}
		for k := range dedupMap {
			delete(dedupMap, k)
		}
		for _, t := range booruTags {
			// Allow only images with 1 character in them and ensure said
			// character matches the requested tag in case of gelbooru-danbooru
			// desync
			if t.Type == boorufetch.Character {
				if hasChar ||
					// Ensure no case mismatch, as tags are queried as lowercase
					// in the boorus
					strings.ToLower(t.Tag) != strings.ToLower(requested) {
					err = db.BlacklistImage(img.MD5)
					if err != nil {
						return
					}
					goto skip
				}
				hasChar = true
			}
			// Dedup tags just in case. Boorus can't be trusted too much.
			dedupMap[t.Tag] = struct{}{}
		}
		dedupMap[requested] = struct{}{} // Ensure map contains initial tag

		img.Tags = make([]string, 0, len(dedupMap))
		for t := range dedupMap {
			img.Tags = append(img.Tags, t)
		}

		err = db.InsertPendingImage(img)
		if err != nil {
			return
		}
		if common.IsTest {
			log.Printf("logged pending image: %s\n", img.URL)
		}

	skip:
	}

	// Set page as seen
	store.pages[page] = struct{}{}

	return
}

package appenginetest

import (
	"net/http"
	"strconv"
	"time"

	"github.com/gorilla/mux"

	"appengine"
	"appengine/datastore"
)

func init() {
	router := mux.NewRouter()
	router.HandleFunc("/db", initDBRoute)
	router.HandleFunc("/run/{max}", runCursorRoute)
	http.Handle("/", router)
}

type TestEntity struct {
	Value int
}

func initDBRoute(res http.ResponseWriter, req *http.Request) {

	c := appengine.NewContext(req)

	for i := 0; i < 10; i++ {
		t := TestEntity{Value: 0}
		_, err := datastore.Put(c, datastore.NewIncompleteKey(c, "testentity", nil), &t)
		if err != nil {
			http.Error(res, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	res.Write([]byte("DB INIT DONE!"))
}

func runCursorRoute(res http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	valueMax, err := strconv.Atoi(vars["max"])
	if err != nil {
		http.Error(res, err.Error(), http.StatusInternalServerError)
		return
	}

	c := appengine.NewContext(req)
	err = iterate(c, valueMax, "")
	if err != nil {
		http.Error(res, err.Error(), http.StatusInternalServerError)
		return
	}

	res.Write([]byte("DONE!"))
}

func iterate(c appengine.Context, valueMax int, cursor string) error {
	q := datastore.NewQuery("testentity").Filter("Value<", valueMax)
	if cursor != "" {
		curs, err := datastore.DecodeCursor(cursor)
		if err != nil {
			return err
		}
		q = q.Start(curs)
	}

	t := q.Run(c)
	for i := 0; i < 5; i++ {
		var te TestEntity
		k, err := t.Next(&te)
		if err == datastore.Done {
			c.Infof("DATASTORE DONE")
			return nil
		}
		if err != nil {
			c.Errorf("NEXT ERROR: %v", err)
			return err
		}

		te.Value = valueMax
		_, err = datastore.Put(c, k, &te)
		if err != nil {
			return err
		}
	}

	curs, err := t.Cursor()
	if err != nil {
		return err
	}

	time.Sleep(time.Millisecond * 500)
	return iterate(c, valueMax, curs.String())
}

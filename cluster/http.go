package cluster

import (
	"net/http"

	"github.com/gorilla/mux"

	"github.com/couchbase/cbgt/rest"
)

func InitStaticRouter() *mux.Router {
	hfsStaticX := http.FileServer(assetFS())

	router := mux.NewRouter()
	router.StrictSlash(true)

	router.Handle("/",
		http.RedirectHandler("/staticx/index.html", 302))
	router.Handle("/index.html",
		http.RedirectHandler("/staticx/index.html", 302))
	router.Handle("/static/partials/index/start.html",
		http.RedirectHandler("/staticx/partials/index/start.html", 302))

	router = rest.InitStaticRouter(router,
		"(no static)", "", []string{
			"/indexes",
			"/nodes",
			"/monitor",
			"/manage",
			"/logs",
			"/debug",
		}, http.RedirectHandler("/staticx/index.html", 302))

	router.PathPrefix("/staticx/").Handler(
		http.StripPrefix("/staticx/", hfsStaticX))

	return router
}

func myAssetDir(name string) ([]string, error) {
	var rv []string
	a, err := AssetDir(name)
	if err != nil {
		rv = append(rv, a...)
	}

	a, err = rest.AssetDir(name)
	if err != nil {
		rv = append(rv, a...)
	}

	return rv, err
}

func myAsset(name string) ([]byte, error) {
	b, err := Asset(name)
	if err != nil {
		b, err = rest.Asset(name)
	}

	return b, err
}

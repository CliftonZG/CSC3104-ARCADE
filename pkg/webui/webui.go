// Package webui serves the embedded browser-based multiplayer Uno client.
package webui

import (
	"embed"
	"io/fs"
	"net/http"
)

//go:embed static/*
var staticFiles embed.FS

// Handler returns an http.Handler serving the Uno web client at "/".
func Handler() http.Handler {
	sub, err := fs.Sub(staticFiles, "static")
	if err != nil {
		panic("webui: embedded static directory missing: " + err.Error())
	}
	return http.FileServer(http.FS(sub))
}

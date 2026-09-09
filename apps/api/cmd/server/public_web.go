package main

import (
	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
	"github.com/sumi-studio/sumi/apps/api/internal/publicweb"
)

func registerPublicWeb(control *agentevents.LocalControlServer) error {
	if control == nil {
		return nil
	}
	return (&publicweb.Server{Fetcher: publicweb.NewFetcher()}).RegisterLocalControlRoutes(control)
}

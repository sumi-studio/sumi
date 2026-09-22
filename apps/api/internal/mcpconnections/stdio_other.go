//go:build !linux

package mcpconnections

import (
	"context"
	"errors"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func startStdio(context.Context, LocalInput) (mcp.Transport, func(), error) {
	return nil, nil, errors.New("Local stdio MCP requires Linux/WSL")
}

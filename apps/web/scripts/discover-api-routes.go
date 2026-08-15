package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

type sourceFile struct {
	relative   string
	packageKey string
	parsed     *ast.File
}

type discoveredRoute struct {
	File    string `json:"file"`
	Line    int    `json:"line"`
	Pattern string `json:"pattern"`
}

type discovery struct {
	Routes                   []discoveredRoute `json:"routes"`
	WorkspacePackagePresent  bool              `json:"workspace_package_present"`
	PrivateDynamicRegistries int               `json:"private_dynamic_registries"`
}

func main() {
	apiRoot := flag.String("api-root", "", "absolute or relative apps/api directory")
	flag.Parse()
	if strings.TrimSpace(*apiRoot) == "" {
		fatalf("-api-root is required")
	}
	root, err := filepath.Abs(*apiRoot)
	if err != nil {
		fatalf("resolve api root: %v", err)
	}

	fset := token.NewFileSet()
	files, err := parseProductionSources(root, fset)
	if err != nil {
		fatalf("parse production API sources: %v", err)
	}
	constants := collectStringConstants(files)
	result := discovery{Routes: make([]discoveredRoute, 0)}

	for _, file := range files {
		if file.parsed.Name.Name == "workspace" || strings.HasPrefix(filepath.ToSlash(file.relative), "internal/workspace/") {
			result.WorkspacePackagePresent = true
		}
		for _, declaration := range file.parsed.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok || function.Body == nil {
				continue
			}
			privateDynamicRegistry := receiverName(function) == "LocalControlServer" && function.Name.Name == "RegisterRoutes"
			ast.Inspect(function.Body, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok || len(call.Args) == 0 || !isHTTPRegistration(call.Fun) {
					return true
				}
				pattern, ok := evaluateString(call.Args[0], constants[file.packageKey], map[string]bool{})
				if !ok {
					if privateDynamicRegistry && isIdentifier(call.Args[0], "pattern") {
						result.PrivateDynamicRegistries++
						return true
					}
					position := fset.Position(call.Args[0].Pos())
					fatalf("unresolved HTTP route expression at %s:%d; route parity cannot skip dynamic registrations", file.relative, position.Line)
				}
				if !isRoutePattern(pattern) {
					position := fset.Position(call.Args[0].Pos())
					fatalf("invalid HTTP route pattern %q at %s:%d", pattern, file.relative, position.Line)
				}
				position := fset.Position(call.Args[0].Pos())
				result.Routes = append(result.Routes, discoveredRoute{
					File:    file.relative,
					Line:    position.Line,
					Pattern: pattern,
				})
				return true
			})
		}
	}

	encoder := json.NewEncoder(os.Stdout)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(result); err != nil {
		fatalf("encode route discovery: %v", err)
	}
}

func parseProductionSources(root string, fset *token.FileSet) ([]sourceFile, error) {
	var result []sourceFile
	for _, subtree := range []string{"cmd/server", "internal"} {
		start := filepath.Join(root, filepath.FromSlash(subtree))
		info, err := os.Stat(start)
		if err != nil {
			return nil, fmt.Errorf("required production source tree %s: %w", subtree, err)
		}
		if !info.IsDir() {
			return nil, fmt.Errorf("required production source tree %s is not a directory", subtree)
		}
		err = filepath.WalkDir(start, func(path string, entry fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() {
				if entry.Name() == "testdata" {
					return filepath.SkipDir
				}
				return nil
			}
			if !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
				return nil
			}
			parsed, err := parser.ParseFile(fset, path, nil, 0)
			if err != nil {
				return err
			}
			relative, err := filepath.Rel(root, path)
			if err != nil {
				return err
			}
			packageKey := filepath.Dir(path) + "\x00" + parsed.Name.Name
			result = append(result, sourceFile{
				relative:   filepath.ToSlash(relative),
				packageKey: packageKey,
				parsed:     parsed,
			})
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

func collectStringConstants(files []sourceFile) map[string]map[string]ast.Expr {
	result := make(map[string]map[string]ast.Expr)
	for _, file := range files {
		byName := result[file.packageKey]
		if byName == nil {
			byName = make(map[string]ast.Expr)
			result[file.packageKey] = byName
		}
		for _, declaration := range file.parsed.Decls {
			general, ok := declaration.(*ast.GenDecl)
			if !ok || general.Tok != token.CONST {
				continue
			}
			for _, specification := range general.Specs {
				value, ok := specification.(*ast.ValueSpec)
				if !ok {
					continue
				}
				for index, name := range value.Names {
					if index < len(value.Values) {
						byName[name.Name] = value.Values[index]
					}
				}
			}
		}
	}
	return result
}

func evaluateString(expression ast.Expr, constants map[string]ast.Expr, visiting map[string]bool) (string, bool) {
	switch value := expression.(type) {
	case *ast.BasicLit:
		if value.Kind != token.STRING {
			return "", false
		}
		unquoted, err := strconv.Unquote(value.Value)
		return unquoted, err == nil
	case *ast.ParenExpr:
		return evaluateString(value.X, constants, visiting)
	case *ast.BinaryExpr:
		if value.Op != token.ADD {
			return "", false
		}
		left, leftOK := evaluateString(value.X, constants, visiting)
		right, rightOK := evaluateString(value.Y, constants, visiting)
		return left + right, leftOK && rightOK
	case *ast.Ident:
		if visiting[value.Name] {
			return "", false
		}
		constant, ok := constants[value.Name]
		if !ok {
			return "", false
		}
		visiting[value.Name] = true
		resolved, resolvedOK := evaluateString(constant, constants, visiting)
		delete(visiting, value.Name)
		return resolved, resolvedOK
	default:
		return "", false
	}
}

func isHTTPRegistration(function ast.Expr) bool {
	selector, ok := function.(*ast.SelectorExpr)
	return ok && (selector.Sel.Name == "Handle" || selector.Sel.Name == "HandleFunc")
}

func isRoutePattern(pattern string) bool {
	method, path, ok := strings.Cut(pattern, " ")
	if !ok || method == "" || path == "" || !strings.HasPrefix(path, "/") {
		return false
	}
	for _, character := range method {
		if character < 'A' || character > 'Z' {
			return false
		}
	}
	return true
}

func receiverName(function *ast.FuncDecl) string {
	if function.Recv == nil || len(function.Recv.List) != 1 {
		return ""
	}
	expression := function.Recv.List[0].Type
	if pointer, ok := expression.(*ast.StarExpr); ok {
		expression = pointer.X
	}
	identifier, _ := expression.(*ast.Ident)
	if identifier == nil {
		return ""
	}
	return identifier.Name
}

func isIdentifier(expression ast.Expr, expected string) bool {
	identifier, ok := expression.(*ast.Ident)
	return ok && identifier.Name == expected
}

func fatalf(format string, arguments ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", arguments...)
	os.Exit(1)
}

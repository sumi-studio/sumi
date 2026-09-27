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
			rangePatterns := literalRangePatterns(function.Body, constants[file.packageKey])
			ast.Inspect(function.Body, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok || len(call.Args) == 0 || !isHTTPRegistration(call.Fun) {
					return true
				}
				pattern, ok := evaluateString(call.Args[0], constants[file.packageKey], map[string]bool{})
				patterns := []string{pattern}
				if identifier, isIdent := call.Args[0].(*ast.Ident); isIdent && identifier.Obj != nil && identifier.Obj.Kind == ast.Var {
					// Bind by lexical object, not name: a local variable must not
					// accidentally resolve to a package constant or another loop.
					patterns, ok = rangePatterns[identifier.Obj]
				}
				if !ok {
					if privateDynamicRegistry && isIdentifier(call.Args[0], "pattern") {
						result.PrivateDynamicRegistries++
						return true
					}
					position := fset.Position(call.Args[0].Pos())
					fatalf("unresolved HTTP route expression at %s:%d; route parity cannot skip dynamic registrations", file.relative, position.Line)
				}
				position := fset.Position(call.Args[0].Pos())
				for _, pattern := range patterns {
					if !isRoutePattern(pattern) {
						fatalf("invalid HTTP route pattern %q at %s:%d", pattern, file.relative, position.Line)
					}
					result.Routes = append(result.Routes, discoveredRoute{
						File:    file.relative,
						Line:    position.Line,
						Pattern: pattern,
					})
				}
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

// Resolve only direct, immutable range bindings over literal []string lists.
// Unknown lists, reassignment and escaped addresses stay unresolved so parity
// cannot silently omit or misidentify registrations that depend on runtime data.
func literalRangePatterns(body *ast.BlockStmt, constants map[string]ast.Expr) map[*ast.Object][]string {
	result := make(map[*ast.Object][]string)
	ast.Inspect(body, func(node ast.Node) bool {
		loop, ok := node.(*ast.RangeStmt)
		if !ok || loop.Tok != token.DEFINE {
			return true
		}
		binding, ok := loop.Value.(*ast.Ident)
		if !ok || binding.Obj == nil || binding.Name == "_" {
			return true
		}
		list, ok := loop.X.(*ast.CompositeLit)
		if !ok {
			return true
		}
		sliceType, ok := list.Type.(*ast.ArrayType)
		if !ok || sliceType.Len != nil || !isIdentifier(sliceType.Elt, "string") {
			return true
		}
		var patterns []string
		for _, item := range list.Elts {
			pattern, ok := evaluateString(item, constants, map[string]bool{})
			if !ok {
				return true
			}
			patterns = append(patterns, pattern)
		}
		immutable := true
		containsBinding := func(expression ast.Expr) bool {
			if expression == nil {
				return false
			}
			found := false
			ast.Inspect(expression, func(node ast.Node) bool {
				if id, ok := node.(*ast.Ident); ok && id.Obj == binding.Obj {
					found = true
				}
				return true
			})
			return found
		}
		ast.Inspect(loop.Body, func(node ast.Node) bool {
			switch value := node.(type) {
			case *ast.AssignStmt:
				for _, lhs := range value.Lhs {
					if containsBinding(lhs) {
						immutable = false
					}
				}
			case *ast.IncDecStmt:
				immutable = immutable && !containsBinding(value.X)
			case *ast.RangeStmt:
				if value.Tok == token.ASSIGN && (containsBinding(value.Key) || containsBinding(value.Value)) {
					immutable = false
				}
			case *ast.UnaryExpr:
				if value.Op == token.AND && containsBinding(value.X) {
					immutable = false
				}
			}
			return true
		})
		if immutable {
			result[binding.Obj] = patterns
		}
		return true
	})
	return result
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
		if value.Obj != nil && value.Obj.Kind != ast.Con {
			return "", false
		}
		if visiting[value.Name] {
			return "", false
		}
		constant, ok := constants[value.Name]
		if value.Obj != nil {
			// A local constant may shadow a package constant with the same
			// name. Resolve its actual declaration, never the name alone.
			ok = false
			if declaration, isValue := value.Obj.Decl.(*ast.ValueSpec); isValue {
				for index, name := range declaration.Names {
					if name.Name == value.Name && index < len(declaration.Values) {
						constant, ok = declaration.Values[index], true
					}
				}
			}
		}
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

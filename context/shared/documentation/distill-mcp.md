# Distill MCP Tool Guidelines

Use Distill MCP tools for token-efficient operations.

## Rule 1: Smart File Reading

When reading source files for **exploration or understanding**:

```
mcp__distill__smart_file_read filePath="path/to/file.py"
```

**When to use native Read instead:**
- Before editing a file (Edit requires Read first)
- Configuration files: `.json`, `.yaml`, `.toml`, `.md`, `.env`

## Rule 2: Compress Verbose Output

After Bash commands that produce verbose output (>500 characters):

```
mcp__distill__auto_optimize content="<paste verbose output>"
```

## Rule 3: Code Execute SDK for Complex Operations

For multi-step operations, use `code_execute` instead of multiple tool calls (**98% token savings**):

```
mcp__distill__code_execute code="<typescript code>"
```

## SDK API Reference

Access via the `ctx` object in code_execute:

### Compression (`ctx.compress`)

| Method | Description |
|--------|-------------|
| `auto(content, hint?)` | Auto-detect content type and compress |
| `logs(logs)` | Summarize log output |
| `diff(diff)` | Compress git diffs |
| `semantic(content, ratio?)` | TF-IDF based compression |

### Code Analysis (`ctx.code`)

| Method | Description |
|--------|-------------|
| `parse(content, lang)` | Parse to AST structure |
| `extract(content, lang, {type, name})` | Extract specific element |
| `skeleton(content, lang)` | Get signatures only (no bodies) |

### File Operations (`ctx.files`)

| Method | Description |
|--------|-------------|
| `read(path)` | Read file contents |
| `exists(path)` | Check if file exists |
| `glob(pattern)` | Find files by pattern |

### Git Operations (`ctx.git`)

| Method | Description |
|--------|-------------|
| `diff(ref?)` | Get git diff |
| `log(limit?)` | Commit history |
| `status()` | Repository status |
| `branch()` | Branch info |
| `blame(file, line?)` | Git blame |

### Search (`ctx.search`)

| Method | Description |
|--------|-------------|
| `grep(pattern, glob?)` | Search pattern in files |
| `symbols(query, glob?)` | Search functions, classes |
| `files(pattern)` | Find files by pattern |
| `references(symbol, glob?)` | Find symbol references |

### Analysis (`ctx.analyze`)

| Method | Description |
|--------|-------------|
| `dependencies(file)` | Analyze imports/exports |
| `callGraph(fn, file, depth?)` | Build call graph |
| `exports(file)` | Get file exports |
| `structure(dir?, depth?)` | Directory structure with analysis |

### Utilities (`ctx.utils`)

| Method | Description |
|--------|-------------|
| `countTokens(text)` | Count tokens |
| `detectType(content)` | Detect content type |
| `detectLanguage(path)` | Detect language from path |

## Quick Reference

| Action | Tool |
|--------|------|
| Read code for exploration | `mcp__distill__smart_file_read filePath="file.py"` |
| Get a function/class | `smart_file_read` with `target={"type":"function","name":"myFunc"}` |
| Compress build errors | `mcp__distill__auto_optimize content="..."` |
| Multi-step operations | `mcp__distill__code_execute code="return ctx.files.glob('src/**/*.py')"` |
| Before editing | Use native `Read` tool |

## Examples

```typescript
// Get skeletons of all Python files
const files = ctx.files.glob("portolan_cli/**/*.py").slice(0, 5);
return files.map(f => ({
  file: f,
  skeleton: ctx.code.skeleton(ctx.files.read(f), "python")
}));

// Compress and analyze logs
const logs = ctx.files.read("server.log");
return ctx.compress.logs(logs);

// Extract a specific function
const content = ctx.files.read("portolan_cli/cli.py");
return ctx.code.extract(content, "python", { type: "function", name: "main" });
```

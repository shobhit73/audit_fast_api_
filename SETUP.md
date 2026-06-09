# SETUP.md — Run the Audit MCP Server locally with Claude Desktop

This is the step-by-step SOP for installing this tool on a fresh machine and
connecting it to the **Claude Desktop** app. No web server, no Vercel, no cloud —
everything runs locally and reads/writes files on your own computer.

When you're done, Claude Desktop will have ~34 payroll-audit tools (ADP / Paycom /
Uzio census, payment, withholding, prior-payroll, consolidated audits, etc.).

---

## 1. Prerequisites

| Requirement | How to get it | Verify |
|---|---|---|
| **Python 3.10+** (3.11 recommended) | Windows: install from [python.org](https://www.python.org/downloads/) and tick **"Add Python to PATH"**. macOS: `brew install python@3.11`. | `python --version` (Windows) / `python3 --version` (macOS) |
| **Claude Desktop app** | [claude.ai/download](https://claude.ai/download) | Open it once and sign in |
| **This repository** | Copy/clone the `audit_fast_api` folder somewhere stable (e.g. `C:\Tools\audit_fast_api` or `~/audit_fast_api`). Avoid spaces in the path if you can. | The folder contains `mcp_server.py` |

> Windows note: if `python` opens the Microsoft Store, Python isn't really on PATH.
> Reinstall from python.org with "Add to PATH" checked, or use the full path to
> `python.exe` (see step 4).

---

## 2. Install the Python dependencies

Open a terminal **in the repo folder** and install:

```bash
# Windows (PowerShell or CMD)
cd C:\Tools\audit_fast_api
python -m pip install -r requirements.txt

# macOS / Linux
cd ~/audit_fast_api
python3 -m pip install -r requirements.txt
```

This installs exactly what the server needs: `mcp`, `pandas`, `numpy`,
`openpyxl`, `xlsxwriter`, `duckdb`, `pyyaml`.

> **Recommended (clean) install — a virtual environment.** Keeps these packages
> from colliding with other Python projects:
> ```bash
> python -m venv .venv
> # Windows:  .venv\Scripts\activate
> # macOS:    source .venv/bin/activate
> python -m pip install -r requirements.txt
> ```
> If you use a venv, point Claude Desktop at the venv's Python in step 4
> (`...\.venv\Scripts\python.exe` / `.../.venv/bin/python`).

### Quick self-test

```bash
python -c "import mcp_server; print('OK -', mcp_server.server.name)"
```

Expected: `OK - audit-tool-server`. If you see `ModuleNotFoundError`, the install
didn't land in the Python you just ran — re-run step 2 with the **same** Python you
intend to use in step 4.

---

## 3. The "Audit Files" inbox

Every report the tools produce is written to a single folder, and you drop your
input files there too. By default this is:

```
<your home folder>\Desktop\Audit Files     (Windows)
~/Desktop/Audit Files                       (macOS)
```

You don't have to create it — the server makes it on first use. To use a different
location, set the `AUDIT_INBOX` environment variable (see the `env` block in step 4).

---

## 4. Register the server in Claude Desktop

Claude Desktop reads a JSON config file and launches each MCP server for you.

**Config file location:**
- **Windows:** `%APPDATA%\Claude\claude_desktop_config.json`
  (paste `%APPDATA%\Claude` into the File Explorer address bar)
- **macOS:** `~/Library/Application Support/Claude/claude_desktop_config.json`

If the file doesn't exist, create it. Add an `audit-tools` entry under `mcpServers`:

```json
{
  "mcpServers": {
    "audit-tools": {
      "command": "C:\\Path\\To\\python.exe",
      "args": ["C:\\Tools\\audit_fast_api\\mcp_server.py"],
      "env": {
        "AUDIT_INBOX": "C:\\Users\\YOURNAME\\Desktop\\Audit Files",
        "STATE_TAX_MASTER_PATH": "C:\\Users\\YOURNAME\\Downloads\\State Tax Code.csv"
      }
    }
  }
}
```

**macOS example:**

```json
{
  "mcpServers": {
    "audit-tools": {
      "command": "/usr/bin/python3",
      "args": ["/Users/yourname/audit_fast_api/mcp_server.py"],
      "env": {
        "AUDIT_INBOX": "/Users/yourname/Desktop/Audit Files"
      }
    }
  }
}
```

Rules that make this work the first time:
- **Use absolute paths** for both `command` (the Python) and the `mcp_server.py` arg.
  Relative paths and a bare `"python"` often fail under Claude Desktop because it
  doesn't run from your repo folder or inherit your shell PATH.
- On Windows, **escape backslashes** in JSON (`\\`) or use forward slashes (`/`).
- To find your Python's absolute path: `where python` (Windows) / `which python3`
  (macOS). If you made a venv, use the venv's python.
- `env` is optional. `AUDIT_INBOX` overrides the default inbox.
  `STATE_TAX_MASTER_PATH` is only needed for the `adp_prior_payroll_setup_helper`
  tool (it needs the State Tax Code master CSV); you can also pass that path to the
  tool directly when you use it.
- Make sure the whole file is **valid JSON** (no trailing commas). Paste it into a
  JSON validator if unsure.

---

## 5. Restart and verify

1. **Fully quit** Claude Desktop (system-tray / menu-bar quit, not just close the
   window) and reopen it.
2. Look for the MCP **tools / plug icon** in the chat box — `audit-tools` should be
   listed with its tools enabled.
3. Test it: put any `.xlsx`/`.csv` in your **Audit Files** folder and ask Claude:
   *"List the files in my audit inbox."* It should call `list_audit_files` and show
   them.

---

## 6. Day-to-day usage

1. Drop the source exports (ADP/Paycom/Uzio) into **Audit Files** (or ask Claude to
   `copy_to_audit_inbox` from Downloads).
2. Ask Claude to run the audit you want (e.g. *"Run the ADP consolidated audit on
   these files"*).
3. The tool writes the report **back into Audit Files** as a timestamped `.xlsx`
   (and a `.csv` for single-sheet outputs). Claude returns a summary + the file path;
   open the file from the folder.

---

## 7. Troubleshooting

| Symptom | Fix |
|---|---|
| `audit-tools` doesn't appear in Claude Desktop | Config JSON is invalid or paths are wrong. Validate the JSON; use absolute paths; fully quit & reopen Claude Desktop. |
| Server shows an error / red status | Run the step-2 self-test in a terminal to see the real error. Usually a missing dependency (`pip install -r requirements.txt`) or wrong Python in `command`. |
| `ModuleNotFoundError: No module named 'duckdb'` (or pandas, mcp, …) | You installed into a different Python than the one in `command`. Install with the exact Python you point Claude Desktop at, or use a venv and point at its python. |
| Reports don't show up | Ask Claude to `list_audit_files`. Check `AUDIT_INBOX` in your config matches where you're looking. |
| `adp_prior_payroll_setup_helper` fails on the tax master | Provide the State Tax Code CSV: set `STATE_TAX_MASTER_PATH` in `env`, or pass `state_tax_master_path` when invoking the tool. |
| Running `python mcp_server.py` "just hangs" | That's correct — it's waiting for Claude Desktop to talk to it over stdin. It's not meant to be run by hand. |

---

## 8. What this is (and isn't), in one line

A **local, stdio-only MCP server** that exposes the payroll-audit toolset to the
Claude Desktop app on your machine. There is no FastAPI, no Vercel, no HTTP port —
just `python mcp_server.py` launched by Claude Desktop, reading and writing files in
your Audit Files folder. Architecture details for developers live in
[CLAUDE.md](CLAUDE.md).

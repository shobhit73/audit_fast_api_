# BUNDLE.md — Packaging this server as a one-click `.mcpb` for Claude Desktop

An **`.mcpb`** (MCP Bundle) is a zip that Claude Desktop installs with a double-click:
the user is prompted for the config fields (here: the Audit Files folder and the
optional State Tax CSV), and the server is registered automatically — no editing
`claude_desktop_config.json` by hand.

This repo ships a [`manifest.json`](manifest.json) (manifest version `0.3`) that wires:
- the **entry point** `mcp_server.py` over stdio,
- `AUDIT_INBOX` and `STATE_TAX_MASTER_PATH` as prompted **user-config** fields,
- `PYTHONPATH=${__dirname}/lib` so bundled dependencies are found.

> ### ⚠️ Read this before you assume "one-click = no setup"
> This is a **Python** server built on `pandas`, `numpy`, and `duckdb` — packages with
> **native, OS-specific binaries**. That has two consequences an `.mcpb` does *not*
> magically remove:
> 1. **The user still needs Python ≥ 3.10 installed** and on PATH. The bundle ships
>    your *libraries*, not a Python *interpreter*.
> 2. **The vendored `lib/` is platform-specific.** A `lib/` built on Windows will not
>    run on macOS/Linux. You must build **one `.mcpb` per operating system** (and CPU
>    arch), each packed on a matching machine.
>
> If every teammate already has Python, the plain-repo + [SETUP.md](SETUP.md) route is
> simpler and fully cross-platform. Reach for `.mcpb` when you specifically want the
> prompted-config + auto-register UX for non-technical users on a **known OS**.

## Build steps (run on the OS you're targeting)

```bash
# 1. Install the MCPB CLI (one time)
npm install -g @anthropic-ai/mcpb        # or: npx @anthropic-ai/mcpb <cmd>

# 2. Vendor the Python dependencies into ./lib for THIS OS/arch.
#    (pandas/numpy/duckdb wheels are native — must match the target machine.)
python -m pip install --target lib -r requirements.txt

# 3. (optional) sanity-check the manifest
mcpb validate manifest.json

# 4. Pack the bundle. .mcpbignore controls what's excluded; ./lib IS included.
mcpb pack
#    -> produces audit-tools.mcpb
```

To include `lib/` in the pack, make sure the `lib/` line stays **commented out** in
[`.mcpbignore`](.mcpbignore) (it is by default).

## Install (the user's side)

1. Send them the `.mcpb` for their OS.
2. They double-click it (or drag it onto Claude Desktop → Settings → Extensions).
3. Claude Desktop prompts for the **Audit Files folder** (defaults to their Desktop)
   and the optional **State Tax Code CSV**, then enables the tools.
4. Done — no JSON editing, no `pip install`. (They still need Python ≥ 3.10 present.)

## Maintenance

- The tool list is discovered at runtime (`tools_generated: true`), so adding/removing
  MCP tools needs **no manifest change**.
- Bump `version` in `manifest.json` when you cut a new bundle.
- Rebuild `lib/` whenever `requirements.txt` changes, on each target OS.

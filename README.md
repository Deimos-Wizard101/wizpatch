# wizpatch

Rust port of the [wizdiff](https://pypi.org/project/wizdiff/) library, scoped
to what is needed to patch Wizard101.

## Library

```rust
use wizpatch::{get_patch_urls, get_file_list_records, Game, Platform};

let urls = smol::block_on(get_patch_urls(Game::Wizard101, Platform::Windows))?;
let records = smol::block_on(get_file_list_records(&urls.file_list_url))?;
```

## CLI

```
wizpatch urls           # print file-list + base URLs
wizpatch revision       # print current revision
wizpatch list --limit 5 # list file records
```

## Scope

Ported:

- TCP handshake to query patch URLs (`webdriver`)
- HTTP fetch with optional `Range` header
- XML file-list parser (`dml`)
- Revision extraction (`utils`)
- File-list aggregation (`notifier`)

Intentionally omitted (unused by MilkLauncher's Wizard101 patch path):

- Binary DML parser (Pirate101)
- WAD journal CRC parsing
- SQLite-backed revision/delta DB
- Update notifier diff machinery

## License

GPL-3.0-or-later.

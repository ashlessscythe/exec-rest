# SAP Auto Runner

A Windows-only Rust CLI tool that spawns `sap_auto.exe`, watches for output files, and uploads them to a PHP endpoint on your intranet.

## Features

- **Process Management**: Spawns and monitors `sap_auto.exe` with configurable arguments
- **File Watching**: Monitors output directory for timestamped files like `20250115143022_y_149-ALL.txt`
- **Smart File Selection**: Finds newest file by modification time or timestamp prefix
- **File Stability**: Waits for files to be fully written before processing
- **Data Transformation**: Optional TSV/CSV normalization with header parsing
- **Multiple Upload Modes**: Supports multipart form-data and JSON base64 uploads
- **Authentication**: Bearer token, basic auth, or no authentication
- **Retry Logic**: Exponential backoff for failed uploads
- **Archiving**: Optional file archiving after successful upload
- **Looping**: Configurable interval-based execution

## Building

This is a Windows-only application. Build with:

```bash
cargo build --release
```

The executable will be created at `target/release/sap_auto_runner.exe`.

## Configuration

Create a `config.toml` file (see `config.toml` for a complete example):

```toml
[extraction]
executable = "C:\\tools\\sap_auto.exe"
subcommand = "run-sequence"
args = ["--plant", "001", "--cols", "plant,material,delivery"]

[files]
output_dir = "C:\\data\\outputs"
file_glob = "*_y_001-ALL.txt"
filename_timestamp_prefix = true
stable_size_check_secs = 2

[api]
endpoint = "https://api.example.com/upload.php"
mode = "multipart"
field_name = "file"
auth = "none"

[loop]
interval_seconds = 300  # 0 = run once, >0 = loop forever
```

## Usage

```bash
# Run with default config.toml
sap_auto_runner.exe

# Run with custom config
sap_auto_runner.exe --config C:\\cfg\\runner.toml

# Override specific settings
sap_auto_runner.exe --endpoint https://api.example.com/upload --mode json_base64 --verbose

# Run once (no looping)
sap_auto_runner.exe --loop-interval 0
```

## File Format Support

The tool expects TSV files with header rows like:

```
In-Transfer (Push Delivery) Materials Report
Acme Manufacturing Corp

User                                   TESTUSER
Run Date   :                           2025-01-15
Run Time   :                           14:30:22

        Plant   Delivery        Material
        PLT01   9876543210      55512345
        PLT02   9876543211      55512346
```

## Upload Modes

### Multipart (default)

Files are uploaded as `multipart/form-data` with the configured field name.

**PHP Backend Example:**

```php
<?php
if (!isset($_FILES['file'])) {
    http_response_code(400);
    echo "no file";
    exit;
}
$fn = $_FILES['file']['name'];
$tmp = $_FILES['file']['tmp_name'];
$dest = __DIR__ . "/uploads/" . basename($fn);
if (!move_uploaded_file($tmp, $dest)) {
    http_response_code(500);
    echo "move failed";
    exit;
}
echo "ok";
```

### JSON Base64

Files are base64-encoded and sent as JSON.

**PHP Backend Example:**

```php
<?php
$raw = file_get_contents('php://input');
$body = json_decode($raw, true);
if (!$body || !isset($body['filename']) || !isset($body['data'])) {
    http_response_code(400);
    echo "bad json";
    exit;
}
$dest = __DIR__ . "/uploads/" . basename($body['filename']);
$data = base64_decode($body['data'], true);
if ($data === false) {
    http_response_code(400);
    echo "bad base64";
    exit;
}
if (file_put_contents($dest, $data) === false) {
    http_response_code(500);
    echo "write failed";
    exit;
}
echo "ok";
```

## Data Transformation

When `[transform].enabled = true`, the tool can normalize TSV files:

- Skips configured header rows
- Validates header content
- Removes duplicate rows (optional)
- Trims whitespace (optional)
- Outputs clean TSV or CSV format

## Error Handling

- **Process Errors**: Non-zero exit codes are logged but don't stop execution
- **File Errors**: Missing or unreadable files are logged and skipped
- **Upload Errors**: Retryable errors (5xx, timeouts) are retried with exponential backoff
- **Client Errors**: 4xx errors are not retried

## Testing

Run the test suite:

```bash
cargo test
```

Tests include:

- File selection and timestamp parsing
- Data transformation with various formats
- Upload error handling and retry logic

## Dependencies

- **tokio**: Async runtime
- **reqwest**: HTTP client
- **clap**: CLI argument parsing
- **serde/toml**: Configuration management
- **glob**: File pattern matching
- **chrono**: Timestamp handling
- **base64**: Base64 encoding
- **encoding_rs**: Character encoding support

## License

This project is proprietary software for internal use.

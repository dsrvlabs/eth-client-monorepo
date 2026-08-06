# Vendored `google.rpc` protos (CC-18a / ADR-P1-14)

Phase 0 shipped no BSR dependency and codegen is `protox` over local files. Cursor /
bootstrap failures need `FAILED_PRECONDITION` + `google.rpc.ErrorInfo{reason}`, which
requires these two files on the local include path. Pulling them from the BSR would add
`buf.lock` *and* a `buf export` step — deliberately avoided (Architecture §7.6).

| Field | Value |
|---|---|
| **Files** | `status.proto`, `error_details.proto` |
| **Package** | `google.rpc` |
| **Upstream** | [googleapis/googleapis](https://github.com/googleapis/googleapis) |
| **Paths** | `google/rpc/status.proto`, `google/rpc/error_details.proto` |
| **Commit** | `02362883cd16428e5f57fa60ca8d8f60dafcdba7` |
| **Fetched** | 2026-08-06 |
| **License** | Apache-2.0 (see file headers) |
| **Well-known imports** | `google/protobuf/any.proto`, `google/protobuf/duration.proto` — resolved by `protox`'s built-in `GoogleFileResolver`, not vendored here |

## Refresh

```bash
COMMIT=<googleapis-sha>
curl -sL "https://raw.githubusercontent.com/googleapis/googleapis/${COMMIT}/google/rpc/status.proto" \
  -o proto/third_party/google/rpc/status.proto
curl -sL "https://raw.githubusercontent.com/googleapis/googleapis/${COMMIT}/google/rpc/error_details.proto" \
  -o proto/third_party/google/rpc/error_details.proto
# Update the commit SHA in this README.
```

## buf

`proto/buf.yaml` lists `third_party` under both `lint.ignore` and `breaking.ignore`. These
files are not our contract surface; they fail `DEFAULT`/`FILE` rules by design
(`PACKAGE_DIRECTORY_MATCH`, `PACKAGE_VERSION_SUFFIX`, multi-value `go_package`, …).

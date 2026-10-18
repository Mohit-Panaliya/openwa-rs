# Windows build toolchain

Rust's default Windows host is `x86_64-pc-windows-msvc`, which needs the MSVC
linker (`link.exe`) from Visual Studio Build Tools. On a machine without Visual
Studio, `cargo build` fails with `link.exe not found` (or, after switching to the
GNU host, with `dlltool ... CreateProcess`).

This project builds on Windows with the **GNU host toolchain plus a MinGW-w64
binutils distribution**. Both steps are one-time.

## 1. GNU host toolchain

```powershell
rustup toolchain install stable-x86_64-pc-windows-gnu --profile minimal
rustup default stable-x86_64-pc-windows-gnu
rustup component add clippy rustfmt --toolchain stable-x86_64-pc-windows-gnu
```

The toolchain ships a MinGW linker and a `dlltool.exe`, but that `dlltool` shells
out to `as.exe`, which the minimal component does not include — the build then
fails with `dlltool could not create import library ... CreateProcess`
(triggered by `windows-sys`/`parking_lot_core` using `raw-dylib`).

## 2. Binutils (provides `as.exe`)

```powershell
winget install --id BrechtSanders.WinLibs.POSIX.MSVCRT -e --accept-source-agreements --accept-package-agreements
```

This installs WinLibs MinGW-w64 (GCC + binutils, MSVCRT runtime, matching the
rust-mingw runtime) and adds its `bin` directory to the user `PATH`, which
supplies `as.exe`, `dlltool.exe`, `gcc.exe` and friends.

Verify in a fresh shell:

```powershell
where.exe cargo
where.exe dlltool
where.exe as.exe
```

## Notes

- `reqwest` uses the `native-tls` feature so TLS goes through SChannel and no
  OpenSSL build is required on this target.
- No dependency in this workspace compiles C code, so the GNU host is a
  drop-in replacement here. If a future dependency needs MSVC-only features,
  install Visual Studio Build Tools and switch back with
  `rustup default stable-x86_64-pc-windows-msvc`.
- Do not pin the GNU triple in `rust-toolchain.toml`: CI runs on Linux and uses
  the default stable host.
- Dependency versions resolved on this machine: rustc/cargo 1.99.1, GCC 16.2.0
  (WinLibs), Node 24.22.0.

{
  description = "A very basic flake";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    flake-utils.url = github:numtide/flake-utils;
    flake-utils.inputs.nixpkgs.follows = "nixpkgs";

    # contains more recent rust 
    rust-overlay.url = "github:oxalica/rust-overlay";
  };

  outputs = { self, nixpkgs, flake-utils, rust-overlay, ... }: 
    flake-utils.lib.eachDefaultSystem  (system: 
      let 
        overlays = [ (import rust-overlay) ];
        pkgs = import nixpkgs {
          inherit system overlays;
        };
        myMkShell = pkgs.mkShell.override { stdenv = pkgs.clang12Stdenv; };
      in {
        devShell = myMkShell rec {
          name = "bloom-lake";
          nativeBuildInputs = with pkgs; [ 
            flatbuffers
            protobuf
            gnuplot
          ];
          buildInputs = with pkgs; [
            protobuf
            cargo-watch
            rust-analyzer
            rustfmt
            clippy
            # see https://discourse.nixos.org/t/rust-src-not-found-and-other-misadventures-of-developing-rust-on-nixos/11570/7?u=samuela.
            (rust-bin.stable.latest.default.override {
              extensions = ["rust-src"];
            })
            sccache
          ];
          LIBCLANG_PATH="${pkgs.llvmPackages_12.clang-unwrapped.lib + /lib}";
          LD_LIBRARY_PATH = pkgs.lib.makeLibraryPath [ pkgs.openssl ];
          RUSTC_WRAPPER = "${pkgs.sccache}/bin/sccache";
        };
      });
}

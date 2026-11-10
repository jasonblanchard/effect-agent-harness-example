{
  inputs = {
    nixpkgs.url = "github:cachix/devenv-nixpkgs/rolling";
    systems.url = "github:nix-systems/default";
    devenv.url = "github:cachix/devenv";
    devenv.inputs.nixpkgs.follows = "nixpkgs";
  };

  nixConfig = {
    extra-trusted-public-keys = "devenv.cachix.org-1:w1cLUi8dv3hnoSPGAuibQv+f9TZLr6cv/Hm9XgU50cw=";
    extra-substituters = "https://devenv.cachix.org";
  };

  outputs =
    {
      self,
      nixpkgs,
      devenv,
      systems,
      ...
    }@inputs:
    let
      forEachSystem = nixpkgs.lib.genAttrs (import systems);
    in
    {
      devShells = forEachSystem (
        system:
        let
          pkgs = nixpkgs.legacyPackages.${system};
        in
        {
          default = devenv.lib.mkShell {
            inherit inputs pkgs;
            modules = [
              {
                languages.javascript = {
                  enable = true;
                  package = pkgs.nodejs_24;
                  pnpm.enable = true;
                };

                services.postgres = {
                  enable = true;
                  listen_addresses = "127.0.0.1";
                  initialDatabases = [ { name = "effect_agent_harness_example"; } ];
                };

                # No credentials: devenv's Postgres trusts local connections, and the driver defaults to $USER.
                env.DATABASE_URL = "postgres://127.0.0.1:5432/effect_agent_harness_example";

                processes.web.exec = "pnpm dev";
              }
            ];
          };
        }
      );
    };
}

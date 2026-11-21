# Oracle Cloud deployment

This records the configuration established during the proof-of-concept deployment. It is an operational reference, not evidence that the VM has been rechecked since that work. Connect from the configured Windows machine with `ssh oracle`. The VM is `instance-20261117-2229`, Ubuntu 24.04 ARM64, in UK South (London).

| Item | Established configuration |
| --- | --- |
| Public address | `145.241.201.25` (ephemeral) |
| HTTPS hostname | `145-241-201-25.nip.io` |
| Current Git checkout | `/home/ubuntu/xsp-pulse-server-git` |
| Original deployment retained for rollback | `/home/ubuntu/xsp-pulse-server` |
| Original worker backup | `/home/ubuntu/xsp-pulse-server/tasty_worker.py.pre-git-backup` |
| Virtual environment, shared by current deployment | `/home/ubuntu/xsp-pulse-server/.venv` |
| Uvicorn executable | `/home/ubuntu/xsp-pulse-server/.venv/bin/uvicorn` |
| systemd unit and backup | `/etc/systemd/system/xsppulse.service`; `/etc/systemd/system/xsppulse.service.pre-git-backup` |
| systemd environment file | `/home/ubuntu/xsp-pulse-server-git/.env` |

The `xsppulse.service` unit runs as `ubuntu`, with `WorkingDirectory=/home/ubuntu/xsp-pulse-server-git`, `Restart=always`, and `RestartSec=5`. Its start command is:

```text
/home/ubuntu/xsp-pulse-server/.venv/bin/uvicorn server:app --host 127.0.0.1 --port 8000
```

**The Git checkout currently reuses the original deployment's virtual environment.** Changing packages there affects both deployments and can undermine rollback. Do not recreate or update it casually.

Caddy terminates HTTPS on port 443 and proxies to `127.0.0.1:8000`. Ports 80 and 443 are allowed through the Oracle VCN and Ubuntu firewall. The exact Caddyfile contents were not recorded in the repository; inspect the live configuration before changing it. Because the public IP is ephemeral and encoded in the hostname and client URL, an IP change requires coordinated DNS/hostname, TLS, firewall and client-configuration review.

## Configuration and deploy access

The repository-specific Git configuration uses the read-only GitHub SSH deploy key at `/home/ubuntu/.ssh/xsppulse_github` for `git@github.com:SlightlyGone/xsp-pulse-server.git`. Keep its private key restricted to the deployment account; it is for reading the server repository, not pushing changes. Verify access without printing private key material. Avoid overwriting unrelated SSH configuration.

The systemd `EnvironmentFile` supplies `TASTYTRADE_CLIENT_SECRET`, `TASTYTRADE_REFRESH_TOKEN`, and `APP_API_KEY`. It is local to the VM and must remain outside Git, readable only by authorized users and the service as appropriate. OAuth credentials stay on the server; clients use only the separate API bearer key. Do not print the environment file, secrets, authorization headers, raw OAuth responses or credential fingerprints into terminals, logs or tickets. Set an explicit, strong `APP_API_KEY`; do not rely on the application's fallback value. Coordinate API-key changes with every client.

## Verify

The optional Phase 2 watchdog remains disabled unless `DXLINK_WATCHDOG_ENABLED=true`
is supplied at worker construction. Review [configuration, calendar expiry, timing
and rollback](WATCHDOG.md) before intentionally enabling it. It performs recovery
inside Python, never restarts systemd, and introduces the `RECOVERING` status.
The `tzdata` and explicit `anyio` requirements support timezone handling and SDK
cleanup; account for dependencies in the existing isolated-environment rollout
plan. Disabling the flag on the next intentional service start restores passive
freshness reporting while retaining safer cleanup and fixed five-second retries.
Watchdog-only settings are ignored when disabled. Cleanup pending for 30 seconds
warns without forcing replacement; expired calendar coverage warns at most hourly
when enabled and continues to suppress stall recovery.
This document change does not deploy or change the VM's environment.

On the VM, check service and proxy health without displaying environment values:

```sh
sudo systemctl status xsppulse.service --no-pager
sudo systemctl status caddy --no-pager
sudo journalctl -u xsppulse.service -n 100 --no-pager
curl --fail --silent --show-error https://145-241-201-25.nip.io/health
```

The health response should be `{"ok":true}`; this alone does not prove streaming. From a trusted interactive machine, test each protected symbol using a prompt rather than putting the key in shell history or a command-line argument:

```sh
python3 - <<'PY'
from getpass import getpass
from urllib.request import Request, urlopen

key = getpass('API key: ')
for symbol in ('SPY', 'XSP', 'ES'):
    request = Request('https://145-241-201-25.nip.io/price/xsp?symbol=' + symbol,
                      headers={'Authorization': 'Bearer ' + key})
    with urlopen(request, timeout=10) as response:
        print(symbol, response.read().decode())
PY
```

Check each response for its expected symbol, status, populated prices and a plausible `updated_at`. With the freshness-reporting change deployed, API reads report `STALE` when the last local receipt exceeds its threshold, or `WAITING_FOR_DATA` before an initial event. Neither status establishes connection failure: quiet markets, closures and scheduled breaks can yield old or absent observations. See [freshness configuration and limitations](../README.md#observation-freshness) before interpreting these as an outage; thresholds are optional environment settings and require worker restart to take effect. The ES response displays the selected contract symbol. Review logs before sharing them: third-party libraries or older versions may emit sensitive details.

## Update from GitHub

On the VM, inspect the unit, current commit and clean working tree before changing anything. Fetch and review the incoming diff and dependencies. The existing environment is shared with the rollback deployment; do not install changed dependencies into it without a separate plan.

```sh
cd /home/ubuntu/xsp-pulse-server-git
git status --short
git rev-parse HEAD
git fetch origin
git status -sb
# Review the intended upstream changes and deployment impact before proceeding.
git diff HEAD '@{upstream}'
git pull --ff-only
sudo systemctl restart xsppulse.service
sudo systemctl status xsppulse.service --no-pager
```

Record the previous commit before updating. If the branch has no configured upstream, resolve that explicitly before pulling. Run appropriate tests before deployment and repeat the verification above after restart. These commands are maintenance instructions; they were not run to create this document.

## Rollback

Choose the rollback approach after inspecting the current unit and backups. For a code-only rollback, if the working tree is clean, restore the recorded prior commit in the checkout using an operator-approved Git procedure, restart `xsppulse.service`, and verify. Avoid destructive resets that would discard local changes.

The retained alternative is the original deployment and backed-up unit. Before restoring the unit, inspect it and confirm its working directory, environment-file path, executable, credentials availability, and compatibility with the **current shared virtual environment**; the original files alone do not guarantee a working rollback. Preserve the current unit securely, then, when intentionally rolling back, restore the backup unit, run `sudo systemctl daemon-reload`, restart `xsppulse.service`, and perform the verification above. Do not print environment files or copy secrets into Git. The separate `tasty_worker.py.pre-git-backup` is an additional worker backup, not by itself a full deployment rollback.

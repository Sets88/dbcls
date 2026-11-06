[← Back to README](../README.md)

# Screen lock and password safety

## Screen Lock

DbCls can lock the terminal after a period of inactivity and require re-authentication before continuing. Three parameters must all be provided to enable locking:

| Parameter | Description |
|-----------|-------------|
| `--lock-init-command CMD` | Shell command that receives a random secret via stdin and outputs a challenge code to stdout |
| `--lock-timeout SECONDS` | Inactivity timeout in seconds (float). Idle time is the larger of the monotonic and the wall-clock delta, so the lock engages whichever one runs out first — neither a system sleep (monotonic stops) nor an NTP jump (wall clock moves) can hold it off |
| `--lock-check-command CMD` | Shell command that receives the challenge code via stdin and outputs the recovered secret to stdout |

### How it works

1. **Startup** — DbCls generates a random secret, pipes it to `--lock-init-command`, and stores the output as the *code*.
2. **Inactivity** — after `--lock-timeout` seconds without a keypress the screen locks. The timeout resets on every keypress while the editor is active.
3. **Unlock** — a lock overlay is shown. Press `Enter` or `Space` to attempt unlock:
   - The stored *code* is piped to `--lock-check-command`.
   - If the output matches the original *secret*, the session is restored and a new secret/code pair is generated.
   - On failure the remaining attempt count decreases. After 3 failed attempts the application exits.
4. Press `Ctrl+Q` at any time to exit the application without unlocking.

### Configuration

All three parameters can be set via CLI flags, JSON config file, or environment variables. CLI flags take precedence over env vars, which take precedence over the config file.

**Config file** (`config.json`):
```json
{
    "host": "127.0.0.1",
    "engine": "mysql",
    "dbname": "mydb",
    "lock_init_command": "ssh-crypt -e",
    "lock_timeout": 300,
    "lock_check_command": "ssh-crypt -d"
}
```

**Environment variables:**
```bash
export DBCLS_LOCK_INIT_COMMAND="ssh-crypt -e"
export DBCLS_LOCK_TIMEOUT=300
export DBCLS_LOCK_CHECK_COMMAND="ssh-crypt -d"
```

### Example with ssh-crypt

[ssh-crypt](https://github.com/Sets88/ssh-crypt) encrypts data with your SSH public key and decrypts it with your private key (via the SSH agent). This makes it a natural fit for the lock protocol:

```bash
dbcls -c config.json mydb.sql \
  --lock-init-command  "ssh-crypt -e" \
  --lock-timeout       300 \
  --lock-check-command "ssh-crypt -d"
```

What happens:
- On startup `ssh-crypt -e` encrypts the random secret → stores the ciphertext as the code.
- On unlock `ssh-crypt -d` decrypts the ciphertext via the SSH agent → the result must equal the original secret.

Because the SSH agent holds the private key in memory, `ssh-crypt -d` succeeds only while your SSH agent session is alive. If the agent is cleared (e.g. `ssh-add -D`) unlock will fail and the application will exit after three attempts.

You can put the lock settings in the config file and decrypt it on the fly:

```bash
#!/bin/bash
dbcls -c <(ssh-crypt -d -s "$ENCRYPTED_CONFIG") mydb.sql
```

### Error handling

If `--lock-init-command` exits with a non-zero code or produces no output at startup, DbCls prints an error and does not open the editor:

```
Error: --lock-init-command exited with code 1: <stderr output>
Error: --lock-init-command produced no output
```

## Password safety
The simplest way not to have a password anywhere on disk is to not save one:
`"ask_password": true` in a connection block (the default for a connection
[described in the editor](configuration.md#describing-a-connection-in-the-editor)) makes dbcls
ask for it when the connection is first used and keep it in memory only.

To ensure password safety, I recommend using the project [ssh-crypt](https://github.com/Sets88/ssh-crypt) to encrypt your config file. This way, you can store your password securely and use it with dbcls.

Caveats:
- If you keep the raw password in a shell script, it will be visible to other users on the system.
- Even if you encrypt your password inside a shell script, if you pass it to dbcls via the command line, it will be visible in the process list.

To avoid this, you can use this technique:

```bash
#!/bin/bash

ENC_PASS='{V|B;*R$Ep:HtO~*;QAd?yR#b?V9~a34?!!sxqQT%{!x)bNby^5'
PASS_DEC=$(ssh-crypt -d -s "$ENC_PASS")

CONFIG=$(cat <<EOF
{
    "host": "127.0.0.1",
    "username": "user",
    "password": "$PASS_DEC",
    "dbname": "mydb",
    "engine": "mysql"
}
EOF
)

dbcls -c <(echo "$CONFIG") mydb.sql
```

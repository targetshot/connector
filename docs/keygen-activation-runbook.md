# Keygen Activation Runbook

This runbook covers the operator workflow for Club Plus licenses in `ts-connect`.

## Model

- `beta.targetshot.app` owns the club-side billing and license assignment.
- `ts-connect` owns the local machine activation.
- One club license is expected to be bound to one active `ts-connect` machine.

That means:

- a valid Club Plus key from beta is necessary
- but the connector is only fully active after local machine activation

## Prerequisites

Set in `ts-connect/.env`:

```env
TS_CONNECT_KEYGEN_ACCOUNT=...
TS_CONNECT_KEYGEN_POLICY_ID=...
TS_CONNECT_KEYGEN_POLICY_NAME=Club Plus
TS_CONNECT_KEYGEN_AUTO_ACTIVATE=true
```

Optional overrides:

```env
TS_CONNECT_KEYGEN_API_URL=
TS_CONNECT_KEYGEN_MACHINE_NAME=
TS_CONNECT_KEYGEN_MACHINE_FINGERPRINT=
TS_CONNECT_KEYGEN_LICENSE_TOKEN=
```

Important persisted files:

- `ui/data/license.key`
- `ui/data/machine_fingerprint`
- `ui/data/license-meta.json`

## First Activation

1. Open `beta.targetshot.app` as club admin.
2. Go to `Club Billing`.
3. Ensure the club has a valid `Club Plus` state in Stripe/Keygen.
4. Copy the generated Keygen license key.
5. Open the `ts-connect` UI.
6. Paste the license key into `Lizenzverwaltung`.
7. Click `Lizenz prüfen`.

Expected result after the save:

- the new key is stored locally
- the plan changes to `Club Plus`
- if the installation is not yet activated, the UI shows that local Keygen activation is still required

8. Click `Installation aktivieren`.

Expected result after activation:

- `Installation aktiviert am ...`
- a `Maschine: ...` id is shown
- the connector is now allowed to leave pure local-buffer mode

## Rolling A New Club Key Onto A Running Connector

Use this when beta has issued a new key for the same club.

1. Copy the new key from `beta.targetshot.app`.
2. Paste it into `Lizenzverwaltung` in `ts-connect`.
3. Click `Lizenz prüfen`.

Current behavior:

- if the new key is invalid, the UI keeps it as a pending draft
- the old saved key remains active until the new key validates successfully
- the form no longer silently snaps back to the old key

4. If the new key validates but the connector is not yet activated for it, click `Installation aktivieren`.

## Machine Replacement / Hardware Change

Because the policy is `one machine per club`, a replacement host must take over the existing license cleanly.

Recommended sequence:

1. Find the current club license in Keygen.
2. Remove or deactivate the old machine entry for that club license.
3. Bring up the new `ts-connect` host.
4. Paste the same Club Plus key into the new connector.
5. Click `Lizenz prüfen`.
6. Click `Installation aktivieren`.

If the old machine is still attached, activation on the new host will usually fail because the machine limit is reached.

## Suspended / Revoked / Invalid Recovery

### Suspended

Typical meaning:

- billing is inactive or past due
- or the club-side cloud record is not active

Operator action:

1. Check `Club Billing` in `beta.targetshot.app`.
2. Restore the club to `trial` or `active`.
3. Resync Keygen if needed.
4. Back in `ts-connect`, run `Lizenz prüfen` again.

### Revoked / Disabled / Cancelled

Typical meaning:

- the current key should no longer be used

Operator action:

1. Get the currently assigned key from beta.
2. Replace the local key in `ts-connect`.
3. Validate it.
4. Activate the installation if required.

### Fingerprint / Machine Mismatch

Typical meaning:

- the key itself is fine
- but this local installation is not the machine currently bound in Keygen

Operator action:

1. Remove the old machine from the Keygen license.
2. Retry `Installation aktivieren` on the current host.

## What The Connector Checks

### Save / `Lizenz prüfen`

The UI save path now checks the club license itself first.

That means:

- a valid beta-generated club key can be stored without immediately failing on local fingerprint scope
- this is a club-license acceptance check, not the final machine binding

### Activate / `Installation aktivieren`

Activation is still machine-bound.

That means:

- fingerprint and machine limits are enforced here
- the connector is only fully active after this step

## Verification Checklist

After a successful rollout, verify all of these:

- `Lizenzverwaltung` shows `Club Plus`
- `Status` is not `invalid`, `revoked`, `cancelled`, or `disabled`
- `Installation aktiviert am ...` is visible
- `Maschine: ...` is populated
- `ui/data/license.key` contains the expected key
- `ui/data/machine_fingerprint` exists
- cloud replication is no longer blocked purely due to missing activation

## Server-Side Checks

Inspect these files on the host:

- `ui/logs/ui.log`
- `ui/logs/update-agent.log`
- `ui/logs/update-runner.log`

Useful runtime checks:

```bash
docker logs ts-connect-ui
docker logs ts-connect-update-agent
```

## Known Boundary

The cloud side can provision and sync the license, but `ts-connect` still expects the operator to paste the assigned key locally and activate the local machine. That is intentional for now and should be reflected in operator onboarding.

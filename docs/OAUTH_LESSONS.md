# OAuth investigation lessons

During the proof-of-concept investigation, an existing DXLink connection continued to work even though the refresh token was no longer usable. A live connection therefore does not prove that a future access-token refresh will succeed. Constructing a tastytrade `Session` likewise does not by itself establish that OAuth authentication has succeeded.

An explicit access-token refresh exposed `invalid_grant: Grant revoked`. A replacement grant initially failed with `Client secret mismatch`. Comparing credential fingerprints locally showed that the client secrets differed. Pairing the correct client secret with the new grant authenticated successfully.

The investigation did not establish why the original grant was revoked. In particular, do not infer that using two IP addresses caused the revocation. Test the refresh path explicitly when diagnosing authentication, and make sure the client secret and grant belong together. Keep secrets, access and refresh tokens, raw OAuth responses and credential fingerprints out of source control, logs, screenshots and shared diagnostic output.

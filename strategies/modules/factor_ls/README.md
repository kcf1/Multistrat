# factor_ls

Reference long/short factor strategy skeleton.

Current status:

- Universe and pipeline wiring are placeholders.
- Persistent features are expected from external feature pipeline storage.
- Ephemeral features are runtime-only lightweight transforms.

Main flow:

1. Load persistent features.
2. Apply ephemeral transforms.
3. Score symbols.
4. Build long/short buckets.
5. Convert to order intents.


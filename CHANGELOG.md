# CHANGELOG


## v1.1.0 (2026-06-30)

### Bug Fixes

- Clear errors for connection, auth, and API failures
  ([#28](https://github.com/lightdash/python-sdk/pull/28),
  [`4e9516e`](https://github.com/lightdash/python-sdk/commit/4e9516e5eb1038d79ca7ff1314a8f3fc8a5350d6))

Co-authored-by: Claude Opus 4.8 <noreply@anthropic.com>

### Continuous Integration

- Automate releases on merge to main ([#32](https://github.com/lightdash/python-sdk/pull/32),
  [`482a44f`](https://github.com/lightdash/python-sdk/commit/482a44f97beba14aa7a63c0cb0074af2e4239f8d))

### Documentation

- Add between() and not_between() to SDK guide
  ([#16](https://github.com/lightdash/python-sdk/pull/16),
  [`cadacf7`](https://github.com/lightdash/python-sdk/commit/cadacf77aded9bb2124f6e109e6ef9ed849c15c4))

Co-authored-by: Claude Opus 4.6 <noreply@anthropic.com>

### Features

- Add between() and not_between() helpers to Dimension
  ([#15](https://github.com/lightdash/python-sdk/pull/15),
  [`db818e5`](https://github.com/lightdash/python-sdk/commit/db818e5d3ac8d1f0f2d976cbb12dc84066a09e28))

Co-authored-by: Claude Opus 4.6 <noreply@anthropic.com>

- Compile a query to warehouse SQL without executing it
  ([#30](https://github.com/lightdash/python-sdk/pull/30),
  [`472e0f6`](https://github.com/lightdash/python-sdk/commit/472e0f6e1aee8d7cfa98e034e8529ea9cb9290b6))

Co-authored-by: Claude Opus 4.8 <noreply@anthropic.com>

- Fetch results beyond the old 50k row cap ([#25](https://github.com/lightdash/python-sdk/pull/25),
  [`3b434e7`](https://github.com/lightdash/python-sdk/commit/3b434e783bf4a164c6be013a1aa25f206a5dec75))

Co-authored-by: Claude Opus 4.8 <noreply@anthropic.com>

- Support filtering on table calculations ([#24](https://github.com/lightdash/python-sdk/pull/24),
  [`9e35685`](https://github.com/lightdash/python-sdk/commit/9e35685f7e3d46599070bcf9bd2dd379fc958acd))

Co-authored-by: Claude Opus 4.8 <noreply@anthropic.com>

- Support multiple filters on the same dimension field
  ([#22](https://github.com/lightdash/python-sdk/pull/22),
  [`a7d9d73`](https://github.com/lightdash/python-sdk/commit/a7d9d73b653f64caf11659aebc1c2c52856f3ca1))

Co-authored-by: Claude Opus 4.8 <noreply@anthropic.com>


## v1.0.1 (2026-06-29)

### Bug Fixes

- Errored models breaking `list_models` method
  ([`ee1b91e`](https://github.com/lightdash/python-sdk/commit/ee1b91edc7abcf17760bbc9e006fb9db4c028225))

- Remove circular types with protocols
  ([`38c2ee0`](https://github.com/lightdash/python-sdk/commit/38c2ee093a534c87a42df446fa55aef22d0830df))

- Use correct API operator names for dimension filters
  ([`a8581bf`](https://github.com/lightdash/python-sdk/commit/a8581bf2fe3e071b6cac3009c1c554195d034835))

The Lightdash API expects camelCase operator names (e.g., notEquals, greaterThan) but the SDK was
  using human-readable strings (e.g., "is not", "is greater than"). This caused API errors when
  using operators like !=.

### Chores

- Add notebook command to justfile ([#13](https://github.com/lightdash/python-sdk/pull/13),
  [`b8324ce`](https://github.com/lightdash/python-sdk/commit/b8324ce9e33cfd6e51ad64ffee499ab3c6a50a68))

- Bump version to 0.3.0
  ([`2396074`](https://github.com/lightdash/python-sdk/commit/23960742b0c62891d5a50f0acc57a0f302e5d5e3))

- Fix release
  ([`45d967e`](https://github.com/lightdash/python-sdk/commit/45d967e96dc3855e12d3ad43e39397d9de9a6d28))

- Update .gitignore ([#12](https://github.com/lightdash/python-sdk/pull/12),
  [`7032c78`](https://github.com/lightdash/python-sdk/commit/7032c78b68ce8be7df2c02d701e891467c76ee4c))

### Documentation

- Update readme
  ([`f332829`](https://github.com/lightdash/python-sdk/commit/f33282971bbe77e0d8893e882e633901d45922f9))

### Features

- Add `get_model` to get a specific model more programablly
  ([`9fe0887`](https://github.com/lightdash/python-sdk/commit/9fe088710a616a803b52492b8e36b288798dd106))

Signed-off-by: Yu Ishikawa <yu-iskw@users.noreply.github.com>

- Enhancement
  ([`c6989e8`](https://github.com/lightdash/python-sdk/commit/c6989e857be64f6cae0d3cd0d79192a28d46580b))

Signed-off-by: Yu Ishikawa <yu-iskw@users.noreply.github.com>

- Make httpx client timeout configurable via config dict
  ([#11](https://github.com/lightdash/python-sdk/pull/11),
  [`4bfd5c1`](https://github.com/lightdash/python-sdk/commit/4bfd5c1dea9622bc1ed750506cab7ccf98124d68))

Co-authored-by: Claude <noreply@anthropic.com>

- Query builder + async query api ([#14](https://github.com/lightdash/python-sdk/pull/14),
  [`f632c58`](https://github.com/lightdash/python-sdk/commit/f632c5832410fe7726c02c93669387b331732ca0))

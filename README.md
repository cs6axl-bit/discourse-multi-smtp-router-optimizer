# discourse-multi-smtp-router-optimizer

Add-on for [discourse-multi-smtp-router](https://github.com/cs6axl-bit/discourse-multi-smtp-router). It replaces only the
router's **per-domain metrics step**; domain overrides, per-campaign only/avoid constraints, verification pools,
weighted/random fallbacks, domain swap and logging all stay in the router.

## What it changes
- **Sample-size aware ranking.** The router sorted providers by raw click % from a 3-day window (cells of 11+ emails),
  so 1 visit in 12 emails beat 60 in 20,000. The optimizer uses counts with a hierarchical Bayesian prior:
  global → provider overall → provider × mailbox-provider group → provider × exact domain.
- **Thompson sampling** instead of "top N% + pre-random + single-row random": each send draws a plausible rate per
  provider and picks the highest, so exploration is automatic and shrinks as evidence grows. New providers and
  domains without data get the group's / provider's prior instead of uniform random.
- **Mailbox-provider groups** (setting): Microsoft, Apple, Spectrum (`*.rr.com`, charter, twc…), Comcast, Yahoo/AOL/AT&T/
  Verizon, Google, Cox, Shaw, Bell, BT, Telstra… Reputation is judged by the receiving provider, not the address domain.
- **Longer, recency-weighted history.** A job snapshots `digest_provider_domain_metrics` (owner's Java, rolling 3 days)
  into `smtp_router_opt_metrics_history` and learns from 14 days with a 7-day half-life.
- **Bounce / complaint guard** from `esp_webhook_events` (discourse-esp-webhooks): a provider above the complaint or
  hard-bounce threshold for a group (with enough sends) is skipped for that group. Needs the ESP webhooks to deliver.
- **Per-provider daily / hourly caps** (Redis counters); overflow goes to the next best provider.
- **Per-user preference (0.2.0)**: providers whose emails a user engaged with (forum visit ≥ 10 s or in-topic, from
  the table `smtp_user_provider_pref`, built daily by the supshop repo) get a decaying boost for that user's next sends
  (×(1 + strength·engaged), capped at ×3, 10% exploration). A/B: `crc32(email) % 100 < test_percent` = boosted arm, rest =
  control; per-day counts in Redis (`SmtpRouterOptimizer` pref log).
- Optional: strip the UUID / slot / weight headers from outgoing mail.

## Modes
- `shadow` (default): the router decides as before; the optimizer records what it would have picked per day in Redis
  (`SmtpRouterOptimizer.shadow_summary`). Compare first.
- `active`: the optimizer picks; if it can't (no model / no domain) the router's own metrics logic runs.
Routing reason in active mode: `opt_ts(group=… domain=… pick=… mean=…% n=domain/group pool=…)`.

## Requirements
discourse-multi-smtp-router with `multi_smtp_router_domain_metrics_enabled` on (the hook sits in that step).

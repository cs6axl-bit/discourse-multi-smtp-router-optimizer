# frozen_string_literal: true

# name: discourse-multi-smtp-router-optimizer
# about: Add-on for discourse-multi-smtp-router. Replaces only its per-domain metrics step with Bayesian Thompson sampling over mailbox-provider groups (Microsoft, Apple, Spectrum, ...), sample-size aware rates with a 14-day decayed history, bounce/complaint guards (esp_webhook_events) and per-provider daily/hourly caps. Shadow mode logs its choice without changing routing.
# version: 0.1.1
# authors: supshop
# required_version: 3.0.0

enabled_site_setting :smtp_router_optimizer_enabled

after_initialize do
  module ::SmtpRouterOptimizer
    PLUGIN_NAME = "discourse-multi-smtp-router-optimizer"
    METRICS_TABLE = "public.digest_provider_domain_metrics"
    HISTORY_TABLE = "smtp_router_opt_metrics_history"
    EVENTS_TABLE = "esp_webhook_events"
    MODEL_CACHE_KEY = "#{PLUGIN_NAME}:model:v1"
    METRICS_WINDOW_DAYS = 3 # digest_provider_domain_metrics is a rolling 3-day window

    def self.enabled?
      SiteSetting.smtp_router_optimizer_enabled
    rescue
      false
    end

    def self.mode
      SiteSetting.smtp_router_optimizer_mode.to_s
    rescue
      "shadow"
    end

    def self.warn(msg)
      Rails.logger.warn("[#{PLUGIN_NAME}] #{msg}")
    rescue
    end

    def self.debug(msg)
      return unless SiteSetting.smtp_router_optimizer_debug_log
      Rails.logger.warn("[#{PLUGIN_NAME}] DEBUG #{msg}")
    rescue
    end

    # ---------------------------------------------------------------------------------------------
    # Domain → mailbox-provider group. One list entry per group: "group=pattern,pattern" (Discourse stores list
    # entries separated by "|", so domains inside a group are separated by commas); a pattern is an exact domain
    # or "*.suffix". Unlisted domains are their own group ("d:<domain>").
    # ---------------------------------------------------------------------------------------------
    def self.group_rules
      raw = SiteSetting.smtp_router_optimizer_domain_groups.to_s
      key = raw.hash
      return @group_rules if @group_rules_key == key && @group_rules
      exact = {}
      suffix = []
      raw.split(/[\n|]+/).each do |line|
        name, pats = line.to_s.strip.split("=", 2)
        next if name.to_s.strip.empty? || pats.to_s.strip.empty?
        pats.split(/[,\s]+/).map { |p| p.strip.downcase }.reject(&:empty?).each do |p|
          if p.start_with?("*.")
            suffix << [p[1..], name.strip]
          else
            exact[p] = name.strip
          end
        end
      end
      @group_rules_key = key
      @group_rules = { exact: exact, suffix: suffix.sort_by { |s, _| -s.length } }
    end

    def self.group_of(domain)
      d = domain.to_s.downcase.strip
      return "d:" if d.empty?
      r = group_rules
      return r[:exact][d] if r[:exact].key?(d)
      r[:suffix].each { |suf, name| return name if d.end_with?(suf) }
      "d:#{d}"
    end

    # ---------------------------------------------------------------------------------------------
    # History: daily snapshots of the 3-day metrics table (written by the owner's Java job ~21:00 UTC).
    # ---------------------------------------------------------------------------------------------
    def self.ensure_history_table!
      return if @history_ready
      DB.exec(<<~SQL)
        CREATE TABLE IF NOT EXISTS #{HISTORY_TABLE} (
          snapshot_at   timestamptz NOT NULL,
          provider_id   text NOT NULL,
          email_domain  text NOT NULL,
          total_records integer NOT NULL,
          opened_records integer,
          clicked_records integer,
          PRIMARY KEY (snapshot_at, provider_id, email_domain)
        )
      SQL
      @history_ready = true
    end

    def self.snapshot_metrics!
      ensure_history_table!
      gen = DB.query_single("SELECT max(generated_at) FROM #{METRICS_TABLE}").first
      return :no_metrics if gen.nil?
      have = DB.query_single("SELECT 1 FROM #{HISTORY_TABLE} WHERE snapshot_at = :g LIMIT 1", g: gen).first
      return :already if have
      DB.exec(<<~SQL, g: gen)
        INSERT INTO #{HISTORY_TABLE} (snapshot_at, provider_id, email_domain, total_records, opened_records, clicked_records)
        SELECT :g, provider_id, lower(email_domain), total_records, opened_records, clicked_records
        FROM #{METRICS_TABLE}
        WHERE provider_id IS NOT NULL AND email_domain IS NOT NULL AND total_records > 0
        ON CONFLICT DO NOTHING
      SQL
      keep = [SiteSetting.smtp_router_optimizer_history_days.to_i, 3].max * 3
      DB.exec("DELETE FROM #{HISTORY_TABLE} WHERE snapshot_at < now() - (:d || ' days')::interval", d: keep.to_s)
      :snapshotted
    rescue => e
      warn("snapshot failed: #{e.class}: #{e.message}")
      :error
    end

    # esp name in esp_webhook_events → router provider id ("sparkpost=sparkpost1, ...")
    def self.esp_provider_map
      SiteSetting.smtp_router_optimizer_esp_provider_map.to_s.split(/[\n,]+/).each_with_object({}) do |pair, h|
        k, v = pair.split("=", 2).map { |x| x.to_s.strip }
        h[k.downcase] = v if k.present? && v.present?
      end
    end

    # ---------------------------------------------------------------------------------------------
    # Model: decayed counts per provider at three levels (domain, group, provider overall) + global
    # rate, plus bounce / complaint rates per provider × group. Rebuilt by the job, shared via cache.
    # ---------------------------------------------------------------------------------------------
    def self.build_model!
      ensure_history_table!
      days = [SiteSetting.smtp_router_optimizer_history_days.to_i, 3].max
      half = [SiteSetting.smtp_router_optimizer_half_life_days.to_f, 0.5].max
      rows = DB.query(<<~SQL, d: days.to_s)
        SELECT provider_id, email_domain, total_records, clicked_records,
               EXTRACT(EPOCH FROM (now() - snapshot_at)) / 86400.0 AS age_days
        FROM #{HISTORY_TABLE}
        WHERE snapshot_at > now() - (:d || ' days')::interval
      SQL
      # no history yet: fall back to the live 3-day table so the model is never empty
      if rows.empty?
        rows = DB.query(<<~SQL)
          SELECT provider_id, lower(email_domain) AS email_domain, total_records, clicked_records, 0.0 AS age_days
          FROM #{METRICS_TABLE} WHERE total_records > 0
        SQL
      end

      dom = Hash.new { |h, k| h[k] = Hash.new { |hh, kk| hh[kk] = [0.0, 0.0] } }   # domain → provider → [sends, clicks]
      grp = Hash.new { |h, k| h[k] = Hash.new { |hh, kk| hh[kk] = [0.0, 0.0] } }   # group → provider → [sends, clicks]
      prov = Hash.new { |h, k| h[k] = [0.0, 0.0] }
      tot = [0.0, 0.0]
      rows.each do |r|
        # overlapping 3-day windows: each email is in ~3 daily snapshots → divide by the window length
        w = (0.5**(r.age_days.to_f / half)) / METRICS_WINDOW_DAYS
        s = r.total_records.to_f * w
        c = r.clicked_records.to_f * w
        d = r.email_domain.to_s.downcase
        g = group_of(d)
        p = r.provider_id.to_s
        dom[d][p][0] += s; dom[d][p][1] += c
        grp[g][p][0] += s; grp[g][p][1] += c
        prov[p][0] += s; prov[p][1] += c
        tot[0] += s; tot[1] += c
      end

      sends_by_pg = Hash.new(0.0)
      grp.each { |g, ps| ps.each { |p, (s, _)| sends_by_pg["#{p}|#{g}"] += s } }
      guards = {}
      begin
        if DB.query_single("SELECT to_regclass(:t)::text", t: EVENTS_TABLE).first.present?
          map = esp_provider_map
          ev = DB.query(<<~SQL, d: days.to_s)
            SELECT lower(esp) AS esp, event_type, bounce_class, raw_event_type,
                   lower(split_part(recipient_email, '@', 2)) AS domain, count(*) AS n
            FROM #{EVENTS_TABLE}
            WHERE received_at > now() - (:d || ' days')::interval AND event_type IN ('bounce', 'complaint')
            GROUP BY 1, 2, 3, 4, 5
          SQL
          ev.each do |e|
            p = map[e.esp.to_s]
            next if p.blank?
            key = "#{p}|#{group_of(e.domain)}"
            guards[key] ||= { bounces: 0, complaints: 0 }
            if e.event_type == "complaint"
              guards[key][:complaints] += e.n.to_i
            elsif hard_bounce?(e)
              guards[key][:bounces] += e.n.to_i
            end
          end
          guards.each { |k, v| v[:sends] = sends_by_pg[k] * METRICS_WINDOW_DAYS } # undo the window division for event rates
        end
      rescue => e
        warn("guards build failed: #{e.class}: #{e.message}")
      end

      # JSON round-trip: plain hashes (the counting hashes have default procs, which the cache can't marshal)
      model = JSON.parse({
        "built_at" => Time.now.to_i,
        "global" => tot,
        "provider" => prov,
        "group" => grp,
        "domain" => dom,
        "guards" => guards
      }.to_json)
      Discourse.cache.write(MODEL_CACHE_KEY, model, expires_in: 2.days)
      @model_l1 = { at: Time.now.to_i, model: model }
      debug("model built: domains=#{dom.size} groups=#{grp.size} providers=#{prov.keys.inspect} rows=#{rows.size} guards=#{guards.size}")
      model
    rescue => e
      warn("build_model failed: #{e.class}: #{e.message}")
      nil
    end

    def self.hard_bounce?(e)
      bc = e.bounce_class.to_s.strip.downcase
      raw = e.raw_event_type.to_s.strip.downcase
      return true if %w[10 90 hard permanent].include?(bc)
      raw.include?("hard") || raw.include?("permanent") || raw == "b"
    end

    def self.model
      l1 = @model_l1
      return l1[:model] if l1 && Time.now.to_i - l1[:at] < 300
      m = Discourse.cache.read(MODEL_CACHE_KEY)
      m ||= build_model!
      @model_l1 = { at: Time.now.to_i, model: m } if m
      m
    end

    # ---------------------------------------------------------------------------------------------
    # Thompson sampling. Beta via two Gamma draws (Marsaglia–Tsang). Hierarchical prior:
    # global → provider overall → provider × group → provider × exact domain, each shrunk with
    # `prior_strength` pseudo-emails toward the level above.
    # ---------------------------------------------------------------------------------------------
    def self.rand_normal
      u1 = rand
      u1 = 1e-12 if u1 <= 0
      Math.sqrt(-2.0 * Math.log(u1)) * Math.cos(2.0 * Math::PI * rand)
    end

    def self.rand_gamma(shape)
      return rand_gamma(shape + 1.0) * (rand**(1.0 / shape)) if shape < 1.0
      d = shape - 1.0 / 3.0
      c = 1.0 / Math.sqrt(9.0 * d)
      loop do
        x = rand_normal
        v = (1.0 + c * x)**3
        next if v <= 0
        u = rand
        return d * v if u < 1.0 - 0.0331 * x**4 || Math.log(u) < 0.5 * x * x + d * (1.0 - v + Math.log(v))
      end
    end

    def self.rand_beta(a, b)
      x = rand_gamma(a)
      y = rand_gamma(b)
      x / (x + y)
    end

    def self.shrink(clicks, sends, prior_mean, k)
      (clicks.to_f + k * prior_mean) / (sends.to_f + k)
    end

    # returns { provider_id => { mean:, sample:, sends: } }
    def self.score(model, domain, group, provider_ids)
      k = [SiteSetting.smtp_router_optimizer_prior_strength.to_f, 1.0].max
      g = model["global"] || [0.0, 0.0]
      global_mean = g[0].to_f > 0 ? g[1].to_f / g[0].to_f : 0.002
      out = {}
      provider_ids.each do |p|
        ps, pc = (model.dig("provider", p) || [0.0, 0.0])
        p_mean = shrink(pc, ps, global_mean, k)
        gs, gc = (model.dig("group", group, p) || [0.0, 0.0])
        g_mean = shrink(gc, gs, p_mean, k)
        ds, dc = (model.dig("domain", domain, p) || [0.0, 0.0])
        a = dc.to_f + k * g_mean
        b = [ds.to_f - dc.to_f, 0.0].max + k * (1.0 - g_mean)
        out[p] = { mean: a / (a + b), sample: rand_beta([a, 1e-6].max, [b, 1e-6].max), sends: ds.to_f.round, group_sends: gs.to_f.round }
      end
      out
    end

    # bounce / complaint guard for provider × group (needs a minimum of sends to judge)
    def self.guarded?(model, provider_id, group)
      gd = model.dig("guards", "#{provider_id}|#{group}")
      return false unless gd
      sends = gd["sends"].to_f
      return false if sends < SiteSetting.smtp_router_optimizer_guard_min_sends.to_f
      comp = gd["complaints"].to_f / sends * 100.0
      bnc = gd["bounces"].to_f / sends * 100.0
      comp > SiteSetting.smtp_router_optimizer_max_complaint_percent.to_f || bnc > SiteSetting.smtp_router_optimizer_max_hard_bounce_percent.to_f
    end

    # ---------------------------------------------------------------------------------------------
    # Caps: per provider, per UTC day / hour, counted in Redis when the optimizer routes a message.
    # ---------------------------------------------------------------------------------------------
    def self.caps
      parse = ->(raw) { raw.to_s.split(/[\n,]+/).each_with_object({}) { |x, h| k, v = x.split("=", 2).map { |y| y.to_s.strip }; h[k] = v.to_i if k.present? && v.to_i > 0 } }
      { day: parse.call(SiteSetting.smtp_router_optimizer_daily_caps), hour: parse.call(SiteSetting.smtp_router_optimizer_hourly_caps) }
    end

    def self.cap_keys(p)
      t = Time.now.utc
      ["#{PLUGIN_NAME}:cap:d:#{p}:#{t.strftime('%Y%m%d')}", "#{PLUGIN_NAME}:cap:h:#{p}:#{t.strftime('%Y%m%d%H')}"]
    end

    def self.capped?(p, cp)
      dk, hk = cap_keys(p)
      (cp[:day][p] && Discourse.redis.get(dk).to_i >= cp[:day][p]) || (cp[:hour][p] && Discourse.redis.get(hk).to_i >= cp[:hour][p])
    rescue
      false
    end

    def self.count_send!(p)
      dk, hk = cap_keys(p)
      Discourse.redis.multi do |m|
        m.incr(dk); m.expire(dk, 3 * 86_400)
        m.incr(hk); m.expire(hk, 2 * 3600)
      end
    rescue => e
      warn("count_send failed: #{e.class}: #{e.message}")
    end

    # ---------------------------------------------------------------------------------------------
    # The decision. Returns [provider_hash, reason] or [nil, why] to let the router's own logic run.
    # ---------------------------------------------------------------------------------------------
    def self.decide(message, list)
      domains = ::MultiSmtpRouter.extract_recipient_domains(message)
      domain = domains.first.to_s
      return [nil, "opt_no_domain"] if domain.empty? || list.blank?
      m = model
      return [nil, "opt_no_model"] unless m
      group = group_of(domain)
      cp = caps
      pool = list.reject { |p| capped?(p[:id].to_s, cp) }
      capped = list.size - pool.size
      guarded = pool.select { |p| guarded?(m, p[:id].to_s, group) }
      pool -= guarded if guarded.size < pool.size # never guard everyone out
      pool = list if pool.empty?                   # all capped: ignore caps rather than drop mail
      scores = score(m, domain, group, pool.map { |p| p[:id].to_s })
      best_id, best = scores.max_by { |_, v| v[:sample] }
      chosen = pool.find { |p| p[:id].to_s == best_id }
      reason = "opt_ts(group=#{group} domain=#{domain} pick=#{best_id} mean=#{(best[:mean] * 100).round(3)}% " \
               "n=#{best[:sends]}/#{best[:group_sends]} pool=#{pool.size}#{capped > 0 ? " capped=#{capped}" : ''}#{guarded.any? ? " guarded=#{guarded.map { |x| x[:id] }.join('+')}" : ''})"
      [chosen, reason]
    rescue => e
      warn("decide failed: #{e.class}: #{e.message}")
      [nil, "opt_error(#{e.class})"]
    end

    # ---------------------------------------------------------------------------------------------
    # Shadow log: what the optimizer would pick vs what the router picked (Redis hash per day).
    # ---------------------------------------------------------------------------------------------
    def self.shadow_record!(group, opt_id, actual_id)
      key = "#{PLUGIN_NAME}:shadow:#{Time.now.utc.strftime('%Y%m%d')}"
      Discourse.redis.hincrby(key, "#{group}|#{opt_id}|#{actual_id}", 1)
      Discourse.redis.expire(key, 14 * 86_400)
    rescue
    end

    def self.shadow_summary(day = Time.now.utc.strftime("%Y%m%d"))
      Discourse.redis.hgetall("#{PLUGIN_NAME}:shadow:#{day}")
    rescue
      {}
    end

    # Hook into the router: runs in place of its per-domain metrics step (overrides, campaign constraints
    # and verification pools have already been applied by the router before this point).
    module RouterPatch
      def choose_provider_by_domain_metrics(message, list)
        return super unless ::SmtpRouterOptimizer.enabled?
        mode = ::SmtpRouterOptimizer.mode
        if mode == "active"
          p, reason = ::SmtpRouterOptimizer.decide(message, list)
          if p
            ::SmtpRouterOptimizer.count_send!(p[:id].to_s)
            return [p, reason]
          end
          ::SmtpRouterOptimizer.debug("active mode fell back to router metrics: #{reason}")
          return super
        end
        # shadow: router decides, optimizer only records what it would have picked
        actual, actual_reason = super
        begin
          p, _r = ::SmtpRouterOptimizer.decide(message, list)
          dom = ::MultiSmtpRouter.extract_recipient_domains(message).first.to_s
          ::SmtpRouterOptimizer.shadow_record!(::SmtpRouterOptimizer.group_of(dom), p&.dig(:id).to_s, actual&.dig(:id).to_s)
        rescue => e
          ::SmtpRouterOptimizer.warn("shadow failed: #{e.class}: #{e.message}")
        end
        [actual, actual_reason]
      end
    end
  end

  # Plugin load order isn't guaranteed (this add-on can initialize before the router defines ::MultiSmtpRouter),
  # so the hook is attached lazily: now if possible, else on the first email / job run. Idempotent.
  module ::SmtpRouterOptimizer
    def self.ensure_patched!
      return true if @patched
      return false unless defined?(::MultiSmtpRouter)
      unless ::MultiSmtpRouter.singleton_class.ancestors.include?(RouterPatch)
        ::MultiSmtpRouter.singleton_class.prepend(RouterPatch)
      end
      @patched = true
    rescue => e
      warn("ensure_patched failed: #{e.class}: #{e.message}")
      false
    end
  end
  ::SmtpRouterOptimizer.ensure_patched!
  # registered at init time, so it runs before the router's own before_email_send work reaches the metrics step
  DiscourseEvent.on(:before_email_send) { |*_p| ::SmtpRouterOptimizer.ensure_patched! }

  # Strip infrastructure headers that recipients' mail filters don't need (keeps Provider-Id and
  # Routing-Reason: other plugins read them). Runs after the router's before_email_send handler.
  DiscourseEvent.on(:before_email_send) do |*params|
    next unless ::SmtpRouterOptimizer.enabled? && SiteSetting.smtp_router_optimizer_strip_extra_headers
    message = params.first
    next if message.nil?
    %w[X-Multi-SMTP-Router-UUID X-Multi-SMTP-Router-Provider-Slot X-Multi-SMTP-Router-Provider-Weight].each do |h|
      message.header[h] = nil
    end
  rescue => e
    ::SmtpRouterOptimizer.warn("strip headers failed: #{e.class}: #{e.message}")
  end

  module ::Jobs
    class SmtpRouterOptimizerRefresh < ::Jobs::Scheduled
      every 30.minutes

      def execute(_args)
        ::SmtpRouterOptimizer.ensure_patched!
        return unless ::SmtpRouterOptimizer.enabled?
        ::SmtpRouterOptimizer.snapshot_metrics!
        ::SmtpRouterOptimizer.build_model!
      end
    end
  end
end

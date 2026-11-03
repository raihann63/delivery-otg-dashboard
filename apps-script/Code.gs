/**
 * Delivery - On The Go: dashboard backend.
 */

var CONFIG = {
  PROJECT_ID: 'delivery-otg-assessment',
  DATASET: 'delivery_otg_assessment',
  LOCATION: 'US',
  QUERY_TIMEOUT_MS: 60000,
  MAX_POLL_ATTEMPTS: 30,
  CACHE_TTL_SEC: 21600,
  CACHE_VERSION: 'v1',
  CACHE_CHUNK_CHARS: 80000,
  MAX_BYTES_BILLED: null
};

var PARAM_VALUES_ = { p_start: '2026-10-13', p_w1_end: '2026-10-19', p_w2_start: '2026-10-20', p_end: '2026-10-26' };
var TABLES_ = ['hubs', 'agents', 'orders', 'order_logs', 'agent_salary', 'volume_forecast', 'costs', 'rules'];
var ALLOWED_KEYS_ = ['hubIds', 'cluster', 'week', 'holdSide', 'ratioBasis'];
var AGENT_SIDE_REASONS_ = ['Late Dispatch', 'Not Attempted', 'Agent Time Shortage'];

var APPROVED_SQL_ = {
  q1Whole: `
WITH daily AS (
  SELECT l.agent_id, a.hub_id, DATE(l.event_time) AS event_date,
         COUNTIF(l.status = 'Assigned')  AS assigned,
         COUNTIF(l.status = 'Delivered') AS delivered
  FROM {{order_logs}} l
  JOIN {{agents}} a USING (agent_id)
  WHERE DATE(l.event_time)
  BETWEEN @p_start AND @p_end
  GROUP BY l.agent_id, a.hub_id, event_date
),
cal AS (
  SELECT a.hub_id, COUNT(*) AS agent_days_calendar
  FROM {{agents}} a
  CROSS JOIN UNNEST(GENERATE_DATE_ARRAY(@p_start, @p_end)) AS d
  WHERE a.joining_date <= d AND (a.active_status = 'Active' OR d <= @p_w1_end)
  GROUP BY a.hub_id
),
act AS (
  SELECT hub_id,
         COUNTIF(assigned > 0) AS agent_days_active,
         SUM(assigned)  AS total_assigned,
         SUM(delivered) AS total_deliveries
  FROM daily
  GROUP BY hub_id
)
SELECT
  h.hub_id, h.hub_name,
  act.total_assigned,
  act.total_deliveries,
  cal.agent_days_calendar,
  ROUND(SAFE_DIVIDE(act.total_deliveries, cal.agent_days_calendar), 2) AS avg_deliveries_per_agent_day_calendar,
  act.agent_days_active,
  ROUND(SAFE_DIVIDE(act.total_deliveries, act.agent_days_active), 2)   AS avg_deliveries_per_agent_day_active
FROM {{hubs}} h
LEFT JOIN cal USING (hub_id)
LEFT JOIN act USING (hub_id)
ORDER BY h.hub_id
`,
  q2: `
WITH attempts AS (
  SELECT a.hub_id,
         IF(DATE(l.event_time) <= @p_w1_end, 'W1', 'W2') AS wk,
         COUNT(*) AS attempts
  FROM {{order_logs}} l
  JOIN {{agents}} a USING (agent_id)
  WHERE l.status = 'Assigned'
    AND DATE(l.event_time) BETWEEN @p_start AND @p_end
  GROUP BY a.hub_id, wk
),
capacity AS (
  SELECT a.hub_id,
         IF(d <= @p_w1_end, 'W1', 'W2') AS wk,
         COUNT(*) AS agent_days
  FROM {{agents}} a
  CROSS JOIN UNNEST(GENERATE_DATE_ARRAY(@p_start, @p_end)) AS d
  WHERE a.joining_date <= d
    AND (d <= @p_w1_end OR a.active_status = 'Active')
  GROUP BY a.hub_id, wk
),
hub_wk AS (
  SELECT c.hub_id,
         SUM(IF(c.wk = 'W1', IFNULL(t.attempts, 0), 0)) AS w1_attempts,
         SUM(IF(c.wk = 'W1', c.agent_days, 0))          AS w1_agent_days,
         SUM(IF(c.wk = 'W2', IFNULL(t.attempts, 0), 0)) AS w2_attempts,
         SUM(IF(c.wk = 'W2', c.agent_days, 0))          AS w2_agent_days
  FROM capacity c
  LEFT JOIN attempts t USING (hub_id, wk)
  GROUP BY c.hub_id
)
SELECT
  h.hub_id, h.hub_name,
  w.w1_attempts, w.w1_agent_days,
  ROUND(100 * SAFE_DIVIDE(w.w1_attempts, w.w1_agent_days * 40), 2) AS week1_utilisation_pct,
  w.w2_attempts, w.w2_agent_days,
  ROUND(100 * SAFE_DIVIDE(w.w2_attempts, w.w2_agent_days * 40), 2) AS week2_utilisation_pct,
  IF(100 * SAFE_DIVIDE(w.w2_attempts, w.w2_agent_days * 40) > 85, 'ABOVE TARGET', 'Within target') AS week2_target_status
FROM {{hubs}} h
JOIN hub_wk w USING (hub_id)
ORDER BY h.hub_id
`,
  q3Totals: `
WITH holds AS (
  SELECT a.hub_id, l.hold_reason,
         l.hold_reason IN ('Late Dispatch', 'Not Attempted', 'Agent Time Shortage') AS agent_side
  FROM {{order_logs}} l
  JOIN {{agents}} a USING (agent_id)
  WHERE l.status = 'Hold' AND DATE(l.event_time) BETWEEN @p_w2_start AND @p_end
),
tot AS (
  SELECT hub_id, COUNT(*) AS total_holds, COUNTIF(agent_side) AS agent_side_holds
  FROM holds GROUP BY hub_id
),
rsn AS (
  SELECT hub_id, hold_reason, COUNT(*) AS n,
         ROW_NUMBER() OVER (PARTITION BY hub_id ORDER BY COUNT(*) DESC, hold_reason) AS rk
  FROM holds GROUP BY hub_id, hold_reason
)
SELECT
  h.hub_id, h.hub_name, t.total_holds, t.agent_side_holds,
  ROUND(100 * SAFE_DIVIDE(t.agent_side_holds, t.total_holds), 2) AS agent_side_share_pct,
  MAX(IF(r.rk = 1, r.hold_reason, NULL)) AS top1_reason,
  MAX(IF(r.rk = 1, r.n, NULL))           AS top1_holds,
  MAX(IF(r.rk = 2, r.hold_reason, NULL)) AS top2_reason,
  MAX(IF(r.rk = 2, r.n, NULL))           AS top2_holds,
  IF(MAX(IF(r.rk = 3, r.n, NULL)) = MAX(IF(r.rk = 2, r.n, NULL)), 'tie at #2 (alphabetical rule used)', '') AS tie_note
FROM {{hubs}} h
JOIN tot t USING (hub_id)
JOIN rsn r USING (hub_id)
GROUP BY h.hub_id, h.hub_name, t.total_holds, t.agent_side_holds
ORDER BY h.hub_id
`,
  q4: `
WITH held AS (
  SELECT DISTINCT order_id FROM {{order_logs}} WHERE status = 'Hold'
),
last_outcome AS (
  SELECT order_id, status AS last_event_status FROM {{order_logs}} WHERE status != 'Assigned'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY attempt_no DESC, event_time DESC) = 1
)
SELECT
  h.hub_id, h.hub_name, COUNT(*) AS held_orders,
  COUNTIF(o.final_status = 'Delivered') AS delivered_orders,
  COUNTIF(o.final_status = 'Returned')  AS returned_orders,
  COUNTIF(o.final_status = 'Hold')      AS still_on_hold_orders,
  ROUND(100 * SAFE_DIVIDE(COUNTIF(o.final_status = 'Delivered'), COUNT(*)), 2) AS delivered_pct,
  ROUND(100 * SAFE_DIVIDE(COUNTIF(o.final_status = 'Returned'),  COUNT(*)), 2) AS returned_pct,
  ROUND(100 * SAFE_DIVIDE(COUNTIF(o.final_status = 'Hold'),      COUNT(*)), 2) AS still_on_hold_pct,
  COUNTIF(lo.last_event_status != o.final_status) AS status_mismatch
FROM held
JOIN {{orders}} o USING (order_id)
JOIN last_outcome lo USING (order_id)
JOIN {{hubs}} h ON h.hub_id = o.hub_id
GROUP BY h.hub_id, h.hub_name
ORDER BY h.hub_id
`,
  q5: `
WITH agent_stats AS (
  SELECT agent_id,
         COUNTIF(status = 'Assigned') AS attempts,
         COUNTIF(status = 'Returned') AS returned_attempts,
         COUNT(DISTINCT IF(status = 'Assigned', order_id, NULL)) AS orders_handled,
         COUNT(DISTINCT IF(status = 'Returned', order_id, NULL)) AS orders_returned
  FROM {{order_logs}} WHERE DATE(event_time) BETWEEN @p_start AND @p_end GROUP BY agent_id
),
calc AS (
  SELECT a.agent_id, a.agent_name, a.hub_id, a.active_status, a.joining_date,
         DATE_DIFF(@p_end, a.joining_date, DAY) AS tenure_days, s.attempts, s.returned_attempts,
         100 * SAFE_DIVIDE(s.returned_attempts, s.attempts) AS ratio_attempt,
         s.orders_handled, s.orders_returned,
         100 * SAFE_DIVIDE(s.orders_returned, s.orders_handled) AS ratio_order
  FROM {{agents}} a JOIN agent_stats s USING (agent_id)
)
SELECT agent_id, agent_name, hub_id, active_status, joining_date, tenure_days,
       attempts, returned_attempts, ROUND(ratio_attempt, 2) AS return_ratio_attempt_pct,
       orders_handled, orders_returned, ROUND(ratio_order, 2) AS return_ratio_order_pct,
       IF(ratio_attempt > 15, 'YES', 'no') AS flag_primary,
       IF(ratio_order > 15, 'YES', 'no')   AS flag_comparison
FROM calc
WHERE tenure_days > 30 AND (ratio_attempt > 15 OR ratio_order > 15)
ORDER BY ratio_attempt DESC, agent_id
`,
  q6: `
WITH rates AS (
  SELECT salary_type, monthly_salary / 26 AS daily_base, daily_allowance, allowance_per_delivery
  FROM {{agent_salary}}
),
cal AS (
  SELECT a.hub_id, a.salary_type, IF(d <= @p_w1_end, 'Week 1', 'Week 2') AS wk, COUNT(*) AS agent_days
  FROM {{agents}} a
  CROSS JOIN UNNEST(GENERATE_DATE_ARRAY(@p_start, @p_end)) AS d
  WHERE a.joining_date <= d AND (d <= @p_w1_end OR a.active_status = 'Active')
  GROUP BY a.hub_id, a.salary_type, wk
),
act AS (
  SELECT a.hub_id, a.salary_type, IF(x.d <= @p_w1_end, 'Week 1', 'Week 2') AS wk, COUNT(*) AS agent_days
  FROM (SELECT DISTINCT agent_id, DATE(event_time) AS d FROM {{order_logs}} WHERE status = 'Assigned' AND DATE(event_time) BETWEEN @p_start AND @p_end) x
  JOIN {{agents}} a USING (agent_id)
  GROUP BY a.hub_id, a.salary_type, wk
),
del AS (
  SELECT a.hub_id, a.salary_type, IF(DATE(l.event_time) <= @p_w1_end, 'Week 1', 'Week 2') AS wk, COUNTIF(l.status = 'Delivered') AS deliveries
  FROM {{order_logs}} l JOIN {{agents}} a USING (agent_id)
  WHERE DATE(l.event_time) BETWEEN @p_start AND @p_end
  GROUP BY a.hub_id, a.salary_type, wk
),
agg AS (
  SELECT c.hub_id, c.wk,
         SUM(IFNULL(d.deliveries, 0))  AS delivered_orders,
         SUM(IFNULL(ac.agent_days, 0)) AS agent_days_activity,
         SUM(c.agent_days)             AS agent_days_calendar,
         SUM(IFNULL(ac.agent_days, 0) * (r.daily_base + r.daily_allowance) + IFNULL(d.deliveries, 0) * r.allowance_per_delivery) AS pay_activity,
         SUM(c.agent_days * (r.daily_base + r.daily_allowance) + IFNULL(d.deliveries, 0) * r.allowance_per_delivery) AS pay_calendar
  FROM cal c
  LEFT JOIN act ac USING (hub_id, salary_type, wk)
  LEFT JOIN del d  USING (hub_id, salary_type, wk)
  JOIN rates r     USING (salary_type)
  GROUP BY c.hub_id, c.wk
)
SELECT h.hub_id, h.hub_name, g.wk AS week,
       g.delivered_orders, g.agent_days_activity, g.agent_days_calendar,
       ROUND(g.pay_activity, 2) AS agent_pay_activity_bdt,
       ROUND(g.pay_calendar, 2) AS agent_pay_calendar_bdt,
       ROUND(SAFE_DIVIDE(g.pay_activity, g.delivered_orders), 2) AS cost_per_delivered_order_activity_primary,
       ROUND(SAFE_DIVIDE(g.pay_calendar, g.delivered_orders), 2) AS cost_per_delivered_order_calendar_comparison
FROM agg g JOIN {{hubs}} h USING (hub_id)
ORDER BY h.hub_id, g.wk
`,
  q7: `
WITH att AS (
  SELECT o.hub_id, COUNT(*) AS attempts
  FROM {{order_logs}} l JOIN {{orders}} o USING (order_id)
  WHERE l.status = 'Assigned' AND DATE(l.event_time) BETWEEN @p_start AND @p_end
  GROUP BY o.hub_id
),
ord AS (
  SELECT hub_id, COUNT(*) AS orders FROM {{orders}} WHERE DATE(created_at) BETWEEN @p_start AND @p_end GROUP BY hub_id
),
ratio AS (
  SELECT ord.hub_id, SAFE_DIVIDE(att.attempts, ord.orders) AS hub_ratio,
         SAFE_DIVIDE(SUM(att.attempts) OVER (), SUM(ord.orders) OVER ()) AS overall_ratio
  FROM ord JOIN att USING (hub_id)
),
act AS (
  SELECT hub_id, COUNT(*) AS active_agents FROM {{agents}} WHERE active_status = 'Active' GROUP BY hub_id
),
base AS (
  SELECT f.hub_id, f.week_start, f.expected_orders, a.active_agents, r.hub_ratio, r.overall_ratio,
         DENSE_RANK() OVER (ORDER BY f.week_start) AS forecast_week, 7 * (40 * 85 / 100) AS attempts_per_agent_week
  FROM {{volume_forecast}} f JOIN ratio r USING (hub_id) JOIN act a USING (hub_id)
),
calc AS (
  SELECT *, expected_orders * hub_ratio / attempts_per_agent_week AS req_hub, expected_orders * overall_ratio / attempts_per_agent_week AS req_overall
  FROM base
)
SELECT h.hub_id, h.hub_name, c.forecast_week, c.week_start, c.expected_orders, c.active_agents,
       ROUND(c.hub_ratio, 4) AS hub_ratio, ROUND(c.req_hub, 2) AS required_agents_hub,
       CAST(CEIL(c.req_hub) AS INT64) AS required_rounded_hub,
       CAST(CEIL(c.req_hub) AS INT64) - c.active_agents AS staffing_gap_hub,
       GREATEST(CAST(CEIL(c.req_hub) AS INT64) - c.active_agents, 0) AS shortfall_hub,
       ROUND(c.overall_ratio, 4) AS overall_ratio, ROUND(c.req_overall, 2) AS required_agents_overall,
       CAST(CEIL(c.req_overall) AS INT64) AS required_rounded_overall,
       CAST(CEIL(c.req_overall) AS INT64) - c.active_agents AS staffing_gap_overall,
       GREATEST(CAST(CEIL(c.req_overall) AS INT64) - c.active_agents, 0) AS shortfall_overall
FROM calc c JOIN {{hubs}} h USING (hub_id)
ORDER BY h.hub_id, c.forecast_week
`
};

var EXTRA_SQL_ = {
  hubs: 'SELECT hub_id, hub_name, cluster FROM {{hubs}} ORDER BY hub_id',
  holdReasons: "SELECT DISTINCT hold_reason FROM {{order_logs}} WHERE status = 'Hold' AND hold_reason IS NOT NULL ORDER BY hold_reason",
  forecastWeeks: 'SELECT week_start, week_end, ROW_NUMBER() OVER (ORDER BY week_start) AS forecast_week FROM (SELECT DISTINCT week_start, week_end FROM {{volume_forecast}}) ORDER BY week_start',
  hubOrders: 'SELECT hub_id, COUNT(*) AS orders FROM {{orders}} WHERE DATE(created_at) BETWEEN @p_start AND @p_end GROUP BY hub_id ORDER BY hub_id',
  hubDay: "SELECT a.hub_id, DATE(l.event_time) AS event_date, COUNTIF(l.status = 'Assigned') AS assigned, COUNTIF(l.status = 'Delivered') AS delivered, COUNTIF(l.status = 'Hold') AS hold, COUNTIF(l.status = 'Returned') AS returned FROM {{order_logs}} l JOIN {{agents}} a USING (agent_id) WHERE DATE(l.event_time) BETWEEN @p_start AND @p_end GROUP BY a.hub_id, event_date ORDER BY a.hub_id, event_date",
  holdsByReason: "SELECT a.hub_id, l.hold_reason, COUNT(*) AS holds FROM {{order_logs}} l JOIN {{agents}} a USING (agent_id) WHERE l.status = 'Hold' AND DATE(l.event_time) BETWEEN @p_w2_start AND @p_end GROUP BY a.hub_id, l.hold_reason ORDER BY a.hub_id, holds DESC, l.hold_reason",
  q1Weekly: "WITH daily AS (SELECT l.agent_id, a.hub_id, DATE(l.event_time) AS event_date, COUNTIF(l.status = 'Assigned') AS assigned, COUNTIF(l.status = 'Delivered') AS delivered FROM {{order_logs}} l JOIN {{agents}} a USING (agent_id) WHERE DATE(l.event_time) BETWEEN @p_start AND @p_end GROUP BY l.agent_id, a.hub_id, event_date), cal AS (SELECT a.hub_id, IF(d <= @p_w1_end, 'Week 1', 'Week 2') AS wk, COUNT(*) AS agent_days_calendar FROM {{agents}} a CROSS JOIN UNNEST(GENERATE_DATE_ARRAY(@p_start, @p_end)) AS d WHERE a.joining_date <= d AND (a.active_status = 'Active' OR d <= @p_w1_end) GROUP BY a.hub_id, wk), act AS (SELECT hub_id, IF(event_date <= @p_w1_end, 'Week 1', 'Week 2') AS wk, COUNTIF(assigned > 0) AS agent_days_active, SUM(assigned) AS total_assigned, SUM(delivered) AS total_deliveries FROM daily GROUP BY hub_id, wk) SELECT h.hub_id, h.hub_name, cal.wk AS week, act.total_assigned, act.total_deliveries, cal.agent_days_calendar, ROUND(SAFE_DIVIDE(act.total_deliveries, cal.agent_days_calendar), 2) AS avg_deliveries_per_agent_day_calendar, act.agent_days_active, ROUND(SAFE_DIVIDE(act.total_deliveries, act.agent_days_active), 2) AS avg_deliveries_per_agent_day_active FROM {{hubs}} h JOIN cal ON cal.hub_id = h.hub_id LEFT JOIN act ON act.hub_id = cal.hub_id AND act.wk = cal.wk ORDER BY h.hub_id, week",
  
  // --- নতুন Q8, Q9, Q10 যোগ করা হলো ---
  q8Scenario: `
WITH plan AS (
  SELECT * FROM UNNEST([
    STRUCT('H01' AS hub_id, 2 AS transfer_in, 0 AS new_hires, 0 AS programme),
    STRUCT('H02', -2, 0, 0),
    STRUCT('H03',  0, 0, 1),
    STRUCT('H04',  0, 2, 0),
    STRUCT('H05',  0, 0, 0),
    STRUCT('H06',  0, 0, 0)])
),
ord AS (
  SELECT hub_id, COUNT(*) AS orders FROM {{orders}} WHERE DATE(created_at) BETWEEN @p_start AND @p_end GROUP BY hub_id
),
ev AS (
  SELECT o.hub_id, COUNTIF(l.status = 'Assigned') AS attempts, COUNTIF(l.status = 'Hold') AS holds,
         COUNTIF(l.status = 'Hold' AND l.hold_reason IN ('Late Dispatch', 'Not Attempted', 'Agent Time Shortage')) AS agent_holds
  FROM {{order_logs}} l JOIN {{orders}} o USING (order_id) WHERE DATE(l.event_time) BETWEEN @p_start AND @p_end GROUP BY o.hub_id
),
hubstat AS (
  SELECT ord.hub_id, ord.orders, ev.attempts, ev.holds, ev.agent_holds,
         SAFE_DIVIDE(ev.attempts, ord.orders) AS hub_ratio,
         SAFE_DIVIDE(ev.attempts - ord.orders, ev.holds) AS k_retry_per_hold,
         SAFE_DIVIDE(ev.agent_holds, ord.orders) AS agent_holds_per_order
  FROM ord JOIN ev USING (hub_id)
),
act AS (
  SELECT hub_id, COUNT(*) AS active_agents FROM {{agents}} WHERE active_status = 'Active' GROUP BY hub_id
),
fw AS (
  SELECT f.hub_id, f.week_start, f.expected_orders, DENSE_RANK() OVER (ORDER BY f.week_start) AS forecast_week FROM {{volume_forecast}} f
),
calc AS (
  SELECT fw.hub_id, fw.forecast_week, fw.expected_orders, a.active_agents, s.hub_ratio, p.programme,
         CASE fw.forecast_week WHEN 1 THEN 0.25 WHEN 2 THEN 0.75 ELSE 1.0 END AS ramp,
         p.programme * 0.5 * s.agent_holds_per_order * s.k_retry_per_hold * CASE fw.forecast_week WHEN 1 THEN 0.25 WHEN 2 THEN 0.75 ELSE 1.0 END AS ratio_drop,
         a.active_agents + p.transfer_in + p.new_hires * IF(fw.forecast_week = 1, 0.6, 1.0) AS planned_agents,
         p.programme * 0.5 * s.agent_holds_per_order * fw.expected_orders * CASE fw.forecast_week WHEN 1 THEN 0.25 WHEN 2 THEN 0.75 ELSE 1.0 END AS holds_avoided
  FROM fw JOIN plan p USING (hub_id) JOIN hubstat s USING (hub_id) JOIN act a USING (hub_id)
)
SELECT h.hub_id, h.hub_name, c.forecast_week, c.expected_orders, c.active_agents,
       ROUND(c.planned_agents, 1) AS planned_agents_effective,
       ROUND(c.hub_ratio, 4) AS ratio_no_plan,
       ROUND(c.hub_ratio - c.ratio_drop, 4) AS ratio_with_plan,
       ROUND(c.expected_orders * c.hub_ratio / 238, 2) AS required_agents_no_plan,
       ROUND(c.expected_orders * (c.hub_ratio - c.ratio_drop) / 238, 2) AS required_agents_with_plan,
       ROUND(100 * c.expected_orders * c.hub_ratio / (c.active_agents * 280), 1) AS utilisation_no_plan_pct,
       ROUND(100 * c.expected_orders * (c.hub_ratio - c.ratio_drop) / (c.planned_agents * 280), 1) AS utilisation_with_plan_pct,
       IF(100 * c.expected_orders * (c.hub_ratio - c.ratio_drop) / (c.planned_agents * 280) > 85, 'above 85%', 'within target') AS plan_status,
       ROUND(c.holds_avoided, 1) AS holds_avoided,
       ROUND(c.holds_avoided * r.hold_cost, 0) AS hold_cost_saved_bdt
FROM calc c JOIN {{hubs}} h USING (hub_id) CROSS JOIN (SELECT MAX(IF(what = 'A hold', amount_bdt, NULL)) AS hold_cost FROM {{costs}}) r
ORDER BY h.hub_id, c.forecast_week
  `,
  
  q9BudgetOptions: `
WITH c AS (
  SELECT MAX(IF(what = 'Hiring a new agent', amount_bdt, NULL)) AS hire_cost,
         MAX(IF(what = 'Moving an agent to another hub', amount_bdt, NULL)) AS move_cost,
         MAX(IF(what = 'Hold reduction programme', amount_bdt, NULL)) AS programme_cost
  FROM {{costs}}
),
r AS (
  SELECT SAFE_CAST(MAX(IF(rule = 'Working days per month', value, NULL)) AS INT64) AS work_days FROM {{rules}}
),
s AS (
  SELECT MAX(IF(salary_type = 'Fixed', monthly_salary, NULL)) AS fixed_salary,
         MAX(IF(salary_type = 'Variable', daily_allowance, NULL)) AS var_daily,
         MAX(IF(salary_type = 'Variable', allowance_per_delivery, NULL)) AS var_per_delivery
  FROM {{agent_salary}}
),
opt AS (
  SELECT * FROM UNNEST([
    STRUCT('1. Hire the 8 requested' AS option_name, 8 AS hires, 0 AS transfers, 0 AS programmes),
    STRUCT('2. Hire 5 (Baseline shortfall)', 5, 0, 0), 
    STRUCT('3. 3 hires + 2 transfers, no programme', 3, 2, 0),
    STRUCT('4. RECOMMENDED: 2 hires + 2 transfers + Hold programme', 2, 2, 1), 
    STRUCT('5. Recommended + fallback hire', 3, 2, 1)
  ])
),


calc AS (
  SELECT o.*, c.hire_cost, c.move_cost, c.programme_cost, s.fixed_salary, r.work_days * (s.var_daily + 28 * s.var_per_delivery) AS var_salary
  FROM opt o CROSS JOIN c CROSS JOIN s CROSS JOIN r
)
SELECT option_name, hires, transfers, programmes,
       hires * (hire_cost + fixed_salary) + transfers * move_cost + programmes * programme_cost AS total_fixed_bdt,
       70000 - (hires * (hire_cost + fixed_salary) + transfers * move_cost + programmes * programme_cost) AS left_fixed_bdt,
       IF(hires * (hire_cost + fixed_salary) + transfers * move_cost + programmes * programme_cost <= 70000, 'fits', 'exceeds') AS fixed_vs_70000,
       hires * (hire_cost + var_salary) + transfers * move_cost + programmes * programme_cost AS total_variable_bdt,
       70000 - (hires * (hire_cost + var_salary) + transfers * move_cost + programmes * programme_cost) AS left_variable_bdt,
       IF(hires * (hire_cost + var_salary) + transfers * move_cost + programmes * programme_cost <= 70000, 'fits', 'exceeds') AS variable_vs_70000
FROM calc ORDER BY option_name
  `,

  q10Headroom: `
WITH plan AS (
  SELECT * FROM UNNEST([
    STRUCT('H01' AS hub_id, 2 AS transfer_in, 0 AS new_hires, 0 AS programme),
    STRUCT('H02', -2, 0, 0), STRUCT('H03', 0, 0, 1), STRUCT('H04', 0, 2, 0), STRUCT('H05', 0, 0, 0), STRUCT('H06', 0, 0, 0)])
),
ord AS (
  SELECT hub_id, COUNT(*) AS orders FROM {{orders}} WHERE DATE(created_at) BETWEEN @p_start AND @p_end GROUP BY hub_id
),
ev AS (
  SELECT o.hub_id, COUNTIF(l.status = 'Assigned') AS attempts, COUNTIF(l.status = 'Hold') AS holds,
         COUNTIF(l.status = 'Hold' AND l.hold_reason IN ('Late Dispatch', 'Not Attempted', 'Agent Time Shortage')) AS agent_holds
  FROM {{order_logs}} l JOIN {{orders}} o USING (order_id) WHERE DATE(l.event_time) BETWEEN @p_start AND @p_end GROUP BY o.hub_id
),
hs AS (
  SELECT ord.hub_id, SAFE_DIVIDE(ev.attempts, ord.orders) AS hub_ratio, SAFE_DIVIDE(ev.attempts - ord.orders, ev.holds) AS k,
         SAFE_DIVIDE(ev.agent_holds, ord.orders) AS ah_per_order
  FROM ord JOIN ev USING (hub_id)
),
act AS (
  SELECT hub_id, COUNT(*) AS active_agents FROM {{agents}} WHERE active_status = 'Active' GROUP BY hub_id
),
fw AS (
  SELECT hub_id, expected_orders, DENSE_RANK() OVER (ORDER BY week_start) AS forecast_week FROM {{volume_forecast}}
),
calc AS (
  SELECT fw.hub_id, fw.forecast_week, fw.expected_orders, s.hub_ratio,
         a.active_agents + p.transfer_in + p.new_hires * IF(fw.forecast_week = 1, 0.6, 1.0) AS planned_agents,
         s.hub_ratio - p.programme * 0.5 * s.ah_per_order * s.k * CASE fw.forecast_week WHEN 1 THEN 0.25 WHEN 2 THEN 0.75 ELSE 1.0 END AS plan_ratio
  FROM fw JOIN plan p USING (hub_id) JOIN hs s USING (hub_id) JOIN act a USING (hub_id)
  WHERE fw.forecast_week IN (1, 3)
)
SELECT h.hub_id, h.hub_name, c.forecast_week, c.expected_orders,
       ROUND(c.planned_agents, 1) AS planned_agents_effective,
       ROUND(c.planned_agents * 238, 0) AS weekly_capacity_attempts,
       ROUND(c.hub_ratio, 4) AS ratio_today,
       ROUND(c.plan_ratio, 4) AS ratio_with_plan,
       ROUND(c.planned_agents * 238 / c.plan_ratio, 0) AS max_orders_carried,
       ROUND(100 * (c.planned_agents * 238 / c.plan_ratio / c.expected_orders - 1), 1) AS headroom_pct,
       ROUND(c.planned_agents * 238 / c.expected_orders, 4) AS max_ratio_tolerated
FROM calc c JOIN {{hubs}} h USING (hub_id)
ORDER BY h.hub_id, c.forecast_week
  `
};


var DATASETS_ = {
  hubs: EXTRA_SQL_.hubs,
  holdReasons: EXTRA_SQL_.holdReasons,
  forecastWeeks: EXTRA_SQL_.forecastWeeks,
  hubOrders: EXTRA_SQL_.hubOrders,
  hubDay: EXTRA_SQL_.hubDay,
  holdsByReason: EXTRA_SQL_.holdsByReason,
  q1Weekly: EXTRA_SQL_.q1Weekly,
  q1Whole: APPROVED_SQL_.q1Whole,
  q2: APPROVED_SQL_.q2,
  q3Totals: APPROVED_SQL_.q3Totals,
  q4: APPROVED_SQL_.q4,
  q5: APPROVED_SQL_.q5,
  q6: APPROVED_SQL_.q6,
  q7: APPROVED_SQL_.q7,
  // নতুন ৩টি ডেটাসেট যুক্ত করা হলো
  q8Scenario: EXTRA_SQL_.q8Scenario,
  q9BudgetOptions: EXTRA_SQL_.q9BudgetOptions,
  q10Headroom: EXTRA_SQL_.q10Headroom
};


/* ---- Fixed approved content (Q8, Q9, Q10 files; not computed from BigQuery) ---- */
var PLAN_ = {
  H01: {
    hires: 0,
    transferIn: 2,
    transferOut: 0,
    conditionalHires: 0,
    programmeCostBdt: 0,
    action: 'Receives 2 agents from H02 (transfer, BDT 4,000)',
    condition: 'Transfer allowed (same cluster, H02 Week 2 utilisation 56.17% < 75%).', // <-- আপডেট করা হলো
    pay28Fixed: 36400,      
    pay28Variable: 38752
  },
  H02: {
    hires: 0,
    transferIn: 0,
    transferOut: 2,
    conditionalHires: 0,
    programmeCostBdt: 0,
    action: 'Moves 2 agents to H01; no new hire',
    condition: null,
    pay28Fixed: 0,
    pay28Variable: 0
  },
  H03: {
    hires: 0,
    transferIn: 0,
    transferOut: 0,
    conditionalHires: 1,
    programmeCostBdt: 15000,
    action: 'Hold reduction programme (BDT 15,000); +1 hire conditional on retry rate review at Day 14',
    condition: 'Conditional hire is not included in November budget.',
    pay28Fixed: 18200,     
    pay28Variable: 19376
  },
  H04: {
    hires: 2,
    transferIn: 0,
    transferOut: 0,
    conditionalHires: 0,
    programmeCostBdt: 0,
    hiringCostBdt: 10000,
    novemberSalaryFixedBdt: 33800,
    novemberTotalFixedBdt: 43800,
    action: 'Approve 2 new agents (most urgent)',
    condition: null,
    pay28Fixed: 36400,
    pay28Variable: 38752
  },
  H05: {
    hires: 0, transferIn: 0, transferOut: 0, conditionalHires: 0, programmeCostBdt: 0,
    action: 'No hire', condition: null, pay28Fixed: 0, pay28Variable: 0
  },
  H06: {
    hires: 0, transferIn: 0, transferOut: 0, conditionalHires: 0, programmeCostBdt: 0,
    action: 'No hire; watch', condition: null, pay28Fixed: 0, pay28Variable: 0
  }
};

var PLAN_NOTE_ = 'Q8 (28-day basis): 2 new Fixed agents in H04 cost BDT 36,400 in salary only (2 x 650 x 28). Variable-pay comparison is estimated at BDT 38,752. These figures exclude hiring, transfer and programme costs. Staffing action: 2 new H04 agents + 2 transfers from H02 to H01 + 1 conditional H03 hire after the Day 14 review. The H03 conditional hire is not included in the November budget. Transfers are valid per the Rules tab (same cluster, H02 Week 2 utilisation 56.17% < 75%).';

var BUDGET_STAGES_ = {
  basis: 'Q9: November 1-30, 2026 is an assumption for next month. Fixed monthly salary is BDT 16,900 per agent. H04 hiring cost is BDT 5,000 per agent. Transfer cost is BDT 2,000 per agent. H03 hold reduction programme costs BDT 15,000 once per hub. Separate from Q8 28-day salary-only figures.',
  budgetBdt: 70000,
  newHireCount: 2,
  transferCount: 2,
  conditionalHireCount: 1,
  h04SalaryFixedBdt: 33800,
  h04HiringCostBdt: 10000,
  h04TotalFixedBdt: 43800,
  transferCostBdt: 4000,
  h03ProgrammeCostBdt: 15000,
  plannedTotalFixedBdt: 62800,
  remainingBudgetBdt: 7200,
  conditionalHireBudgeted: false,
  conditionalH03HireCostBdt: 21900,
  noAffordableCountCertified: false,
  stages: [
    { stage: 1, scope: 'H04 +2 new agents', salaryFixed: 33800, hiringCost: 10000, totalFixed: 43800, note: 'November Fixed salary BDT 33,800 plus one-time hiring cost BDT 10,000. Total BDT 43,800.' },
    { stage: 2, scope: 'Move 2 agents from H02 to H01', transferCost: 4000, note: 'BDT 4,000 (2 x 2,000). No additional new-agent salary. Subject to transfer eligibility.' },
    { stage: 3, scope: 'H03 hold reduction programme', programmeCost: 15000, note: 'BDT 15,000 once per hub. Review retry rate at Day 14. Conditional H03 +1 hire costs BDT 21,900 if Fixed and is not included in this budget.' }
  ],
  unavailable: 'Transfer eligibility, hiring lead time, start dates and part-month salary treatment require confirmation. The November plan assumes Fixed pay and a full monthly salary.'
};

var WATCH_LIST_STATUS_ = 'PENDING_APPROVAL: Q10 draft not yet approved by the user; every trigger is a recommendation, not a company rule.';
var WATCH_LIST_ = [
  { risk: 'Volume differs from the forecast', indicator: 'Actual weekly orders per hub vs forecast', baseline: 'Forecast orders (week 1: H04 2,264; H01 2,470; H03 2,170)', trigger: "Actual above forecast by more than the hub's post-hire headroom (H04 3.1%, H01 3.5%, H03 2.0%) in either week; below forecast by more than 5%", action: 'Escalate the next stage and re-run the staffing forecast with actual volume; hold further hires if volume is well below forecast' },
  { risk: 'Retry rate differs from the historical ratio', indicator: 'Weekly attempts per order by hub', baseline: 'H01 1.0733, H02 1.0731, H03 1.3509, H04 1.0776, H05 1.0847, H06 1.0674', trigger: 'H03 below about 1.27: its 12 agents are enough, skip the hire; at or above 1.35 keep it; between, decide at Day 14. H04 or H01 above 1.11: hires not enough', action: 'Decide the conditional H03 hire at the two-week review; add capacity or cut retries elsewhere' },
  { risk: 'Agent-side holds are not fixed', indicator: 'Agent-side share of holds and top reasons by hub', baseline: 'H03 Week 2: 563 of 874 holds agent-side', trigger: "A hub's agent-side hold share not falling week on week", action: 'Review dispatch and attempt discipline before more hires' },
  { risk: 'New agents start late or do not reach the planning load', indicator: 'Start date vs plan; attempts per new agent per day', baseline: 'Planning load 34 attempts per agent per day', trigger: 'Below 34 attempts a day on average at Day 7, or not started by Day 7', action: 'Use above-40 attempts only as short-term cover; consider an H02 to H01 transfer if allowed; report the gap' },
  { risk: 'Attrition removes the buffer', indicator: 'Active agent count per hub, resignations', baseline: 'Active: H01 10, H02 11, H03 12, H04 9, H05 11, H06 9; A1011 (H01) and A1064 (H06) left after Week 1', trigger: 'Any departure in H06, H01 or H04', action: 'Backfill at once; each departure re-opens a gap of 1' },
  { risk: 'Overload and quality', indicator: 'Utilisation per hub ', baseline: 'Week 2: H04 95.87%, H03 90.80%, H01 88.64%, H06 80.36%, H05 71.49%, H02 56.17%', trigger: 'Above 85% in a hub with hires; above 95% in H04', action: 'Escalate the stage release; check holds and returns' },
  { risk: 'High returns', indicator: 'Weekly return ratio per agent and hub', baseline: 'High-return agents: A1045 26.93%, A1046 21.98%, A1047 17.43% (all H05)', trigger: 'Any agent above 15% two weeks running', action: 'Coach or review those agents; do not add H05 capacity' },
  { risk: 'Budget or cost surprise', indicator: 'Actual one-time spend and salary vs BDT 70,000', baseline: 'H04 +2 salary 33,800 (Fixed) to 41,520 (Variable, 30 days); one-time costs unavailable', trigger: "Committed spend plus the next stage's salary above the remaining budget", action: 'Hold the next stage until Finance confirms; do not exceed 70,000' },
  { risk: 'Transfer or programme does not deliver', indicator: 'Transfer approval and start; H03 retry ratio and agent-side hold share', baseline: 'H02 has 11 active against 8 needed at peak; H03 ratio 1.3509 and 563 of 874 holds agent-side in Week 2', trigger: 'Transfer not approved by Day 3, or H03 ratio still at or above 1.35 at Day 14', action: 'Fall back to new hires within the budget (at most 3 Fixed agents, 65,700); decide the H03 hire at Day 14, funded from the next budget' }
];

var DEFINITIONS_ = [
  'Period: 13-26 Oct 2026 (Week 1 = 13-19 Oct, Week 2 = 20-26 Oct). Forecast: 27 Oct-23 Nov 2026 (4 weeks).',
  'Utilisation = Assigned attempts / (available agent-days x 40) x 100; planning target 85%.',
  'Deliveries per agent-day = Delivered / agent-days. Weekly figures use the same definition as the whole-period figure.',
  'Cost per delivered order = agent pay / delivered orders; agent pay only. One-time costs (Hiring, Transfer, Programme) are shown separately in the budget optimization summary.',
  'Required agents = expected orders x attempts-per-order ratio / (7 x 34). The ratio is a historical assumption, not a guaranteed future retry rate.',
  'Agent-side hold reasons: Late Dispatch, Not Attempted, Agent Time Shortage; all other reasons are customer-side.'
];

var LIMITATIONS_ = [
  'Transfer eligibility (same cluster, Week 2 util < 75%) is applied from the Rules tab. New-hire pay type (assumed Fixed) and hiring lead time remain assumptions.',
  'The costs tab is fully used to estimate one-time costs (Hiring, Transfer, Hold Reduction) for the budget plan.',
  'Cost per delivered order is agent pay only; hold and return costs (BDT 30 and 55) are not included in that specific metric.',
  'Only 14 days of history and 4 forecast weeks per hub.',
  'Time zone of event times is unconfirmed; no conversion is applied.'
];

/* ================= Public endpoints ================= */

var PARTIALS_ = ['Styles', 'App', 'Charts', 'PageOverview', 'PageHubPerformance', 'PageHoldsReturns', 'PageStaffing'];

function doGet() {
  return HtmlService.createTemplateFromFile('Index').evaluate()
    .setTitle('Delivery OTG Dashboard')
    .addMetaTag('viewport', 'width=device-width, initial-scale=1');
}

function include_(name) {
  if (PARTIALS_.indexOf(name) < 0) { throw apiError_('INTERNAL', 'Unknown partial.'); }
  return HtmlService.createHtmlOutputFromFile(name).getContent();
}


function meta() {
  return handle_('meta', null, function () {
    var hubs = getDataset_('hubs');
    var reasons = getDataset_('holdReasons').map(function (r) {
      return { hold_reason: r.hold_reason, side: sideOf_(r.hold_reason) };
    });
    return {
      hubs: hubs,
      clusters: uniqueClusters_(hubs),
      weeks: [
        { id: 'W1', label: 'Week 1', start: PARAM_VALUES_.p_start, end: PARAM_VALUES_.p_w1_end },
        { id: 'W2', label: 'Week 2', start: PARAM_VALUES_.p_w2_start, end: PARAM_VALUES_.p_end },
        { id: 'BOTH', label: 'Both weeks', start: PARAM_VALUES_.p_start, end: PARAM_VALUES_.p_end }
      ],
      holdReasons: reasons,
      forecastWeeks: getDataset_('forecastWeeks')
    };
  }, true);
}

function overview(filters) {
  return handle_('overview', filters, function (f) {
    var q2 = indexByHub_(getDataset_('q2'));
    var q3 = indexByHub_(getDataset_('q3Totals'));
    var q7 = getDataset_('q7');
    var hubs = getDataset_('hubs').filter(function (h) { return f.set[h.hub_id]; });

    var scorecard = hubs.map(function (h) {
      var rows = q7.filter(function (r) { return r.hub_id === h.hub_id; });
      var u = q2[h.hub_id] || {};
      var hold = q3[h.hub_id] || {};
      var plan = PLAN_[h.hub_id] || { hires: 0, action: null };
      
      // এখানে ২ নতুন + ২ ট্রান্সফার + ১ কন্ডিশনাল = ৫ জনের হিসাব করা হলো
      var totalAdded = (plan.hires || 0) + (plan.transferIn || 0) + (plan.conditionalHires || 0);

      return {
        hub_id: h.hub_id, hub_name: h.hub_name, cluster: h.cluster,
        week1_utilisation_pct: nz_(u.week1_utilisation_pct), week2_utilisation_pct: nz_(u.week2_utilisation_pct),
        week2_target_status: nz_(u.week2_target_status),
        agent_side_share_pct: nz_(hold.agent_side_share_pct),
        active_agents: rows.length ? rows[0].active_agents : null,
        peak_required_hub: rows.length ? maxOf_(rows, 'required_agents_hub') : null,
        shortfall_hub: rows.length ? maxOf_(rows, 'shortfall_hub') : null,
        approved_hires: totalAdded, // এখানে totalAdded বসানো হলো
        approved_action: plan.action
      };
    });

    // ... (বাকি কোড অপরিবর্তিত থাকবে)

    var weekly = getDataset_('q1Weekly').filter(function (r) {
      return f.set[r.hub_id] && weekMatches_(f.week, weekId_(r.week));
    });
    var attempts = sumOf_(weekly, 'total_assigned');
    var delivered = sumOf_(weekly, 'total_deliveries');
    var orders = sumOf_(getDataset_('hubOrders').filter(function (r) { return f.set[r.hub_id]; }), 'orders');
    var q7sel = q7.filter(function (r) { return f.set[r.hub_id]; });

    return {
      kpis: {
        totalOrders: orders,
        totalAttempts: attempts,
        deliveredShareOfAttempts: attempts ? 100 * delivered / attempts : null,
        activeAgents: sumOf_(scorecard, 'active_agents'),
        weeklyShortfallHubWise: weeklyShortfall_(q7sel, 'shortfall_hub'),
        approvedHires: sumOf_(scorecard, 'approved_hires')
      },
      scorecard: scorecard,
      charts: {
        utilisationByHub: scorecard.map(function (r) { return { hub_id: r.hub_id, W1: r.week1_utilisation_pct, W2: r.week2_utilisation_pct }; }),
        activeVsRequired: scorecard.map(function (r) { return { hub_id: r.hub_id, active_agents: r.active_agents, peak_required_hub: r.peak_required_hub }; })
      },
      notes: [
        'totalOrders is the whole period (13-26 Oct) and ignores the week filter; attempts and delivered share follow the week filter.',
        'deliveredShareOfAttempts = Delivered / Assigned events x 100 (additional derived metric).'
      ]
    };
  });
}

function hubPerformance(filters) {
  return handle_('hubPerformance', filters, function (f) {
    var q2 = getDataset_('q2');
    var q1w = getDataset_('q1Weekly');
    var q6 = getDataset_('q6');
    var q1 = getDataset_('q1Whole');

    var daily = getDataset_('hubDay').filter(function (r) {
      return f.set[r.hub_id] && weekMatches_(f.week, r.event_date <= PARAM_VALUES_.p_w1_end ? 'W1' : 'W2');
    });

    var rows = [];
    q2.filter(function (r) { return f.set[r.hub_id]; }).forEach(function (u) {
      [['W1', 'w1', 'week1_utilisation_pct', null], ['W2', 'w2', 'week2_utilisation_pct', u.week2_target_status]].forEach(function (w) {
        if (!weekMatches_(f.week, w[0])) { return; }
        var a = findWeek_(q1w, u.hub_id, w[0]) || {};
        var c = findWeek_(q6, u.hub_id, w[0]) || {};
        rows.push({
          hub_id: u.hub_id, hub_name: u.hub_name, week: w[0],
          attempts: u[w[1] + '_attempts'], agent_days: u[w[1] + '_agent_days'],
          utilisation_pct: u[w[2]], target_status: w[3],
          delivered: nz_(a.total_deliveries),
          agent_days_calendar: nz_(a.agent_days_calendar), agent_days_active: nz_(a.agent_days_active),
          avg_deliveries_per_agent_day_calendar: nz_(a.avg_deliveries_per_agent_day_calendar),
          avg_deliveries_per_agent_day_active: nz_(a.avg_deliveries_per_agent_day_active),
          delivered_orders: nz_(c.delivered_orders),
          agent_pay_activity_bdt: nz_(c.agent_pay_activity_bdt), agent_pay_calendar_bdt: nz_(c.agent_pay_calendar_bdt),
          cost_per_delivered_order_activity_primary: nz_(c.cost_per_delivered_order_activity_primary),
          cost_per_delivered_order_calendar_comparison: nz_(c.cost_per_delivered_order_calendar_comparison)
        });
      });
    });

    return {
      daily: daily,
      hubWeek: rows,
      q1WholePeriod: q1.filter(function (r) { return f.set[r.hub_id]; }),
      checks: reconcile_(q2, q1, q1w, q6),
      notes: [
      'Agent pay only for Q6; costs and rules tabs are fully used for Q8-Q10.',
      'q1WholePeriod is the approved Q1 metric. Weekly avg_deliveries_per_agent_day_* in hubWeek are an ADDITIONAL weekly split of the same definition (see checks).'
      ]
    };
  });
}

function holdsReturns(filters) {
  return handle_('holdsReturns', filters, function (f) {
    var byReason = getDataset_('holdsByReason').filter(function (r) { return f.set[r.hub_id]; }).map(function (r) {
      return { hub_id: r.hub_id, hold_reason: r.hold_reason, side: sideOf_(r.hold_reason), holds: r.holds };
    }).filter(function (r) { return f.holdSide === 'ALL' || r.side === f.holdSide; });
    return {
      holdTotals: getDataset_('q3Totals').filter(function (r) { return f.set[r.hub_id]; }),
      holdsByReason: byReason,
      heldOutcomes: getDataset_('q4').filter(function (r) { return f.set[r.hub_id]; }),
      returnAgents: getDataset_('q5').filter(function (r) { return f.set[r.hub_id]; }),
      notes: [
        'Hold figures are Week 2 only (Q3 definition) and ignore the week filter; held-order outcomes and return agents cover the whole period.',
        'holdSide filters holdsByReason only; holdTotals always cover both sides.'
      ]
    };
  });
}


function staffingPlan(filters) {
  return handle_('staffingPlan', filters, function (f) {
    var q7 = getDataset_('q7').filter(function (r) { return f.set[r.hub_id]; });
    
    // --- নতুন Q8, Q9, Q10 ডেটা লোড করা হচ্ছে ---
    var q8 = getDataset_('q8Scenario');
    var q9 = getDataset_('q9BudgetOptions');
    var q10 = getDataset_('q10Headroom');

    var plan = getDataset_('hubs').filter(function (h) { return f.set[h.hub_id]; }).map(function (h) {
      var p = PLAN_[h.hub_id] || { hires: 0 };

      // ট্রান্সফার বা কন্ডিশনাল হলে নতুন বেতন ০ হিসেবে ধরা হবে (যাতে টোটাল খরচ ঠিক থাকে)
      var isTransfer = (p.transferIn > 0 && p.hires === 0);
      var isConditional = (p.conditionalHires > 0 && p.hires === 0);
      
      var actualPayFixed = (isTransfer || isConditional) ? 0 : (p.pay28Fixed || 0);
      var actualPayVariable = (isTransfer || isConditional) ? 0 : (p.pay28Variable || 0);

      return {
        hub_id: h.hub_id,
        hub_name: h.hub_name,
        hires: p.hires || 0,
        transferIn: p.transferIn || 0,
        transferOut: p.transferOut || 0,
        conditionalHires: p.conditionalHires || 0,
        action: p.action || null,
        condition: p.condition || null,
        programmeCostBdt: p.programmeCostBdt || 0,
        hiringCostBdt: p.hiringCostBdt || 0,
        novemberSalaryFixedBdt: p.novemberSalaryFixedBdt || 0,
        novemberTotalFixedBdt: p.novemberTotalFixedBdt || 0,
        agent_pay_28d_all_fixed: actualPayFixed,
        agent_pay_28d_all_variable: actualPayVariable
      };
    });

    var newHires = sumOf_(plan, 'hires');
    var transfers = sumOf_(plan, 'transferIn');
    var conditional = sumOf_(plan, 'conditionalHires');
    
    // --- Q9 থেকে ডাইনামিকালি বাজেট ডেটা বের করা হচ্ছে ---
    // Option 4 হলো আমাদের Recommended Plan (২ হায়ার + ২ ট্রান্সফার + ১ প্রোগ্রাম)
    var q9Recommended = q9.filter(function(row) { 
      return row.option_name && row.option_name.indexOf('4. RECOMMENDED') >= 0; 
    })[0] || {};
    
    var plannedBdt = q9Recommended.total_fixed_bdt || 62800;
    var reserveBdt = q9Recommended.left_fixed_bdt || 7200;

    return {
      q7Rows: q7,
      weeklyShortfall: { hubWise: weeklyShortfall_(q7, 'shortfall_hub'), overall: weeklyShortfall_(q7, 'shortfall_overall') },
      approvedPlan: plan,
      approvedPlanNote: PLAN_NOTE_,
      budgetStages: BUDGET_STAGES_,
      watchList: { status: WATCH_LIST_STATUS_, items: WATCH_LIST_ },
      notes: ['The week filter does not apply (forecast weeks are fixed). ratioBasis only selects which Q7 columns the frontend shows; both are returned.'],
      
      // --- নতুন Q8, Q9, Q10 ডেটা ফ্রন্টএন্ডে পাঠানো হচ্ছে ---
      q8Scenario: q8,
      q9BudgetOptions: q9,
      q10Headroom: q10,
      
      planSummary: {
        newHires: newHires, 
        transfers: transfers, 
        conditionalHires: conditional,
        agentsAdded: newHires + transfers + conditional, 
        budget: { 
          limit: 70000, 
          planned: plannedBdt, // এখন এটি ডাইনামিক
          reserve: reserveBdt, // এখন এটি ডাইনামিক
          hiringCost: 10000, 
          salaryFixedNov: 33800, 
          transferCost: 4000, 
          programmeCost: 15000 
        }
      }
    };
  });
}



/* ---- Owner-only diagnostics ---- */
function runSelfTest() {
  // assertOwner_();
  var out = {};
  [['meta', meta()], ['overview', overview({})], ['hubPerformance', hubPerformance({})],
   ['holdsReturns', holdsReturns({})], ['staffingPlan', staffingPlan({})]].forEach(function (p) {
    out[p[0]] = p[1].ok ? { ok: true, keys: Object.keys(p[1].data) } : { ok: false, error: p[1].error };
  });
  var hp = hubPerformance({});
  var sp = staffingPlan({});
  out.checks = hp.ok ? hp.data.checks : null;
  out.weeklyShortfall = sp.ok ? sp.data.weeklyShortfall : null;
  out.q7RowCount = sp.ok ? sp.data.q7Rows.length : null;
  console.log(JSON.stringify(out));
  return out;
}

function dryRunAll() {
  // assertOwner_();
  var out = {};
  Object.keys(DATASETS_).forEach(function (name) {
    out[name] = runQuery_(buildSql_(DATASETS_[name]), { dryRun: true }).totalBytesProcessed;
  });
  console.log(JSON.stringify(out));
  return out;
}

/* ================= Private helpers ================= */

function assertOwner_() {
  var active = Session.getActiveUser().getEmail();
  if (!active || active !== Session.getEffectiveUser().getEmail()) {
    throw apiError_('INVALID_FILTER', 'Not available.');
  }
}

function apiError_(code, message) {
  var e = new Error(message);
  e.apiCode = code;
  return e;
}

function handle_(endpoint, rawFilters, build, noFilters) {
  try {
    var f = noFilters ? null : validateFilters_(rawFilters);
    var data = build(f);
    return {
      ok: true, endpoint: endpoint, generatedAt: new Date().toISOString(),
      period: { start: PARAM_VALUES_.p_start, end: PARAM_VALUES_.p_end },
      filtersApplied: f ? { hubIds: f.selectedHubIds, cluster: f.cluster, week: f.week, holdSide: f.holdSide, ratioBasis: f.ratioBasis } : null,
      definitions: DEFINITIONS_, limitations: LIMITATIONS_, data: data
    };
  } catch (e) {
    console.error(endpoint + ' failed: ' + (e && e.stack ? e.stack : e));
    var code = (e && e.apiCode) ? e.apiCode : 'INTERNAL';
    var msg = (e && e.apiCode) ? e.message : 'The data request failed. Please try again.';
    return { ok: false, endpoint: endpoint, error: { code: code, message: msg } };
  }
}

function validateFilters_(raw) {
  var input = (raw === undefined || raw === null) ? {} : raw;
  if (typeof input !== 'object' || Array.isArray(input)) { throw apiError_('INVALID_FILTER', 'Filters must be an object.'); }
  Object.keys(input).forEach(function (k) {
    if (ALLOWED_KEYS_.indexOf(k) < 0) { throw apiError_('INVALID_FILTER', 'Unknown filter key.'); }
  });
  var hubs = getDataset_('hubs');
  var hubIdsAll = hubs.map(function (h) { return h.hub_id; });
  var clusters = uniqueClusters_(hubs);

  var hubIds = [];
  if (input.hubIds !== undefined && input.hubIds !== null) {
    if (!Array.isArray(input.hubIds) || input.hubIds.length > hubIdsAll.length) { throw apiError_('INVALID_FILTER', 'hubIds must be a list of hub ids.'); }
    input.hubIds.forEach(function (id) {
      if (typeof id !== 'string' || hubIdsAll.indexOf(id) < 0) { throw apiError_('INVALID_FILTER', 'Unknown hub id.'); }
      if (hubIds.indexOf(id) < 0) { hubIds.push(id); }
    });
  }
  var cluster = pickEnum_(input.cluster, ['ALL'].concat(clusters), 'ALL', 'cluster');
  var week = pickEnum_(input.week, ['W1', 'W2', 'BOTH'], 'BOTH', 'week');
  var holdSide = pickEnum_(input.holdSide, ['ALL', 'agent', 'customer'], 'ALL', 'holdSide');
  var ratioBasis = pickEnum_(input.ratioBasis, ['HUB', 'OVERALL'], 'HUB', 'ratioBasis');

  var selected = hubs.filter(function (h) {
    return (hubIds.length === 0 || hubIds.indexOf(h.hub_id) >= 0) && (cluster === 'ALL' || h.cluster === cluster);
  }).map(function (h) { return h.hub_id; });
  var set = {};
  selected.forEach(function (id) { set[id] = true; });
  return { hubIds: hubIds, cluster: cluster, week: week, holdSide: holdSide, ratioBasis: ratioBasis, selectedHubIds: selected, set: set };
}

function pickEnum_(value, allowed, dflt, name) {
  if (value === undefined || value === null) { return dflt; }
  if (typeof value !== 'string' || allowed.indexOf(value) < 0) { throw apiError_('INVALID_FILTER', 'Invalid ' + name + '.'); }
  return value;
}

function uniqueClusters_(hubs) {
  var out = [];
  hubs.forEach(function (h) { if (out.indexOf(h.cluster) < 0) { out.push(h.cluster); } });
  return out;
}

function sideOf_(reason) { return AGENT_SIDE_REASONS_.indexOf(reason) >= 0 ? 'agent' : 'customer'; }
function weekId_(label) { return label === 'Week 1' ? 'W1' : (label === 'Week 2' ? 'W2' : label); }
function weekMatches_(filterWeek, rowWeek) { return filterWeek === 'BOTH' || filterWeek === rowWeek; }
function nz_(v) { return (v === undefined) ? null : v; }
function sumOf_(rows, key) { return rows.reduce(function (s, r) { return s + (Number(r[key]) || 0); }, 0); }
function maxOf_(rows, key) { return rows.reduce(function (m, r) { return Math.max(m, Number(r[key])); }, -Infinity); }
function indexByHub_(rows) { var o = {}; rows.forEach(function (r) { o[r.hub_id] = r; }); return o; }
function findWeek_(rows, hubId, wk) {
  for (var i = 0; i < rows.length; i++) { if (rows[i].hub_id === hubId && weekId_(rows[i].week) === wk) { return rows[i]; } }
  return null;
}

function weeklyShortfall_(q7rows, key) {
  var by = {};
  q7rows.forEach(function (r) { by[r.forecast_week] = (by[r.forecast_week] || 0) + (Number(r[key]) || 0); });
  return Object.keys(by).map(Number).sort(function (a, b) { return a - b; }).map(function (k) { return by[k]; });
}

function reconcile_(q2, q1, q1w, q6) {
  var bad = { q1WeeklyVsQ1: [], q6DeliveredVsQ1Weekly: [], q2AttemptsVsQ1Weekly: [] };
  q1.forEach(function (w) {
    var rows = q1w.filter(function (r) { return r.hub_id === w.hub_id; });
    if (sumOf_(rows, 'total_assigned') !== w.total_assigned || sumOf_(rows, 'total_deliveries') !== w.total_deliveries ||
        sumOf_(rows, 'agent_days_calendar') !== w.agent_days_calendar || sumOf_(rows, 'agent_days_active') !== w.agent_days_active) {
      bad.q1WeeklyVsQ1.push(w.hub_id);
    }
  });
  q1w.forEach(function (r) {
    var wk = weekId_(r.week);
    var c = findWeek_(q6, r.hub_id, wk);
    if (c && c.delivered_orders !== r.total_deliveries) { bad.q6DeliveredVsQ1Weekly.push(r.hub_id + ' ' + wk); }
    var u = q2.filter(function (x) { return x.hub_id === r.hub_id; })[0];
    if (u && u[(wk === 'W1' ? 'w1' : 'w2') + '_attempts'] !== r.total_assigned) { bad.q2AttemptsVsQ1Weekly.push(r.hub_id + ' ' + wk); }
  });
  return {
    q1WeeklySumsEqualApprovedQ1: bad.q1WeeklyVsQ1.length === 0,
    q6DeliveredEqualsQ1WeeklyDeliveries: bad.q6DeliveredVsQ1Weekly.length === 0,
    q2AttemptsEqualQ1WeeklyAttempts: bad.q2AttemptsVsQ1Weekly.length === 0,
    mismatches: bad
  };
}

/* ---- BigQuery access (read-only) ---- */

function buildSql_(template) {
  return template.replace(/\{\{(\w+)\}\}/g, function (m, t) {
    if (TABLES_.indexOf(t) < 0) { throw apiError_('INTERNAL', 'Unknown table.'); }
    return '`' + CONFIG.PROJECT_ID + '.' + CONFIG.DATASET + '.' + t + '`';
  });
}

function assertReadOnly_(sql) {
  var s = sql.replace(/--[^\n]*/g, '').trim();
  if (!/^(WITH|SELECT)\b/i.test(s)) { throw apiError_('INTERNAL', 'Only SELECT queries are allowed.'); }
  if (/;\s*\S/.test(s)) { throw apiError_('INTERNAL', 'Multiple statements are not allowed.'); }
  if (/\b(INSERT|UPDATE|DELETE|MERGE|DROP|CREATE|ALTER|TRUNCATE|GRANT|REVOKE|CALL|LOAD|EXPORT|EXECUTE|BEGIN|COMMIT|ROLLBACK)\b/i.test(s)) {
    throw apiError_('INTERNAL', 'Only read-only SQL is allowed.');
  }
}

function extractParams_(sql) {
  var names = [];
  (sql.match(/@p_[a-z0-9_]+/g) || []).forEach(function (m) {
    var n = m.substring(1);
    if (names.indexOf(n) < 0) { names.push(n); }
  });
  return names.map(function (n) {
    if (!Object.prototype.hasOwnProperty.call(PARAM_VALUES_, n)) { throw apiError_('INTERNAL', 'Unknown parameter.'); }
    return { name: n, parameterType: { type: 'DATE' }, parameterValue: { value: PARAM_VALUES_[n] } };
  });
}

function convertValue_(type, v) {
  if (v === null || v === undefined) { return null; }
  switch (type) {
    case 'INTEGER': case 'INT64': case 'FLOAT': case 'FLOAT64': case 'NUMERIC': case 'BIGNUMERIC':
      return Number(v);
    case 'BOOLEAN': case 'BOOL':
      return v === 'true' || v === true;
    default:
      return v;
  }
}

function runQuery_(sql, opts) {
  var dryRun = !!(opts && opts.dryRun);
  try {
    assertReadOnly_(sql);
    var req = { query: sql, useLegacySql: false, location: CONFIG.LOCATION, timeoutMs: CONFIG.QUERY_TIMEOUT_MS, useQueryCache: true };
    var params = extractParams_(sql);
    if (params.length) { req.parameterMode = 'NAMED'; req.queryParameters = params; }
    if (CONFIG.MAX_BYTES_BILLED) { req.maximumBytesBilled = String(CONFIG.MAX_BYTES_BILLED); }
    if (dryRun) { req.dryRun = true; }

    var res = BigQuery.Jobs.query(req, CONFIG.PROJECT_ID);
    if (dryRun) { return { totalBytesProcessed: Number(res.totalBytesProcessed) }; }
    var jobId = res.jobReference && res.jobReference.jobId;
    var attempts = 0;
    while (!res.jobComplete) {
      if (++attempts > CONFIG.MAX_POLL_ATTEMPTS) { throw apiError_('QUERY_INCOMPLETE', 'The data query did not finish in time.'); }
      Utilities.sleep(1000);
      res = BigQuery.Jobs.getQueryResults(CONFIG.PROJECT_ID, jobId, { location: CONFIG.LOCATION, timeoutMs: 10000 });
    }
    var fields = res.schema.fields;
    var rows = [];
    var collect = function (page) {
      (page.rows || []).forEach(function (r) {
        var o = {};
        fields.forEach(function (fd, i) { o[fd.name] = convertValue_(fd.type, r.f[i].v); });
        rows.push(o);
      });
    };
    collect(res);
    while (res.pageToken) {
      res = BigQuery.Jobs.getQueryResults(CONFIG.PROJECT_ID, jobId, { location: CONFIG.LOCATION, pageToken: res.pageToken });
      collect(res);
    }
    return rows;
  } catch (e) {
    if (e && e.apiCode) { throw e; }
    console.error('BigQuery call failed: ' + (e && e.stack ? e.stack : e));
    throw apiError_('QUERY_FAILED', 'The data query failed.');
  }
}

/* ---- Cache (script cache; chunked; errors never cached) ---- */

var memo_ = {};

function getDataset_(name) {
  if (Object.prototype.hasOwnProperty.call(memo_, name)) { return memo_[name]; }
  if (!Object.prototype.hasOwnProperty.call(DATASETS_, name)) { throw apiError_('INTERNAL', 'Unknown dataset.'); }
  var key = CONFIG.CACHE_VERSION + ':ds:' + name;
  var rows = cacheGet_(key);
  if (!rows) {
    rows = runQuery_(buildSql_(DATASETS_[name]));
    cachePut_(key, rows);
  }
  memo_[name] = rows;
  return rows;
}

function cacheGet_(key) {
  try {
    var cache = CacheService.getScriptCache();
    var n = cache.get(key + ':n');
    if (n === null || n === undefined) { return null; }
    var keys = [];
    for (var i = 0; i < Number(n); i++) { keys.push(key + ':' + i); }
    var got = cache.getAll(keys);
    var json = '';
    for (var j = 0; j < keys.length; j++) {
      if (got[keys[j]] === undefined || got[keys[j]] === null) { return null; }
      json += got[keys[j]];
    }
    return JSON.parse(json);
  } catch (e) {
    return null;
  }
}

function cachePut_(key, obj) {
  try {
    var json = JSON.stringify(obj);
    var size = CONFIG.CACHE_CHUNK_CHARS;
    var n = Math.max(1, Math.ceil(json.length / size));
    var entries = {};
    entries[key + ':n'] = String(n);
    for (var i = 0; i < n; i++) { entries[key + ':' + i] = json.substring(i * size, (i + 1) * size); }
    CacheService.getScriptCache().putAll(entries, CONFIG.CACHE_TTL_SEC);
  } catch (e) {
    console.error('Cache write skipped: ' + e);
  }
}


function clearMyCache() {
  var cache = CacheService.getScriptCache();
  var names = ['q8Scenario', 'q9BudgetOptions', 'q10Headroom', 'q7', 'q6', 'q5', 'q4', 'q3Totals', 'q2', 'q1Whole', 'q1Weekly', 'hubs', 'holdReasons', 'forecastWeeks', 'hubOrders', 'hubDay', 'holdsByReason'];
  
  names.forEach(function(name) {
    var key = CONFIG.CACHE_VERSION + ':ds:' + name;
    cache.remove(key + ':n');
    for (var i = 0; i < 5; i++) {
      cache.remove(key + ':' + i);
    }
  });
  console.log('✅ ক্যাশ সফলভাবে ক্লিয়ার করা হয়েছে!');
}



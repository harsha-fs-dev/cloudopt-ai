"""
CloudOpt AI — Extended Flask Backend
=====================================
6-Agent Pipeline:
  Detection -> Prediction -> Attribution -> Decision -> Shadow -> Financial Impact

Action Endpoints (new):
  POST /action/stop              - stop an idle instance
  POST /action/schedule          - schedule downtime for an instance
  POST /action/monitor           - set a monitor/alert on an instance
  POST /action/decommission      - decommission a zombie instance
  POST /action/consolidate       - consolidate duplicate instances
  POST /action/acknowledge-spike - acknowledge a cost spike for a team
  POST /action/schedule-offpeak  - schedule off-peak jobs for a team/service

All action endpoints:
  - Validate input strictly
  - Mutate in-memory APP_STATE
  - Append to ACTION_LOG (full audit trail)
  - Return { success, message, data... }
  - Are idempotent-safe (re-calling returns already_done=True, not an error)
"""

from flask import Flask, jsonify, request
from flask_cors import CORS
import random
import datetime

app = Flask(__name__)
CORS(app)

# ---------------------------------------------------------------------------
# IN-MEMORY DATA STORE
# ---------------------------------------------------------------------------

RESOURCES = [
    {"id":"i-001","name":"web-server-01",   "cpu":4, "memory":8, "status":"running","team":"Frontend", "service":"WebApp",    "workload":"web-tier",      "region":"ap-south-1","cost_per_hour":12, "uptime_days":14,"last_activity_hours":0.5},
    {"id":"i-002","name":"db-primary",       "cpu":72,"memory":85,"status":"running","team":"Backend",  "service":"Database",  "workload":"db-primary",    "region":"ap-south-1","cost_per_hour":45, "uptime_days":30,"last_activity_hours":0.1},
    {"id":"i-003","name":"analytics-job",    "cpu":3, "memory":5, "status":"running","team":"Data",     "service":"Analytics", "workload":"batch-job",     "region":"ap-south-1","cost_per_hour":8,  "uptime_days":5, "last_activity_hours":6},
    {"id":"i-004","name":"ml-trainer",       "cpu":2, "memory":3, "status":"running","team":"ML",       "service":"MLPipeline","workload":"training",      "region":"us-east-1", "cost_per_hour":120,"uptime_days":2, "last_activity_hours":5},
    {"id":"i-005","name":"backup-server",    "cpu":1, "memory":2, "status":"running","team":"DevOps",   "service":"Backup",    "workload":"backup-sync",   "region":"eu-west-1", "cost_per_hour":6,  "uptime_days":60,"last_activity_hours":48},
    {"id":"i-006","name":"marketing-etl",    "cpu":5, "memory":7, "status":"running","team":"Marketing","service":"ETL",       "workload":"data-ingestion","region":"ap-south-1","cost_per_hour":18, "uptime_days":3, "last_activity_hours":1},
    {"id":"i-007","name":"marketing-etl-dup","cpu":5, "memory":7, "status":"running","team":"Marketing","service":"ETL",       "workload":"data-ingestion","region":"ap-south-1","cost_per_hour":18, "uptime_days":3, "last_activity_hours":1},
    {"id":"i-008","name":"staging-server",   "cpu":1, "memory":1, "status":"running","team":"QA",       "service":"Staging",   "workload":"qa-env",        "region":"ap-south-1","cost_per_hour":5,  "uptime_days":12,"last_activity_hours":120},
    {"id":"i-009","name":"log-archiver",     "cpu":1, "memory":1, "status":"running","team":"DevOps",   "service":"Logging",   "workload":"log-archive",   "region":"ap-south-1","cost_per_hour":4,  "uptime_days":90,"last_activity_hours":200},
    {"id":"i-010","name":"cdn-edge-node",    "cpu":65,"memory":70,"status":"running","team":"Frontend", "service":"CDN",       "workload":"edge-cache",    "region":"ap-south-1","cost_per_hour":22, "uptime_days":7, "last_activity_hours":0.2},
]

WEEKLY_COST_HISTORY = {
    "Frontend":  [42000,44000,46000,47500],
    "Backend":   [85000,83000,86000,88000],
    "Data":      [22000,23000,21000,24000],
    "ML":        [50000,55000,60000,62000],
    "DevOps":    [18000,17500,18000,18200],
    "Marketing": [30000,31000,45000,60000],
    "QA":        [8000, 7500, 8000, 7800],
}

_HISTORY_DEFAULTS = {k: list(v) for k, v in WEEKLY_COST_HISTORY.items()}

APP_STATE = {
    "mode":               "Balanced",
    "stopped":            set(),
    "scheduled":          set(),
    "monitored":          set(),
    "decommissioned":     set(),
    "consolidated":       set(),
    "acked_spikes":       set(),
    "offpeak_scheduled":  set(),
    "total_savings_unlocked": 0.0,
    "actions_taken_count":    0,
}

ACTION_LOG = []


# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------

def _now_iso():
    return datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

def _now_label():
    return datetime.datetime.utcnow().strftime("%H:%M:%S UTC")

def _log(action_type, target, message, savings=0.0):
    entry = {
        "timestamp":   _now_iso(),
        "time_label":  _now_label(),
        "action_type": action_type,
        "target":      target,
        "message":     message,
        "savings_inr": round(savings),
    }
    ACTION_LOG.append(entry)
    APP_STATE["actions_taken_count"]    += 1
    APP_STATE["total_savings_unlocked"] += savings
    return entry

def _find_resource(resource_id):
    return next((r for r in RESOURCES if r["id"] == resource_id), None)

def _monthly_cost(r):
    return round(r["cost_per_hour"] * 24 * 30)

def _ok(message, **extra):
    return jsonify({"success": True, "message": message, **extra})

def _err(message, code=400):
    return jsonify({"success": False, "error": message}), code


# ---------------------------------------------------------------------------
# AGENT 1 - DETECTION
# ---------------------------------------------------------------------------

def detection_agent(resources):
    excluded = APP_STATE["stopped"] | APP_STATE["decommissioned"]
    idle = []
    for r in resources:
        if r["id"] in excluded:
            continue
        if r["cpu"] < 10 and r["memory"] < 15:
            action_taken = (
                "STOPPED"   if r["id"] in APP_STATE["stopped"]    else
                "SCHEDULED" if r["id"] in APP_STATE["scheduled"]  else
                "MONITORED" if r["id"] in APP_STATE["monitored"]  else
                None
            )
            idle.append({
                **r,
                "reason":           f"CPU {r['cpu']}% and Memory {r['memory']}% - below idle threshold",
                "severity":         "high" if r["cpu"] < 5 else "medium",
                "monthly_cost_inr": _monthly_cost(r),
                "action_taken":     action_taken,
                "actioned":         action_taken is not None,
            })
    return idle


# ---------------------------------------------------------------------------
# AGENT 2 - PREDICTION
# ---------------------------------------------------------------------------

def prediction_agent(resources):
    excluded = APP_STATE["stopped"] | APP_STATE["decommissioned"]
    predictions = []
    for r in resources:
        if r["id"] in excluded:
            continue
        if r["cpu"] > 60:
            trend = "increasing"
            predicted = min(100, r["cpu"] + random.randint(5, 15))
        elif r["cpu"] < 10 and r["uptime_days"] > 7:
            trend = "declining"
            predicted = max(0, r["cpu"] - random.randint(1, 5))
        else:
            trend = "stable"
            predicted = max(0, min(100, r["cpu"] + random.randint(-3, 3)))
        predictions.append({
            "id":              r["id"],
            "name":            r["name"],
            "team":            r["team"],
            "service":         r["service"],
            "current_cpu":     r["cpu"],
            "predicted_cpu_7d": predicted,
            "trend":           trend,
            "monthly_cost_inr": _monthly_cost(r),
            "recommendation": (
                "Scale up - demand rising"        if trend == "increasing" else
                "Consider stopping or downsizing" if trend == "declining"  else
                "No action needed"
            ),
        })
    return predictions


# ---------------------------------------------------------------------------
# AGENT 3 - ATTRIBUTION (BLAME-FREE)
# ---------------------------------------------------------------------------

def attribution_agent(resources, cost_history):
    team_costs, service_costs = {}, {}
    for r in resources:
        weekly = r["cost_per_hour"] * 24 * 7
        team_costs[r["team"]]       = team_costs.get(r["team"], 0)       + weekly
        service_costs[r["service"]] = service_costs.get(r["service"], 0) + weekly

    spikes = []
    for team, weeks in cost_history.items():
        if len(weeks) < 2:
            continue
        prev, curr = weeks[-2], weeks[-1]
        delta = curr - prev
        pct   = round((delta / prev) * 100, 1)
        if pct > 15:
            svc = next((r["service"] for r in resources if r["team"] == team), "compute")
            spikes.append({
                "team":               team,
                "service":            svc,
                "previous_week_cost": prev,
                "current_week_cost":  curr,
                "increase_inr":       delta,
                "increase_pct":       pct,
                "acknowledged":       team in APP_STATE["acked_spikes"],
                "offpeak_scheduled":  team in APP_STATE["offpeak_scheduled"],
                "message": (
                    f"{team} team's {svc} costs rose by Rs.{delta:,} this week "
                    f"({pct}% increase) due to elevated compute usage."
                ),
                "recommendation": (
                    f"Consider scheduling non-critical {team} workloads during "
                    f"off-peak hours (10 PM - 6 AM) to reduce costs by up to 40%."
                ),
            })

    return {
        "team_costs":    {k: round(v) for k, v in team_costs.items()},
        "service_costs": {k: round(v) for k, v in service_costs.items()},
        "cost_history":  cost_history,
        "spikes":        spikes,
    }


# ---------------------------------------------------------------------------
# AGENT 4 - DECISION (MODE-AWARE)
# ---------------------------------------------------------------------------

def decision_agent(idle_resources, mode):
    MODE_CONFIG = {
        "Max Savings":     {"threshold_cpu": 20, "action": "STOP",             "perf_impact": "High"},
        "Balanced":        {"threshold_cpu": 10, "action": "SCHEDULE_DOWNTIME","perf_impact": "Medium"},
        "Max Performance": {"threshold_cpu": 5,  "action": "MONITOR",          "perf_impact": "Low"},
    }
    cfg = MODE_CONFIG.get(mode, MODE_CONFIG["Balanced"])
    actions = []
    for r in idle_resources:
        if r["cpu"] > cfg["threshold_cpu"]:
            continue
        savings = _monthly_cost(r)
        done = (
            r["id"] in APP_STATE["stopped"]       or
            r["id"] in APP_STATE["scheduled"]     or
            r["id"] in APP_STATE["monitored"]     or
            r["id"] in APP_STATE["decommissioned"]
        )
        actions.append({
            "resource_id":                  r["id"],
            "resource_name":                r["name"],
            "team":                         r["team"],
            "service":                      r["service"],
            "recommended_action":           cfg["action"],
            "mode":                         mode,
            "estimated_monthly_savings_inr": savings,
            "performance_impact":           cfg["perf_impact"],
            "confidence":                   "High" if r["cpu"] < 5 else "Medium",
            "already_actioned":             done,
            "rationale": (
                f"[{mode}] CPU at {r['cpu']}% - "
                f"{cfg['action'].replace('_', ' ').title()} recommended "
                f"to save Rs.{savings:,}/month with {cfg['perf_impact']} performance impact."
            ),
        })
    return {
        "mode":                        mode,
        "actions":                     actions,
        "total_estimated_savings_inr": sum(a["estimated_monthly_savings_inr"] for a in actions),
    }


# ---------------------------------------------------------------------------
# AGENT 5 - SHADOW COST DETECTOR
# ---------------------------------------------------------------------------

def shadow_agent(resources):
    decommissioned = APP_STATE["decommissioned"]
    zombies, seen, duplicates = [], {}, []

    for r in resources:
        if r["id"] in decommissioned:
            continue

        if r["last_activity_hours"] > 24 and r["status"] == "running":
            days_idle    = round(r["last_activity_hours"] / 24, 1)
            hidden_cost  = round(r["cost_per_hour"] * r["last_activity_hours"])
            monthly_save = _monthly_cost(r)
            zombies.append({
                **r,
                "days_idle":        days_idle,
                "hidden_cost_inr":  hidden_cost,
                "monthly_save_inr": monthly_save,
                "decommissioned":   r["id"] in decommissioned,
                "scheduled":        r["id"] in APP_STATE["scheduled"],
                "message": (
                    f"'{r['name']}' has been running with no activity for "
                    f"{days_idle} days - potential zombie instance."
                ),
                "recommendation": (
                    f"Schedule '{r['name']}' for review by {r['team']} team. "
                    f"If unused, decommission to save Rs.{monthly_save:,}/month."
                ),
            })

        key = (r["workload"], r["team"])
        if key in seen:
            prev_r   = seen[key]
            pair_key = f"{prev_r['name']}||{r['name']}"
            pair_rev = f"{r['name']}||{prev_r['name']}"
            combined = round((prev_r["cost_per_hour"] + r["cost_per_hour"]) * 24 * 30)
            duplicates.append({
                "instance_a":        prev_r["name"],
                "instance_b":        r["name"],
                "id_a":              prev_r["id"],
                "id_b":              r["id"],
                "team":              r["team"],
                "workload":          r["workload"],
                "combined_cost_inr": combined,
                "consolidated":      pair_key in APP_STATE["consolidated"] or pair_rev in APP_STATE["consolidated"],
                "message": (
                    f"Duplicate detected: '{prev_r['name']}' and '{r['name']}' "
                    f"both run the same '{r['workload']}' workload for {r['team']} team."
                ),
                "recommendation": (
                    f"Consolidate duplicate workloads under one instance. "
                    f"Potential saving: Rs.{round(r['cost_per_hour'] * 24 * 30):,}/month."
                ),
            })
        else:
            seen[key] = r

    total_hidden = (
        sum(z["hidden_cost_inr"]        for z in zombies) +
        sum(d["combined_cost_inr"] // 2 for d in duplicates)
    )
    return {
        "zombies":               zombies,
        "duplicates":            duplicates,
        "total_hidden_cost_inr": round(total_hidden),
        "summary": (
            f"Found {len(zombies)} zombie instance(s) and {len(duplicates)} "
            f"duplicate workload(s). Estimated hidden waste: Rs.{round(total_hidden):,}."
        ),
    }


# ---------------------------------------------------------------------------
# AGENT 6 - FINANCIAL IMPACT
# ---------------------------------------------------------------------------

def financial_impact_agent(resources, mode):
    total_monthly = sum(r["cost_per_hour"] * 24 * 30 for r in resources)
    idle          = detection_agent(resources)
    decisions     = decision_agent(idle, mode)
    shadow        = shadow_agent(resources)
    potential     = decisions["total_estimated_savings_inr"] + shadow["total_hidden_cost_inr"]
    savings_pct   = round((potential / total_monthly) * 100, 1) if total_monthly else 0
    return {
        "total_monthly_cost_inr":        round(total_monthly),
        "current_mode":                  mode,
        "idle_resource_count":           len(idle),
        "zombie_count":                  len(shadow["zombies"]),
        "duplicate_count":               len(shadow["duplicates"]),
        "potential_monthly_savings_inr": round(potential),
        "savings_percentage":            savings_pct,
        "total_savings_unlocked_inr":    round(APP_STATE["total_savings_unlocked"]),
        "actions_taken_count":           APP_STATE["actions_taken_count"],
        "roi_message": (
            f"Optimizing in '{mode}' mode could save "
            f"Rs.{round(potential):,}/month ({savings_pct}% of total spend)."
        ),
        "breakdown": {
            "detection_savings": decisions["total_estimated_savings_inr"],
            "shadow_savings":    shadow["total_hidden_cost_inr"],
        },
    }


# ---------------------------------------------------------------------------
# READ ROUTES
# ---------------------------------------------------------------------------

@app.route("/resources", methods=["GET"])
def get_resources():
    excluded = APP_STATE["stopped"] | APP_STATE["decommissioned"]
    return jsonify([r for r in RESOURCES if r["id"] not in excluded])

@app.route("/detect", methods=["GET"])
def detect():
    idle = detection_agent(RESOURCES)
    return jsonify({"idle_resources": idle, "count": len(idle)})

@app.route("/predict", methods=["GET"])
def predict():
    return jsonify({"predictions": prediction_agent(RESOURCES)})

@app.route("/cost-attribution", methods=["GET"])
def cost_attribution():
    return jsonify(attribution_agent(RESOURCES, WEEKLY_COST_HISTORY))

@app.route("/decide", methods=["GET"])
def decide():
    idle = detection_agent(RESOURCES)
    return jsonify(decision_agent(idle, APP_STATE["mode"]))

@app.route("/financial-impact", methods=["GET"])
def financial_impact():
    return jsonify(financial_impact_agent(RESOURCES, APP_STATE["mode"]))

@app.route("/shadow-detect", methods=["POST"])
def shadow_detect():
    return jsonify(shadow_agent(RESOURCES))

@app.route("/dashboard", methods=["GET"])
def dashboard():
    mode      = APP_STATE["mode"]
    idle      = detection_agent(RESOURCES)
    shadow    = shadow_agent(RESOURCES)
    financial = financial_impact_agent(RESOURCES, mode)
    return jsonify({
        "mode":                   mode,
        "total_resources":        len(RESOURCES),
        "idle_count":             len(idle),
        "zombie_count":           len(shadow["zombies"]),
        "duplicate_count":        len(shadow["duplicates"]),
        "total_monthly_cost_inr": financial["total_monthly_cost_inr"],
        "potential_savings_inr":  financial["potential_monthly_savings_inr"],
        "savings_pct":            financial["savings_percentage"],
        "savings_unlocked_inr":   round(APP_STATE["total_savings_unlocked"]),
        "actions_taken_count":    APP_STATE["actions_taken_count"],
        "roi_message":            financial["roi_message"],
    })

@app.route("/tradeoff-mode", methods=["POST"])
def set_tradeoff_mode():
    body = request.get_json(silent=True) or {}
    mode = body.get("mode", "Balanced")
    if mode not in ["Max Savings", "Balanced", "Max Performance"]:
        return _err("mode must be one of: Max Savings, Balanced, Max Performance")
    APP_STATE["mode"] = mode
    idle = detection_agent(RESOURCES)
    _log("MODE_CHANGE", mode, f"Optimization mode changed to '{mode}'")
    return jsonify({
        "success":          True,
        "selected_mode":    mode,
        "decision_summary": decision_agent(idle, mode),
    })


# ---------------------------------------------------------------------------
# ACTION ROUTES
# ---------------------------------------------------------------------------

@app.route("/action/stop", methods=["POST"])
def action_stop():
    """Stop an idle resource immediately. Body: { resource_id }"""
    body        = request.get_json(silent=True) or {}
    resource_id = body.get("resource_id", "").strip()
    if not resource_id:
        return _err("resource_id is required")
    resource = _find_resource(resource_id)
    if not resource:
        return _err(f"Resource '{resource_id}' not found", 404)
    if resource_id in APP_STATE["decommissioned"]:
        return _err(f"'{resource['name']}' is already decommissioned")
    if resource_id in APP_STATE["stopped"]:
        return _ok(f"'{resource['name']}' was already stopped.", resource_id=resource_id, already_done=True)

    savings = _monthly_cost(resource)
    APP_STATE["stopped"].add(resource_id)
    APP_STATE["scheduled"].discard(resource_id)
    APP_STATE["monitored"].discard(resource_id)
    entry = _log("STOP", resource["name"],
        f"Stopped '{resource['name']}' ({resource['team']} / {resource['service']}) - saving Rs.{savings:,}/month",
        savings)
    return _ok(f"'{resource['name']}' stopped. Saving Rs.{savings:,}/month.",
        resource_id=resource_id, resource_name=resource["name"],
        team=resource["team"], service=resource["service"],
        monthly_savings_inr=savings, log_entry=entry)


@app.route("/action/schedule", methods=["POST"])
def action_schedule():
    """Schedule off-peak downtime. Body: { resource_id }"""
    body        = request.get_json(silent=True) or {}
    resource_id = body.get("resource_id", "").strip()
    if not resource_id:
        return _err("resource_id is required")
    resource = _find_resource(resource_id)
    if not resource:
        return _err(f"Resource '{resource_id}' not found", 404)
    if resource_id in APP_STATE["stopped"] or resource_id in APP_STATE["decommissioned"]:
        return _err(f"'{resource['name']}' is already stopped/decommissioned")
    if resource_id in APP_STATE["scheduled"]:
        return _ok(f"'{resource['name']}' already has downtime scheduled.", resource_id=resource_id, already_done=True)

    savings = round(_monthly_cost(resource) * 0.4)
    APP_STATE["scheduled"].add(resource_id)
    APP_STATE["monitored"].discard(resource_id)
    entry = _log("SCHEDULE_DOWNTIME", resource["name"],
        f"Scheduled off-peak downtime for '{resource['name']}' ({resource['team']}) - est. saving Rs.{savings:,}/month",
        savings)
    return _ok(f"Downtime scheduled for '{resource['name']}' (22:00-06:00 IST). Est. saving Rs.{savings:,}/month.",
        resource_id=resource_id, resource_name=resource["name"],
        team=resource["team"], schedule="22:00-06:00 IST daily",
        estimated_monthly_savings_inr=savings, log_entry=entry)


@app.route("/action/monitor", methods=["POST"])
def action_monitor():
    """Set a monitoring alert. Body: { resource_id }"""
    body        = request.get_json(silent=True) or {}
    resource_id = body.get("resource_id", "").strip()
    if not resource_id:
        return _err("resource_id is required")
    resource = _find_resource(resource_id)
    if not resource:
        return _err(f"Resource '{resource_id}' not found", 404)
    if resource_id in APP_STATE["monitored"]:
        return _ok(f"Monitor alert already active for '{resource['name']}'.", resource_id=resource_id, already_done=True)

    APP_STATE["monitored"].add(resource_id)
    entry = _log("MONITOR_ALERT", resource["name"],
        f"Monitor alert set for '{resource['name']}' ({resource['team']} / {resource['service']}) - triggers if CPU < 10% for 24h")
    return _ok(f"Monitor alert activated for '{resource['name']}'. Fires if CPU stays below 10% for 24 hours.",
        resource_id=resource_id, resource_name=resource["name"],
        team=resource["team"], alert_condition="CPU < 10% sustained for 24h", log_entry=entry)


@app.route("/action/decommission", methods=["POST"])
def action_decommission():
    """Permanently decommission a zombie. Body: { resource_id }"""
    body        = request.get_json(silent=True) or {}
    resource_id = body.get("resource_id", "").strip()
    if not resource_id:
        return _err("resource_id is required")
    resource = _find_resource(resource_id)
    if not resource:
        return _err(f"Resource '{resource_id}' not found", 404)
    if resource_id in APP_STATE["decommissioned"]:
        return _ok(f"'{resource['name']}' was already decommissioned.", resource_id=resource_id, already_done=True)

    savings   = _monthly_cost(resource)
    days_idle = round(resource["last_activity_hours"] / 24, 1)
    wasted    = round(resource["cost_per_hour"] * resource["last_activity_hours"])
    APP_STATE["decommissioned"].add(resource_id)
    APP_STATE["stopped"].discard(resource_id)
    APP_STATE["scheduled"].discard(resource_id)
    APP_STATE["monitored"].discard(resource_id)
    entry = _log("DECOMMISSION", resource["name"],
        f"Decommissioned zombie '{resource['name']}' ({resource['team']}) - idle {days_idle}d, wasted Rs.{wasted:,}, saving Rs.{savings:,}/month",
        savings)
    return _ok(f"'{resource['name']}' decommissioned. Was idle {days_idle} days (wasted Rs.{wasted:,}). Now saving Rs.{savings:,}/month.",
        resource_id=resource_id, resource_name=resource["name"],
        team=resource["team"], days_idle=days_idle,
        wasted_inr=wasted, monthly_savings_inr=savings, log_entry=entry)


@app.route("/action/consolidate", methods=["POST"])
def action_consolidate():
    """Consolidate two duplicate instances. Body: { instance_a, instance_b }"""
    body       = request.get_json(silent=True) or {}
    instance_a = body.get("instance_a", "").strip()
    instance_b = body.get("instance_b", "").strip()
    if not instance_a or not instance_b:
        return _err("Both instance_a and instance_b are required")
    if instance_a == instance_b:
        return _err("instance_a and instance_b must be different")

    res_a = next((r for r in RESOURCES if r["name"] == instance_a), None)
    res_b = next((r for r in RESOURCES if r["name"] == instance_b), None)
    if not res_a:
        return _err(f"Instance '{instance_a}' not found", 404)
    if not res_b:
        return _err(f"Instance '{instance_b}' not found", 404)

    pair_key = f"{instance_a}||{instance_b}"
    pair_rev = f"{instance_b}||{instance_a}"
    if pair_key in APP_STATE["consolidated"] or pair_rev in APP_STATE["consolidated"]:
        return _ok(f"'{instance_a}' and '{instance_b}' are already consolidated.", already_done=True)

    savings = _monthly_cost(res_b)
    APP_STATE["consolidated"].add(pair_key)
    APP_STATE["stopped"].add(res_b["id"])
    APP_STATE["decommissioned"].discard(res_b["id"])
    entry = _log("CONSOLIDATE", f"{instance_a} + {instance_b}",
        f"Consolidated '{res_b['workload']}' workload for {res_b['team']} team: kept '{instance_a}', stopped '{instance_b}' - saving Rs.{savings:,}/month",
        savings)
    return _ok(f"Consolidated! Kept '{instance_a}', stopped '{instance_b}'. Saving Rs.{savings:,}/month on duplicate '{res_b['workload']}' workload.",
        kept=instance_a, stopped=instance_b,
        team=res_b["team"], workload=res_b["workload"],
        monthly_savings_inr=savings, log_entry=entry)


@app.route("/action/acknowledge-spike", methods=["POST"])
def action_acknowledge_spike():
    """Acknowledge a cost spike. Body: { team }"""
    body = request.get_json(silent=True) or {}
    team = body.get("team", "").strip()
    if not team:
        return _err("team is required")
    if team not in WEEKLY_COST_HISTORY:
        return _err(f"Team '{team}' not found in cost history", 404)
    if team in APP_STATE["acked_spikes"]:
        return _ok(f"Spike for '{team}' was already acknowledged.", team=team, already_done=True)

    APP_STATE["acked_spikes"].add(team)
    weeks = WEEKLY_COST_HISTORY[team]
    delta = weeks[-1] - weeks[-2] if len(weeks) >= 2 else 0
    entry = _log("ACK_SPIKE", team,
        f"Cost spike acknowledged for {team} team (+Rs.{delta:,} this week) - under review")
    return _ok(f"Spike for '{team}' team acknowledged. The anomaly is marked for review.",
        team=team, week_increase_inr=delta, log_entry=entry)


@app.route("/action/schedule-offpeak", methods=["POST"])
def action_schedule_offpeak():
    """Schedule off-peak jobs for a team. Body: { team, service }"""
    body    = request.get_json(silent=True) or {}
    team    = body.get("team", "").strip()
    service = body.get("service", "").strip()
    if not team:
        return _err("team is required")
    if not service:
        return _err("service is required")
    if team not in WEEKLY_COST_HISTORY:
        return _err(f"Team '{team}' not found", 404)
    if team in APP_STATE["offpeak_scheduled"]:
        return _ok(f"Off-peak schedule for '{team} / {service}' is already active.",
            team=team, service=service, already_done=True)

    APP_STATE["offpeak_scheduled"].add(team)
    APP_STATE["acked_spikes"].add(team)
    weeks          = WEEKLY_COST_HISTORY[team]
    current_weekly = weeks[-1] if weeks else 0
    projected_save = round(current_weekly * 0.35)
    WEEKLY_COST_HISTORY[team].append(round(current_weekly * 0.65))
    entry = _log("SCHEDULE_OFFPEAK", f"{team} / {service}",
        f"Off-peak schedule applied for {team} / {service} (22:00-06:00 IST) - projected weekly saving Rs.{projected_save:,}",
        projected_save)
    return _ok(f"Off-peak schedule active for {team} / {service} (22:00-06:00 IST). Projected weekly saving: Rs.{projected_save:,}.",
        team=team, service=service, schedule_window="22:00-06:00 IST",
        projected_weekly_saving_inr=projected_save, log_entry=entry)


# ---------------------------------------------------------------------------
# UTILITY ROUTES
# ---------------------------------------------------------------------------

@app.route("/action-log", methods=["GET"])
def get_action_log():
    limit   = request.args.get("limit", 50, type=int)
    entries = list(reversed(ACTION_LOG))[:limit]
    return jsonify({
        "entries":             entries,
        "total":               len(ACTION_LOG),
        "actions_taken_count": APP_STATE["actions_taken_count"],
        "total_savings_inr":   round(APP_STATE["total_savings_unlocked"]),
    })

@app.route("/state", methods=["GET"])
def get_state():
    return jsonify({
        "mode":                       APP_STATE["mode"],
        "stopped":                    list(APP_STATE["stopped"]),
        "scheduled":                  list(APP_STATE["scheduled"]),
        "monitored":                  list(APP_STATE["monitored"]),
        "decommissioned":             list(APP_STATE["decommissioned"]),
        "consolidated":               list(APP_STATE["consolidated"]),
        "acked_spikes":               list(APP_STATE["acked_spikes"]),
        "offpeak_scheduled":          list(APP_STATE["offpeak_scheduled"]),
        "actions_taken_count":        APP_STATE["actions_taken_count"],
        "total_savings_unlocked_inr": round(APP_STATE["total_savings_unlocked"]),
    })

@app.route("/state/reset", methods=["POST"])
def reset_state():
    APP_STATE.update({
        "mode": "Balanced", "stopped": set(), "scheduled": set(),
        "monitored": set(), "decommissioned": set(), "consolidated": set(),
        "acked_spikes": set(), "offpeak_scheduled": set(),
        "total_savings_unlocked": 0.0, "actions_taken_count": 0,
    })
    ACTION_LOG.clear()
    WEEKLY_COST_HISTORY.update({k: list(v) for k, v in _HISTORY_DEFAULTS.items()})
    return _ok("State reset. All actions cleared - ready for a fresh demo.")

@app.route("/health", methods=["GET"])
def health():
    return jsonify({
        "status": "ok", "timestamp": _now_iso(),
        "mode": APP_STATE["mode"],
        "actions_taken": APP_STATE["actions_taken_count"],
        "savings_unlocked_inr": round(APP_STATE["total_savings_unlocked"]),
    })


if __name__ == "__main__":
    app.run(debug=True, port=5000)

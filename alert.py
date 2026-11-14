#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
币安带单员持仓变动 -> 微信提醒
适用对象：币安合约带单广场上的任意带单员，例如「allincrypto 熬鹰资本」

================================ 它做什么 ================================
每隔 N 秒拉一次目标带单员的「当前持仓」公开数据，跟上一轮的快照做 diff：
  · 出现新持仓        -> 推「开仓」提醒
  · 持仓消失 / 归零    -> 推「平仓」提醒（附已实现盈亏、持仓时长）
  · 同方向数量明显变化 -> 推「加减仓」提醒（默认关闭）
然后把消息推到你的微信（PushPlus / WxPusher / 企业微信机器人 / Server酱）。

============================== 它不做什么 ================================
· 不跟单、不下单、不读你的账户 —— 纯读公开数据，零 API Key，零资金风险。
· 不保证及时性：币安网页接口本身有秒级到十几秒延迟，高频短线请勿依赖。

================================ 快速开始 ================================
1) 装依赖
       pip install requests
2) 拿到带单员的 portfolioId
   浏览器打开他的带单主页，地址形如
       https://www.binance.com/zh-CN/copy-trading/lead-details/1234567890123456789
   末尾那串纯数字就是 portfolioId（上面是占位符，别照抄）。
3) 自检（强烈建议先跑，确认接口通、字段对得上）
       python alert.py --probe --pid 你的数字ID
   · 能打印出持仓列表 = 接口可用
   · 报错 451 / 000 / 超时 = 你的网络访问不了币安，换海外 VPS 或挂代理
4) 配置 config.json（复制 config.example.json 改名），至少填 portfolio_id 和一个推送通道
5) 首次运行先建基线，避免一上来把存量持仓全推一遍
       python alert.py --init
6) 常驻运行
       python alert.py
   只想跑一次（给 cron / GitHub Actions 用）
       python alert.py --once

=============================== 推送通道怎么选 ==============================
PushPlus（推荐，最省事）：https://www.pushplus.plus  微信扫码登录 -> 复制 token
    频率：免费版每日 200 条，够用；微信里直接收公众号消息。
WxPusher：https://wxpusher.zjiecode.com  建应用拿 appToken，用 UID 推送，免费。
企业微信群机器人：群设置 -> 群机器人 -> 添加 -> 复制 Webhook。需装企业微信 App，不在微信里。
Server酱³：https://sct.ftqq.com  免费额度很少。
四个通道可同时开，填几个发几个。

=============================== 常见坑 ================================
· 币安 bapi 是网页内部接口，币安随时可能改路径或加风控。脚本内置多个候选路径，
  全部失败时会打印原始响应，按提示改 ENDPOINTS 即可。
· 请求频率别太高，20 秒一次足够；过快会被限流（418/429）。
· 国内服务器直连 www.binance.com 通常不通，用海外 VPS 最省心。
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import traceback
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

try:
    import requests
except ImportError:
    sys.exit("缺少依赖，请先执行: pip install requests")

BASE_DIR = Path(__file__).resolve().parent
CONFIG_PATH = BASE_DIR / "config.json"
STATE_PATH = BASE_DIR / "state.json"
LOG_PATH = BASE_DIR / "alert.log"

TZ_CN = timezone(timedelta(hours=8))

# 币安网页端 bapi，多个候选路径依次尝试（币安改版时按 --probe 输出改这里）
BAPI_PUB = "https://www.binance.com/bapi/futures/v1/public/future/copy-trade"
BAPI = "https://www.binance.com/bapi/futures/v1/friendly/future/copy-trade"
ENDPOINTS: Dict[str, List[str]] = {
    "positions": [
        f"{BAPI}/lead-data/positions",
        f"{BAPI}/lead-data/position",
        f"{BAPI}/lead-portfolio/positions",
    ],
    "history": [
        f"{BAPI}/lead-data/position-history",
        f"{BAPI}/lead-data/position-histories",
        f"{BAPI}/lead-portfolio/position-history",
    ],
    "detail": [
        f"{BAPI}/lead-portfolio/detail",
        f"{BAPI}/lead-data/detail",
    ],
}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "zh-CN,zh;q=0.9",
    "clienttype": "web",
    "lang": "zh-CN",
    "origin": "https://www.binance.com",
    "referer": "https://www.binance.com/zh-CN/copy-trading",
    "Content-Type": "application/json",
}

DEFAULT_CONFIG: Dict[str, Any] = {
    "portfolio_id": "",
    "trader_name": "熬鹰",
    "poll_interval_sec": 20,
    "notify": {
        "pushplus_token": "",
        "wxpusher_app_token": "",
        "wxpusher_uids": [],
        "wecom_webhook": "",
        "serverchan_key": "",
    },
    "notify_on": {"open": True, "close": True, "adjust": False},
    "watch_symbols": [],
    "min_notional_usdt": 0,
    "adjust_ratio": 0.2,
    "timeout_sec": 15,
    "retries": 2,
    "proxy": "",
}

# --------------------------------------------------------------------------- #
# 基础设施
# --------------------------------------------------------------------------- #


def log(msg: str) -> None:
    line = f"[{datetime.now(TZ_CN):%Y-%m-%d %H:%M:%S}] {msg}"
    print(line, flush=True)
    try:
        with LOG_PATH.open("a", encoding="utf-8") as f:
            f.write(line + "\n")
    except OSError:
        pass


def deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(base)
    for k, v in (override or {}).items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = deep_merge(out[k], v)
        else:
            out[k] = v
    return out


def load_config(path: Path) -> Dict[str, Any]:
    cfg = dict(DEFAULT_CONFIG)
    if path.exists():
        try:
            cfg = deep_merge(cfg, json.loads(path.read_text(encoding="utf-8")))
        except json.JSONDecodeError as e:
            sys.exit(f"config.json 格式错误：{e}")
    # 环境变量优先（方便 GitHub Actions / 云函数用 secrets 注入）
    env_map = {
        "PORTFOLIO_ID": ("portfolio_id", str),
        "TRADER_NAME": ("trader_name", str),
        "PUSHPLUS_TOKEN": ("notify.pushplus_token", str),
        "WXPUSHER_APP_TOKEN": ("notify.wxpusher_app_token", str),
        "WECOM_WEBHOOK": ("notify.wecom_webhook", str),
        "SERVERCHAN_KEY": ("notify.serverchan_key", str),
        "PROXY": ("proxy", str),
        "POLL_INTERVAL_SEC": ("poll_interval_sec", int),
    }
    for env_key, (dotted, caster) in env_map.items():
        raw = os.getenv(env_key)
        if not raw:
            continue
        node = cfg
        parts = dotted.split(".")
        for p in parts[:-1]:
            node = node.setdefault(p, {})
        try:
            node[parts[-1]] = caster(raw)
        except ValueError:
            pass
    env_uids = os.getenv("WXPUSHER_UIDS")
    if env_uids:
        cfg["notify"]["wxpusher_uids"] = [x.strip() for x in env_uids.split(",") if x.strip()]
    return cfg


def load_state() -> Dict[str, Any]:
    if STATE_PATH.exists():
        try:
            return json.loads(STATE_PATH.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            log("state.json 损坏，按空状态处理")
    return {"positions": {}, "last_push": {}}


def save_state(state: Dict[str, Any]) -> None:
    tmp = STATE_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(STATE_PATH)


# --------------------------------------------------------------------------- #
# 数据抓取
# --------------------------------------------------------------------------- #


class FetchError(RuntimeError):
    pass


def _unwrap(payload: Any) -> Any:
    """剥掉币安 {code, message, data} 外壳，兼容 data 里再套 list/rows 的写法。"""
    if not isinstance(payload, dict):
        return payload
    code = payload.get("code")
    if code is not None and str(code) not in ("000000", "0", "200"):
        raise FetchError(f"接口返回异常 code={code} message={payload.get('message')}")
    data = payload.get("data", payload)
    if isinstance(data, dict):
        for key in ("list", "rows", "data", "positions", "items"):
            if isinstance(data.get(key), list):
                return data[key]
    return data


def _session(cfg: Dict[str, Any]) -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    proxy = (cfg.get("proxy") or "").strip()
    if proxy:
        s.proxies.update({"http": proxy, "https": proxy})
    return s


def _get_json(
    session: requests.Session, url: str, params: Dict[str, Any], cfg: Dict[str, Any]
) -> Any:
    last_err: Optional[Exception] = None
    for attempt in range(int(cfg.get("retries", 2)) + 1):
        try:
            r = session.get(url, params=params, timeout=float(cfg.get("timeout_sec", 15)))
            if r.status_code in (418, 429):
                raise FetchError("被限流（418/429），把 poll_interval_sec 调大")
            if r.status_code == 451:
                raise FetchError("451 地域限制：你的 IP 所在地区被币安屏蔽，换海外节点")
            r.raise_for_status()
            return r.json()
        except Exception as e:  # noqa: BLE001
            last_err = e
            if attempt < int(cfg.get("retries", 2)):
                time.sleep(1.5 * (attempt + 1))
    raise FetchError(f"请求失败 {url} -> {last_err}")


def fetch_with_fallback(
    kind: str, cfg: Dict[str, Any], extra_params: Optional[Dict[str, Any]] = None
) -> Tuple[List[Dict[str, Any]], str]:
    """依次尝试候选路径，返回 (列表, 命中的 url)。"""
    pid = str(cfg["portfolio_id"]).strip()
    if not pid:
        raise FetchError("config.json 里没填 portfolio_id")
    session = _session(cfg)
    params = {"portfolioId": pid}
    if extra_params:
        params.update(extra_params)

    errors: List[str] = []
    for url in ENDPOINTS[kind]:
        try:
            payload = _get_json(session, url, params, cfg)
            data = _unwrap(payload)
            if isinstance(data, dict):
                data = data.get("list") or []
            if not isinstance(data, list):
                raise FetchError(f"返回结构不是列表: {type(data).__name__}")
            return data, url
        except FetchError as e:
            errors.append(f"  · {url}\n    {e}")
        except Exception as e:  # noqa: BLE001
            errors.append(f"  · {url}\n    {type(e).__name__}: {e}")
    raise FetchError(
        f"{kind} 全部候选路径失败，币安可能改版了：\n" + "\n".join(errors)
        + "\n提示：跑 python alert.py --probe --pid <id> 看原始响应，再改脚本顶部 ENDPOINTS。"
    )


def _fnum(v: Any) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def normalize_position(raw: Dict[str, Any]) -> Dict[str, Any]:
    """把币安各种命名的字段归一化。"""
    symbol = str(raw.get("symbol") or raw.get("pair") or "").upper()
    side_raw = str(
        raw.get("positionSide") or raw.get("side") or raw.get("positionDirection") or "BOTH"
    ).upper()
    amount = _fnum(
        raw.get("positionAmount")
        or raw.get("positionAmt")
        or raw.get("amount")
        or raw.get("totalPositionAmount")
    )
    entry = _fnum(
        raw.get("entryPrice") or raw.get("avgEntryPrice") or raw.get("openPrice")
    )
    mark = _fnum(raw.get("markPrice") or raw.get("lastPrice"))
    upnl = _fnum(raw.get("unrealizedProfit") or raw.get("unrealizedPnl"))
    lev = raw.get("leverage") or raw.get("leverageLevel") or ""

    # BOTH 模式下用数量正负判断多空
    if side_raw in ("BOTH", "", "NONE"):
        side_raw = "SHORT" if amount < 0 else "LONG"
    amount = abs(amount)

    return {
        "symbol": symbol,
        "side": side_raw,
        "amount": amount,
        "entry_price": entry,
        "mark_price": mark,
        "unrealized": upnl,
        "leverage": lev,
        "isolated": bool(raw.get("isolated") or raw.get("marginType") == "ISOLATED"),
        "notional": amount * (entry or mark),
        "raw": raw,
    }


def fetch_positions(cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows, _ = fetch_with_fallback("positions", cfg)
    out: List[Dict[str, Any]] = []
    for r in rows:
        if not isinstance(r, dict):
            continue
        p = normalize_position(r)
        if not p["symbol"] or p["amount"] <= 0:
            continue
        if cfg.get("watch_symbols"):
            if p["symbol"] not in [s.upper() for s in cfg["watch_symbols"]]:
                continue
        if p["notional"] and p["notional"] < float(cfg.get("min_notional_usdt") or 0):
            continue
        out.append(p)
    return out


def fetch_history(cfg: Dict[str, Any], limit: int = 30) -> List[Dict[str, Any]]:
    try:
        rows, _ = fetch_with_fallback("history", cfg, {"pageNumber": 1, "pageSize": limit})
        return [r for r in rows if isinstance(r, dict)]
    except FetchError as e:
        log(f"拉取成交历史失败（不影响开仓提醒）：{e}")
        return []


def match_closed_trade(
    history: List[Dict[str, Any]], symbol: str, side: str
) -> Optional[Dict[str, Any]]:
    """在历史成交里找该币种该方向最近的一笔平仓，用来补上盈亏。"""
    best: Optional[Dict[str, Any]] = None
    best_ts = -1.0
    for h in history:
        if str(h.get("symbol") or h.get("pair") or "").upper() != symbol:
            continue
        h_side = str(h.get("positionSide") or h.get("side") or "").upper()
        if h_side and h_side not in ("BOTH",) and h_side != side:
            continue
        ts = _fnum(h.get("closeTime") or h.get("updateTime") or h.get("time"))
        if ts > best_ts:
            best_ts, best = ts, h
    return best


# --------------------------------------------------------------------------- #
# 消息格式化
# --------------------------------------------------------------------------- #


def side_cn(side: str) -> str:
    return {"LONG": "做多", "SHORT": "做空"}.get(side, side)


def side_mark(side: str) -> str:
    # 中国习惯：红涨绿跌 —— 多单配红、空单配绿
    return "🔴" if side == "LONG" else "🟢"


def fmt_num(v: float) -> str:
    if v == 0:
        return "0"
    if abs(v) >= 1000:
        return f"{v:,.2f}"
    if abs(v) >= 1:
        return f"{v:,.4f}".rstrip("0").rstrip(".")
    return f"{v:.8f}".rstrip("0").rstrip(".")


def fmt_price(v: float) -> str:
    """价格单独格式化：同一条消息里的价格小数位要对齐，别出现 874.63 和 870.1 并排。"""
    if v == 0:
        return "0"
    if abs(v) >= 1:
        return f"{v:,.2f}"
    if abs(v) >= 0.01:
        return f"{v:,.4f}"
    return f"{v:.8f}".rstrip("0").rstrip(".")


def fmt_pnl(v: float) -> str:
    sign = "+" if v > 0 else ""
    return f"{sign}{v:,.2f} U"


def msg_open(p: Dict[str, Any], trader: str) -> Tuple[str, str]:
    title = f"【开仓】{trader} {side_cn(p['side'])} {p['symbol']}"
    body = (
        f"{side_mark(p['side'])} {trader} 开仓\n"
        f"币种：{p['symbol']}\n"
        f"方向：{side_cn(p['side'])}　杠杆：{p['leverage']}x　"
        f"{'逐仓' if p['isolated'] else '全仓'}\n"
        f"开仓价：{fmt_price(p['entry_price'])}\n"
        f"标记价：{fmt_price(p['mark_price']) or '—'}\n"
        f"数量：{fmt_num(p['amount'])}\n"
        f"名义价值：约 {fmt_num(p['notional'])} U\n"
        f"浮动盈亏：{fmt_pnl(p['unrealized'])}\n"
        f"时间：{datetime.now(TZ_CN):%m-%d %H:%M:%S}"
    )
    return title, body


def msg_close(
    p: Dict[str, Any], trader: str, hist: Optional[Dict[str, Any]]
) -> Tuple[str, str]:
    lines = [
        f"⚪ {trader} 平仓",
        f"币种：{p['symbol']}",
        f"方向：{side_cn(p['side'])}　杠杆：{p['leverage']}x",
        f"开仓价：{fmt_price(p['entry_price'])}",
    ]
    title = f"【平仓】{trader} {p['symbol']}"
    if hist:
        close_px = _fnum(hist.get("closePrice") or hist.get("avgClosePrice"))
        pnl = _fnum(hist.get("realizedProfit") or hist.get("realizedPnl") or hist.get("pnl"))
        if close_px:
            lines.append(f"平仓价：{fmt_price(close_px)}")
            if p["entry_price"]:
                chg = (close_px - p["entry_price"]) / p["entry_price"] * 100
                lines.append(f"价格变动：{chg:+.2f}%")
        lines.append(f"已实现盈亏：{fmt_pnl(pnl)}")
        title = f"【平仓 {fmt_pnl(pnl)}】{trader} {p['symbol']}"
        ot, ct = _fnum(hist.get("openTime")), _fnum(hist.get("closeTime"))
        if ot and ct and ct > ot:
            mins = int((ct - ot) / 1000 / 60)
            lines.append(f"持仓时长：{mins // 60}h{mins % 60:02d}m")
    else:
        lines.append("（未取到成交明细，盈亏未知）")
    lines.append(f"时间：{datetime.now(TZ_CN):%m-%d %H:%M:%S}")
    return title, "\n".join(lines)


# --------------------------------------------------------------------------- #
# 推送
# --------------------------------------------------------------------------- #


def push_wechat(cfg: Dict[str, Any], title: str, content: str) -> None:
    n = cfg.get("notify") or {}
    proxy = (cfg.get("proxy") or "").strip()
    proxies = {"http": proxy, "https": proxy} if proxy else None
    sent = False

    token = (n.get("pushplus_token") or "").strip()
    if token:
        try:
            requests.post(
                "https://www.pushplus.plus/send",
                json={
                    "token": token,
                    "title": title,
                    "content": content.replace("\n", "<br/>"),
                    "template": "html",
                },
                timeout=15,
                proxies=proxies,
            ).raise_for_status()
            sent = True
        except Exception as e:  # noqa: BLE001
            log(f"PushPlus 推送失败：{e}")

    app_token = (n.get("wxpusher_app_token") or "").strip()
    uids = n.get("wxpusher_uids") or []
    if app_token and uids:
        try:
            requests.post(
                "https://wxpusher.zjiecode.com/api/send/message",
                json={
                    "appToken": app_token,
                    "content": f"<h3>{title}</h3><pre>{content}</pre>",
                    "summary": title,
                    "contentType": 2,
                    "uids": uids,
                },
                timeout=15,
                proxies=proxies,
            ).raise_for_status()
            sent = True
        except Exception as e:  # noqa: BLE001
            log(f"WxPusher 推送失败：{e}")

    hook = (n.get("wecom_webhook") or "").strip()
    if hook:
        try:
            requests.post(
                hook,
                json={"msgtype": "text", "text": {"content": f"{title}\n{content}"}},
                timeout=15,
                proxies=proxies,
            ).raise_for_status()
            sent = True
        except Exception as e:  # noqa: BLE001
            log(f"企业微信推送失败：{e}")

    key = (n.get("serverchan_key") or "").strip()
    if key:
        try:
            requests.post(
                f"https://sctapi.ftqq.com/{key}.send",
                data={"title": title, "desp": content},
                timeout=15,
                proxies=proxies,
            ).raise_for_status()
            sent = True
        except Exception as e:  # noqa: BLE001
            log(f"Server酱推送失败：{e}")

    if not sent:
        log("⚠ 没有配置任何推送通道，只在控制台输出：\n" + title + "\n" + content)


# --------------------------------------------------------------------------- #
# 主逻辑
# --------------------------------------------------------------------------- #


def key_of(p: Dict[str, Any]) -> str:
    return f"{p['symbol']}|{p['side']}"


def tick(cfg: Dict[str, Any], state: Dict[str, Any], init_only: bool = False) -> int:
    positions = fetch_positions(cfg)
    cur: Dict[str, Dict[str, Any]] = {}
    for p in positions:
        cur[key_of(p)] = {
            "symbol": p["symbol"],
            "side": p["side"],
            "amount": p["amount"],
            "entry_price": p["entry_price"],
            "leverage": p["leverage"],
            "first_seen": int(time.time() * 1000),
        }

    prev: Dict[str, Any] = state.get("positions") or {}
    on = cfg.get("notify_on") or {}
    trader = cfg.get("trader_name") or "带单员"
    events = 0

    # 防误判：接口偶发返回空列表（风控/维护/网络）时，不能立刻把所有仓判成平仓。
    # 连续 2 轮都为空，才认定为真的清仓。
    if prev and not cur:
        streak = int(state.get("empty_streak") or 0) + 1
        state["empty_streak"] = streak
        if streak < 2:
            save_state(state)
            log(f"⚠ 本轮返回 0 个持仓（可能接口异常），第 {streak} 次，暂不判定平仓")
            return 0
        log("连续 2 轮返回 0 持仓，按已清仓处理")
    else:
        state["empty_streak"] = 0

    if init_only:
        state["positions"] = cur
        state["last_push"] = {}
        state["initialized"] = True
        save_state(state)
        log(f"基线已建立：当前 {len(cur)} 个持仓，不推送")
        for k in cur:
            log(f"  · {k} 数量 {fmt_num(cur[k]['amount'])}")
        return 0

    opened = [k for k in cur if k not in prev]
    closed = [k for k in prev if k not in cur]

    if opened and on.get("open", True):
        for k in opened:
            p = next(x for x in positions if key_of(x) == k)
            title, body = msg_open(p, trader)
            log(f"新开仓 {k}")
            push_wechat(cfg, title, body)
            events += 1

    if closed and on.get("close", True):
        history = fetch_history(cfg) if closed else []
        for k in closed:
            old = prev[k]
            pseudo = {
                "symbol": old.get("symbol", k.split("|")[0]),
                "side": old.get("side", k.split("|")[-1]),
                "entry_price": _fnum(old.get("entry_price")),
                "leverage": old.get("leverage", ""),
            }
            hist = match_closed_trade(history, pseudo["symbol"], pseudo["side"])
            title, body = msg_close(pseudo, trader, hist)
            log(f"平仓 {k}")
            push_wechat(cfg, title, body)
            events += 1

    if on.get("adjust", False):
        ratio = float(cfg.get("adjust_ratio") or 0.2)
        for k in cur:
            if k not in prev:
                continue
            old_amt = _fnum(prev[k].get("amount"))
            new_amt = cur[k]["amount"]
            if old_amt <= 0:
                continue
            delta = (new_amt - old_amt) / old_amt
            if abs(delta) >= ratio:
                act = "加仓" if delta > 0 else "减仓"
                title = f"【{act}】{trader} {cur[k]['symbol']}"
                body = (
                    f"{trader} {act}\n"
                    f"币种：{cur[k]['symbol']}\n"
                    f"数量：{fmt_num(old_amt)} → {fmt_num(new_amt)}"
                    f"（{delta * 100:+.1f}%）\n"
                    f"时间：{datetime.now(TZ_CN):%m-%d %H:%M:%S}"
                )
                log(f"{act} {k} {delta:+.1%}")
                push_wechat(cfg, title, body)
                events += 1

    # 保留 early 记录，避免同一个仓被反复当新仓
    for k, v in cur.items():
        if k in prev:
            v["first_seen"] = prev[k].get("first_seen", v["first_seen"])
    state["positions"] = cur
    save_state(state)

    if not events:
        log(f"无变化（当前 {len(cur)} 仓）")
    return events


def _post_json(
    session: requests.Session, url: str, body: Dict[str, Any], cfg: Dict[str, Any]
) -> Any:
    last_err: Optional[Exception] = None
    for attempt in range(int(cfg.get("retries", 2)) + 1):
        try:
            r = session.post(url, json=body, timeout=float(cfg.get("timeout_sec", 15)))
            if r.status_code in (418, 429):
                raise FetchError("被限流（418/429）")
            if r.status_code == 451:
                raise FetchError("451 地域限制")
            r.raise_for_status()
            return r.json()
        except Exception as e:  # noqa: BLE001
            last_err = e
            if attempt < int(cfg.get("retries", 2)):
                time.sleep(1.5 * (attempt + 1))
    raise FetchError(f"请求失败 {url} -> {last_err}")


# 探测用的候选路径（2025-11 实测：public 前缀下 trade-history 只吃 POST）
PROBE_PATHS = [
    ("positions", "GET",  f"{BAPI_PUB}/lead-portfolio/position"),
    ("positions", "POST", f"{BAPI_PUB}/lead-portfolio/position"),
    ("positions", "GET",  f"{BAPI_PUB}/lead-data/positions"),
    ("history",   "POST", f"{BAPI_PUB}/lead-portfolio/trade-history"),
    ("detail",    "GET",  f"{BAPI_PUB}/lead-portfolio/detail"),
]


def _peek_list(data: Any) -> Optional[List[Any]]:
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for k in ("list", "rows", "positionList", "positions", "data", "items"):
            v = data.get(k)
            if isinstance(v, list):
                return v
    return None


def _try_one(session, method, url, pid, cfg) -> Tuple[Optional[List[Any]], Any, str]:
    body = {"portfolioId": pid, "pageNumber": 1, "pageSize": 10}
    if method == "GET":
        payload = _get_json(session, url, body, cfg)
    else:
        payload = _post_json(session, url, body, cfg)
    data = payload.get("data") if isinstance(payload, dict) else payload
    return _peek_list(data), payload, ""


def cmd_probe(cfg: Dict[str, Any]) -> None:
    """三步定性：能不能连 bapi / 能不能看到别人数据 / 持仓走哪条路。"""
    pid = str(cfg.get("portfolio_id") or "").strip()
    session = _session(cfg)
    print(f"config 里的 portfolioId = {pid or '(空)'}\n")

    # 1) bapi 通不通
    print("===== 1) bapi 连通性 =====")
    try:
        payload = _get_json(session, "https://www.binance.com/bapi/composite/v1/public/ping", {}, cfg)
        print("ping ->", json.dumps(payload, ensure_ascii=False)[:160])
    except Exception as e:  # noqa: BLE001
        print("ping 失败:", str(e)[:160])

    # 2) 公开榜单：能拿到别人数据 = 没被拦；顺便按昵称找熬鹰，验证 ID
    print("\n===== 2) 带单广场榜单（不用登录、不用 ID）=====")
    rank_url = f"{BAPI}/home-page/query-list"
    base = {
        "pageNumber": 1, "pageSize": 5, "timeRange": "90D", "dataType": "ROI",
        "favoriteOnly": False, "hideFull": False, "nickname": "",
        "order": "DESC", "apiKeyOnly": False,
    }
    found: List[Dict[str, Any]] = []
    try:
        payload = _post_json(session, rank_url, dict(base), cfg)
        lst = _peek_list(payload.get("data") if isinstance(payload, dict) else payload) or []
        print(f"返回条数 = {len(lst)}")
        for it in lst[:5]:
            print(f"  · {it.get('nickname')}  id={it.get('leadPortfolioId')}  "
                  f"跟单 {it.get('currentCopyCount')}/{it.get('maxCopyCount')}")
    except Exception as e:  # noqa: BLE001
        print("榜单请求失败:", str(e)[:200])

    print("\n===== 3) 按昵称搜「熬鹰」=====")
    try:
        body = dict(base)
        body["nickname"] = "熬鹰"
        body["pageSize"] = 10
        payload = _post_json(session, rank_url, body, cfg)
        lst = _peek_list(payload.get("data") if isinstance(payload, dict) else payload) or []
        print(f"命中 {len(lst)} 条")
        for it in lst[:10]:
            found.append(it)
            print(f"  · {it.get('nickname')}  id={it.get('leadPortfolioId')}  "
                  f"跟单 {it.get('currentCopyCount')}/{it.get('maxCopyCount')}")
    except Exception as e:  # noqa: BLE001
        print("搜索失败:", str(e)[:200])

    # 4) 用「搜到的 ID」和「你给的 ID」各试一遍持仓接口
    ids: List[str] = []
    for it in found:
        v = str(it.get("leadPortfolioId") or "")
        if v and v not in ids:
            ids.append(v)
    if pid and pid not in ids:
        ids.append(pid)

    print("\n===== 4) 持仓 / 历史接口 =====")
    best: Dict[str, str] = {}
    for test_id in ids:
        print(f"\n--- portfolioId = {test_id} ---")
        for kind, method, url in PROBE_PATHS:
            short = url.split("/copy-trade")[-1]
            try:
                lst, payload, _ = _try_one(session, method, url, test_id, cfg)
                n = len(lst) if lst is not None else -1
                code = payload.get("code") if isinstance(payload, dict) else None
                mark = "OK " if n > 0 else "-- "
                print(f"  {mark}[{method}] {short}  code={code} n={n}")
                if n > 0 and kind not in best:
                    best[kind] = f"{method} {url}"
                    print("      " + json.dumps(lst[0], ensure_ascii=False)[:200])
            except Exception as e:  # noqa: BLE001
                print(f"  --  [{method}] {short}  {str(e).splitlines()[0][:110]}")

    print("\n===== 结论 =====")
    if best:
        for k, v in best.items():
            print(f"{k}: {v}")
        print("\n把「结论」和上面命中的昵称发我，我改脚本。")
    else:
        print("拿不到任何持仓数据。把这一整屏发我——")
        print("如果第 2 步榜单也是 0 条，说明这台机器被币安软拦了，得换地区。")



def main() -> None:
    ap = argparse.ArgumentParser(description="币安带单员持仓变动 -> 微信提醒")
    ap.add_argument("--config", default=str(CONFIG_PATH))
    ap.add_argument("--pid", help="带单员 portfolioId，覆盖 config")
    ap.add_argument("--probe", action="store_true", help="只做接口连通性自检，不推送")
    ap.add_argument("--init", action="store_true", help="建立基线快照，不推送")
    ap.add_argument("--once", action="store_true", help="只跑一轮（cron / Actions 用）")
    args = ap.parse_args()

    cfg = load_config(Path(args.config))
    if args.pid:
        cfg["portfolio_id"] = args.pid

    if args.probe:
        cmd_probe(cfg)
        return

    state = load_state()

    if args.init:
        tick(cfg, state, init_only=True)
        return

    # 首次运行（没有 state.json，或 state 丢了）自动只建基线，不推送，
    # 避免一上来把存量持仓全当"新开仓"刷屏。GitHub Actions 场景尤其重要。
    if not state.get("initialized"):
        log("首次运行：自动建立基线，本轮不推送")
        try:
            tick(cfg, state, init_only=True)
        except FetchError as e:
            log(f"抓取失败：{e}")
            sys.exit(1)
        if args.once:
            return

    if args.once:
        try:
            tick(cfg, state)
        except FetchError as e:
            log(f"抓取失败：{e}")
            sys.exit(1)
        return

    interval = max(5, int(cfg.get("poll_interval_sec") or 20))
    log(f"启动常驻监控：{cfg.get('trader_name')} portfolioId={cfg['portfolio_id']} 间隔={interval}s")
    while True:
        try:
            tick(cfg, state)
        except FetchError as e:
            log(f"抓取失败：{e}")
        except KeyboardInterrupt:
            log("手动退出")
            return
        except Exception:  # noqa: BLE001
            log("未预期错误：\n" + traceback.format_exc())
        time.sleep(interval)


if __name__ == "__main__":
    main()

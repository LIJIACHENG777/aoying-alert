#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
币安带单员「成交动作」-> 微信提醒
目标带单员：币安合约带单广场 allincrypto 熬鹰资本（5075281354358777856）

================================ 为什么盯「成交记录」而不是「持仓」 ================================
熬鹰把带单主页的「公开持仓」关掉了（positionsShared = false），
任何第三方都读不到他的当前仓位，接口一律返回空列表 —— 这不是代码问题，是他人为设置的。

但「成交记录」是公开的：他每开一笔、平一笔，都会留下一条带时间戳的成交。
所以本程序盯成交：出现新成交 -> 立刻推微信。
对「他一动手我就知道」这个需求，成交记录其实比持仓更准（持仓有延迟、还会合并）。

================================ 它做什么 ================================
每轮拉一次最新成交，和上一轮做差：
  · 新成交且方向 = 持仓方向（做多买入 / 做空卖出）-> 推「开仓」
  · 新成交且方向 = 平仓方向（做多卖出 / 做空买入）-> 推「平仓」，并附已实现盈亏与持仓时长
同一轮里同币种同方向的多笔会合并成一条（他分批建仓时不会刷你五条）。
然后把消息推到微信（PushPlus / WxPusher / 企业微信机器人 / Server酱）。

================================ 它不做什么 ================================
· 不跟单、不下单、不读你的账户，不碰任何交易权限。
· 不保证秒级：数据源本身有数分钟延迟。

================================ 数据来源 ================================
Kopyon 公共行情 API（https://kopyon.com），它读取币安公开页面。
公开数据，不需要注册、不需要 key、不要钱。

================================ 快速开始 ================================
    pip install requests
    python alert.py --probe     # 自检：确认能读到、编号对不对
    python alert.py --once      # 跑一轮（首轮只建基线，不推送）
    python alert.py             # 常驻，默认 60 秒一轮
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

try:
    import requests
except ImportError:  # pragma: no cover
    sys.exit("缺少依赖：pip install requests")

BASE_DIR = Path(__file__).resolve().parent
STATE_PATH = BASE_DIR / "state.json"
LOG_PATH = BASE_DIR / "alert.log"

KOPYON_API = "https://api.kopyon.com/v5/binance-copy-trading"
TZ = timezone(timedelta(hours=8))

DEFAULT_CONFIG: Dict[str, Any] = {
    "portfolio_id": "",
    "trader_name": "熬鹰",
    "kopyon_key": "",
    "poll_interval_sec": 60,
    "timeout_sec": 20,
    "retries": 1,
    # 通知渠道：填哪个用哪个，优先 PushPlus
    "notify": {
        "pushplus_token": "",
        "wxpusher_app_token": "",
        "wxpusher_uids": [],
        "wecom_webhook": "",
        "serverchan_key": "",
    },
    # 只提醒哪类动作
    "notify_on": {"open": True, "close": True},
    # 只盯这些币种（留空 = 全部）；例：["BTCUSDT", "ETHUSDT"]
    "watch_symbols": [],
    # 名义价值低于这个数就不提醒（0 = 不过滤），单位 U
    "min_notional_usdt": 0,
    # 单轮里同币种同方向的成交合并成一条
    "merge_same_side": True,
}


# --------------------------------------------------------------------------- #
# 基础工具
# --------------------------------------------------------------------------- #


def log(msg: str) -> None:
    line = f"[{datetime.now(TZ).strftime('%Y-%m-%d %H:%M:%S')}] {msg}"
    print(line)
    try:
        with LOG_PATH.open("a", encoding="utf-8") as f:
            f.write(line + "\n")
    except OSError:
        pass


def fmt_price(v: float) -> str:
    """价格：同一条消息里小数位要对齐，避免 874.63 旁边出现 870.1。"""
    if not v:
        return "—"
    if abs(v) >= 1:
        return f"{v:,.2f}"
    if abs(v) >= 0.01:
        return f"{v:,.4f}"
    return f"{v:.8f}".rstrip("0").rstrip(".")


def fmt_qty(v: float) -> str:
    if v == int(v):
        return f"{int(v):,}"
    return f"{v:,.4f}".rstrip("0").rstrip(".")


def fmt_pnl(v: float) -> str:
    return f"{v:+,.2f} U"


def fmt_ts(ms: Any) -> str:
    try:
        return datetime.fromtimestamp(int(ms) / 1000, TZ).strftime("%m-%d %H:%M:%S")
    except (TypeError, ValueError, OSError):
        return "—"


def fmt_duration(start_ms: Any, end_ms: Any) -> str:
    try:
        sec = (int(end_ms) - int(start_ms)) / 1000
    except (TypeError, ValueError):
        return "—"
    if sec <= 0:
        return "—"
    h, m = int(sec // 3600), int(sec % 3600 // 60)
    return f"{h}h{m:02d}m" if h else f"{m}m"


def load_config(path: Path) -> Dict[str, Any]:
    cfg: Dict[str, Any] = json.loads(json.dumps(DEFAULT_CONFIG))
    if path.exists():
        try:
            user = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(user, dict):
                for k, v in user.items():
                    if isinstance(v, dict) and isinstance(cfg.get(k), dict):
                        cfg[k].update(v)
                    else:
                        cfg[k] = v
        except (json.JSONDecodeError, OSError) as e:
            log(f"配置文件读不了，改用默认值：{e}")

    # 环境变量优先（GitHub Secrets / export 都走这里，避免把 key 写进公开仓库）
    env_map = {
        "PORTFOLIO_ID": ("portfolio_id",),
        "TRADER_NAME": ("trader_name",),
        "KOPYON_KEY": ("kopyon_key",),
        "PUSHPLUS_TOKEN": ("notify", "pushplus_token"),
        "WXPUSHER_APP_TOKEN": ("notify", "wxpusher_app_token"),
        "WECOM_WEBHOOK": ("notify", "wecom_webhook"),
        "SERVERCHAN_KEY": ("notify", "serverchan_key"),
    }
    for env_name, path_keys in env_map.items():
        val = os.environ.get(env_name, "").strip()
        if not val:
            continue
        if len(path_keys) == 1:
            cfg[path_keys[0]] = val
        else:
            cfg[path_keys[0]][path_keys[1]] = val

    uids = os.environ.get("WXPUSHER_UIDS", "").strip()
    if uids:
        cfg["notify"]["wxpusher_uids"] = [u for u in uids.replace(",", " ").split() if u]
    return cfg


def load_state() -> Dict[str, Any]:
    if STATE_PATH.exists():
        try:
            data = json.loads(STATE_PATH.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                return data
        except (json.JSONDecodeError, OSError):
            pass
    return {"initialized": False, "seen_orders": [], "last_push": {}}


def save_state(state: Dict[str, Any]) -> None:
    state["updated_at"] = datetime.now(TZ).isoformat(timespec="seconds")
    tmp = STATE_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=False, indent=1), encoding="utf-8")
    tmp.replace(STATE_PATH)


# --------------------------------------------------------------------------- #
# 数据抓取
# --------------------------------------------------------------------------- #


class FetchError(RuntimeError):
    pass


def _session(cfg: Dict[str, Any]) -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
            ),
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "zh-CN,zh;q=0.9",
        }
    )
    return s


def _kopyon_get(cfg: Dict[str, Any], route: str, params: Dict[str, Any]) -> Dict[str, Any]:
    # key 可选：不填也能匿名读（公开数据），填了就带上，个别时段更稳
    key = str(cfg.get("kopyon_key") or "").strip()
    url = f"{KOPYON_API}/{route}"
    headers = {"X-API-KEY": key} if key else {}
    last: Optional[Exception] = None
    for attempt in range(int(cfg.get("retries", 1)) + 1):
        try:
            r = _session(cfg).get(
                url,
                params=params,
                headers=headers,
                timeout=float(cfg.get("timeout_sec", 20)),
            )
            if r.status_code in (401, 403):
                raise FetchError(f"被拒绝（HTTP {r.status_code}），若填了 KOPYON_KEY 请检查是否有效")
            if r.status_code == 429:
                raise FetchError("请求太频繁（HTTP 429），稍后会重试")
            r.raise_for_status()
            payload = r.json()
            if not isinstance(payload, dict) or "data" not in payload:
                raise FetchError(f"返回格式不对：{str(payload)[:120]}")
            return payload["data"] or {}
        except Exception as e:  # noqa: BLE001
            last = e
            if attempt < int(cfg.get("retries", 1)):
                time.sleep(2)
    raise FetchError(f"{route} 请求失败：{last}")


def fetch_orders(cfg: Dict[str, Any], limit: int = 50) -> List[Dict[str, Any]]:
    data = _kopyon_get(cfg, "orderHistory", {"traderId": str(cfg["portfolio_id"]), "limit": limit})
    orders = data.get("orders")
    if not isinstance(orders, list):
        raise FetchError(f"orderHistory 里没有 orders：{str(data)[:120]}")
    return orders


def fetch_closed(cfg: Dict[str, Any], limit: int = 20) -> List[Dict[str, Any]]:
    data = _kopyon_get(cfg, "positionHistory", {"traderId": str(cfg["portfolio_id"]), "limit": limit})
    positions = data.get("positions")
    return positions if isinstance(positions, list) else []


def fetch_profile(cfg: Dict[str, Any]) -> Dict[str, Any]:
    return _kopyon_get(cfg, "traderProfile", {"traderId": str(cfg["portfolio_id"])})


# --------------------------------------------------------------------------- #
# 消息
# --------------------------------------------------------------------------- #


def order_key(o: Dict[str, Any]) -> str:
    return "|".join(
        str(o.get(k)) for k in ("createdAt", "symbol", "side", "positionSide", "price", "quantity")
    )


def is_open_side(order: Dict[str, Any]) -> bool:
    """买入开多、卖出开空；反向即平仓。"""
    side = str(order.get("side") or "").upper()
    pside = str(order.get("positionSide") or "").upper()
    return (side == "BUY" and pside == "LONG") or (side == "SELL" and pside == "SHORT")


def build_message(
    group: List[Dict[str, Any]],
    trader: str,
    closed_map: Optional[Dict[str, List[Dict[str, Any]]]] = None,
) -> Tuple[str, str]:
    first = group[0]
    symbol = str(first.get("symbol") or "?")
    pside = str(first.get("positionSide") or "").upper()
    side = str(first.get("side") or "").upper()
    opening = is_open_side(first)

    qty = sum(float(o.get("quantity") or 0) for o in group)
    notional = sum(float(o.get("price") or 0) * float(o.get("quantity") or 0) for o in group)
    avg = notional / qty if qty else 0.0
    times = [int(o.get("createdAt") or 0) for o in group if o.get("createdAt")]
    t_first, t_last = (min(times), max(times)) if times else (0, 0)

    dir_cn = "做多" if pside == "LONG" else "做空" if pside == "SHORT" else "—"
    side_cn = "买入" if side == "BUY" else "卖出" if side == "SELL" else side

    lines: List[str] = []
    if opening:
        title = f"【开仓】{trader} {dir_cn} {symbol}"
        lines += [
            f"🟢 {trader} 开仓",
            f"币种：{symbol}",
            f"方向：{dir_cn}",
            f"成交：{len(group)} 笔　合计 {fmt_qty(qty)}",
            f"均价：{fmt_price(avg)}",
            f"名义价值：约 {notional:,.0f} U",
            f"时间：{fmt_ts(t_first)}",
        ]
    else:
        title = f"【平仓】{trader} {symbol}"
        lines += [
            f"⚪ {trader} 平仓",
            f"币种：{symbol}",
            f"方向：{dir_cn}（{side_cn}平仓）",
            f"成交：{len(group)} 笔　合计 {fmt_qty(qty)}",
            f"平仓均价：{fmt_price(avg)}",
            f"时间：{fmt_ts(t_first)}",
        ]
        # 用已平仓记录补盈亏和持仓时长（可能晚几分钟才出现，没有就跳过）
        for pos in (closed_map or {}).get(symbol, []):
            try:
                close_px = float(pos.get("closePrice") or 0)
                if avg and abs(close_px - avg) / max(avg, 1e-9) < 0.02:
                    pnl = float(pos.get("grossPnl") or 0)
                    lines += [
                        f"开仓价：{fmt_price(float(pos.get('entryPrice') or 0))}",
                        f"已实现盈亏：{fmt_pnl(pnl)}",
                        f"收益率：{float(pos.get('roi') or 0) * 100:+.1f}%",
                        f"持仓时长：{fmt_duration(pos.get('openedAt'), pos.get('closedAt'))}",
                    ]
                    title = f"【平仓 {pnl:+,.0f} U】{trader} {symbol}"
                    break
            except (TypeError, ValueError):
                continue

    if t_last > t_first:
        lines.append(f"最后一笔：{fmt_ts(t_last)}")
    return title, "\n".join(lines)


# --------------------------------------------------------------------------- #
# 推送
# --------------------------------------------------------------------------- #


def push_wechat(cfg: Dict[str, Any], title: str, body: str) -> bool:
    n = cfg.get("notify") or {}
    timeout = float(cfg.get("timeout_sec", 20))
    sent = False

    token = str(n.get("pushplus_token") or "").strip()
    if token:
        try:
            r = requests.post(
                "https://www.pushplus.plus/send",
                json={"token": token, "title": title, "content": body, "template": "txt"},
                timeout=timeout,
            )
            d = r.json()
            if d.get("code") == 200:
                sent = True
            else:
                log(f"PushPlus 失败：code={d.get('code')} msg={d.get('msg')}（905=没实名）")
        except Exception as e:  # noqa: BLE001
            log(f"PushPlus 异常：{e}")

    app_token = str(n.get("wxpusher_app_token") or "").strip()
    uids = [u for u in (n.get("wxpusher_uids") or []) if u]
    if app_token and uids:
        try:
            r = requests.post(
                "https://wxpusher.zjiecode.com/api/send/message",
                json={
                    "appToken": app_token,
                    "content": f"<h3>{title}</h3><pre>{body}</pre>",
                    "summary": title,
                    "contentType": 2,
                    "uids": uids,
                },
                timeout=timeout,
            )
            if r.json().get("code") == 1000:
                sent = True
            else:
                log(f"WxPusher 失败：{r.text[:150]}")
        except Exception as e:  # noqa: BLE001
            log(f"WxPusher 异常：{e}")

    hook = str(n.get("wecom_webhook") or "").strip()
    if hook:
        try:
            r = requests.post(
                hook,
                json={"msgtype": "text", "text": {"content": f"{title}\n{body}"}},
                timeout=timeout,
            )
            if r.json().get("errcode") == 0:
                sent = True
            else:
                log(f"企业微信失败：{r.text[:150]}")
        except Exception as e:  # noqa: BLE001
            log(f"企业微信异常：{e}")

    sc = str(n.get("serverchan_key") or "").strip()
    if sc:
        try:
            r = requests.post(
                f"https://sctapi.ftqq.com/{sc}.send",
                data={"title": title, "desp": body},
                timeout=timeout,
            )
            if r.json().get("code") == 0:
                sent = True
        except Exception as e:  # noqa: BLE001
            log(f"Server酱异常：{e}")

    if not sent:
        log("没有任何渠道发送成功，检查 notify 配置")
    return sent


# --------------------------------------------------------------------------- #
# 主流程
# --------------------------------------------------------------------------- #


def tick(cfg: Dict[str, Any], state: Dict[str, Any], init_only: bool = False) -> int:
    trader = str(cfg.get("trader_name") or "带单员")
    orders = fetch_orders(cfg)

    seen: List[str] = list(state.get("seen_orders") or [])
    seen_set = set(seen)

    # 老版本的 state 存的是持仓不是成交，seen_orders 会是空的。
    # 这种情况当成第一次跑重建基线，否则会把 20 笔历史成交全推一遍。
    if state.get("initialized") and not seen:
        state["initialized"] = False

    if not state.get("initialized") or init_only:
        keys = [order_key(o) for o in orders]
        state["seen_orders"] = keys[:300]
        state["initialized"] = True
        save_state(state)
        log(f"基线已建立：已记录最近 {len(keys)} 笔成交，不推送")
        for o in orders[:5]:
            kind = "开仓" if is_open_side(o) else "平仓"
            pside = "多" if str(o.get("positionSide")).upper() == "LONG" else "空"
            log(
                f"  · {o.get('symbol')} {kind}{pside} "
                f"{fmt_qty(float(o.get('quantity') or 0))} @ {fmt_price(float(o.get('price') or 0))}"
            )
        return 0

    fresh = [o for o in orders if order_key(o) not in seen_set]
    if not fresh:
        log("没有新成交")
        return 0

    # 同币种同方向归组，避免分批建仓刷一串消息
    if cfg.get("merge_same_side", True):
        buckets: Dict[Tuple[str, str, str], List[Dict[str, Any]]] = {}
        for o in fresh:
            k = (str(o.get("symbol")), str(o.get("positionSide")), str(o.get("side")))
            buckets.setdefault(k, []).append(o)
        groups = list(buckets.values())
    else:
        groups = [[o] for o in fresh]

    watch = [str(s).upper() for s in (cfg.get("watch_symbols") or [])]
    min_notional = float(cfg.get("min_notional_usdt") or 0)
    on = cfg.get("notify_on") or {}

    # 只有出现平仓时才多花一次额度去取已平仓明细
    closed_map: Dict[str, List[Dict[str, Any]]] = {}
    if any(not is_open_side(g[0]) for g in groups):
        try:
            for pos in fetch_closed(cfg):
                closed_map.setdefault(str(pos.get("symbol")), []).append(pos)
        except FetchError as e:
            log(f"平仓明细没取到（不影响提醒）：{e}")

    pushed = 0
    for g in groups:
        symbol = str(g[0].get("symbol") or "").upper()
        if watch and symbol not in watch:
            continue
        notional = sum(float(o.get("price") or 0) * float(o.get("quantity") or 0) for o in g)
        if min_notional and notional < min_notional:
            continue
        opening = is_open_side(g[0])
        if opening and not on.get("open", True):
            continue
        if not opening and not on.get("close", True):
            continue
        title, body = build_message(g, trader, closed_map)
        log(f"推送：{title}")
        push_wechat(cfg, title, body)
        pushed += 1

    state["seen_orders"] = (list(seen) + [order_key(o) for o in fresh])[-400:]
    save_state(state)
    log(f"新成交 {len(fresh)} 笔，合并 {len(groups)} 组，推送 {pushed} 条")
    return pushed


def cmd_test(cfg: Dict[str, Any]) -> None:
    """发一条测试消息，确认微信能收到。不读数据、不动 state。"""
    trader = str(cfg.get("trader_name") or "带单员")
    title = f"【测试】{trader} 提醒已开通"
    body = (
        f"✅ 推送通道正常\n"
        f"监控对象：{trader}\n"
        f"监控内容：开仓 / 平仓\n"
        f"频率：约 5 分钟检查一次\n\n"
        f"以后他有新动作，你就会收到这样的消息。\n"
        f"（这条是测试，忽略即可）"
    )
    log(f"发送测试消息：{title}")
    if push_wechat(cfg, title, body):
        log("测试消息已发出，去微信看看")
    else:
        log("测试消息没发出去，检查 token 是否填对")
        sys.exit(1)


def cmd_probe(cfg: Dict[str, Any]) -> None:
    pid = str(cfg.get("portfolio_id") or "").strip()
    print(f"portfolioId = {pid or '(空)'}")
    if not pid:
        sys.exit("请先在 config.json 填 portfolio_id")

    print("\n===== 1) 带单员身份 =====")
    p = fetch_profile(cfg)
    sd = p.get("sourceDetails") or {}
    print(f"  昵称：{p.get('name')}")
    print(f"  带单状态：{sd.get('status')}")
    print(f"  公开持仓：{sd.get('positionsShared')}")
    print(f"  跟单人数：{p.get('copiers')}/{p.get('maxCopiers')}")

    print("\n===== 2) 最近成交（这就是提醒的数据源）=====")
    orders = fetch_orders(cfg, limit=10)
    print(f"  拿到 {len(orders)} 笔")
    for o in orders[:8]:
        kind = "开仓" if is_open_side(o) else "平仓"
        pside = "多" if str(o.get("positionSide")).upper() == "LONG" else "空"
        print(
            f"  · {fmt_ts(o.get('createdAt'))}  {o.get('symbol')}  {kind}{pside}  "
            f"{fmt_qty(float(o.get('quantity') or 0))} @ {fmt_price(float(o.get('price') or 0))}"
        )

    print("\n===== 3) 已平仓记录（用来补盈亏）=====")
    closed = fetch_closed(cfg, limit=3)
    print(f"  拿到 {len(closed)} 条")
    for c in closed[:3]:
        print(
            f"  · {c.get('symbol')} {c.get('side')}  "
            f"{fmt_price(float(c.get('entryPrice') or 0))} → {fmt_price(float(c.get('closePrice') or 0))}  "
            f"盈亏 {fmt_pnl(float(c.get('grossPnl') or 0))}"
        )

    print("\n===== 结论 =====")
    print("昵称是「熬鹰资本」→ 编号没抄错；有成交记录 → 提醒能跑起来。")


def main() -> None:
    ap = argparse.ArgumentParser(description="币安带单员成交变动 -> 微信提醒")
    ap.add_argument("--config", default=str(BASE_DIR / "config.json"))
    ap.add_argument("--once", action="store_true", help="只跑一轮（GitHub Actions 用）")
    ap.add_argument("--init", action="store_true", help="把当前成交记为基线，不推送")
    ap.add_argument("--probe", action="store_true", help="自检：打印身份和最近成交")
    ap.add_argument("--test", action="store_true", help="往微信发一条测试消息")
    ap.add_argument("--pid", help="临时指定 portfolioId")
    args = ap.parse_args()

    cfg = load_config(Path(args.config))
    if args.pid:
        cfg["portfolio_id"] = args.pid

    if args.test:
        cmd_test(cfg)
        return

    if args.probe:
        try:
            cmd_probe(cfg)
        except FetchError as e:
            log(f"自检失败：{e}")
            sys.exit(1)
        return

    state = load_state()

    if args.init:
        try:
            tick(cfg, state, init_only=True)
        except FetchError as e:
            log(f"抓取失败：{e}")
            sys.exit(1)
        return

    if args.once:
        try:
            tick(cfg, state)
        except FetchError as e:
            log(f"抓取失败：{e}")
            sys.exit(1)
        return

    log(f"开始常驻，每 {cfg.get('poll_interval_sec', 60)} 秒一轮，Ctrl+C 退出")
    while True:
        try:
            tick(cfg, state)
        except KeyboardInterrupt:
            log("手动退出")
            break
        except FetchError as e:
            log(f"抓取失败：{e}")
        except Exception as e:  # noqa: BLE001
            log(f"未预期错误：{e}")
        time.sleep(int(cfg.get("poll_interval_sec", 60)))


if __name__ == "__main__":
    main()

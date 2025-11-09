# -*- coding: utf-8 -*-
"""
Created on Sun Nov  9 10:34:05 2025

@author: james
"""

import json
import os
from typing import Dict, List, Tuple

class ETFDataProcessor:
    """
    功能新增摘要
    1) 連續增減偵測(#7, 修正版)：最近 N 日內，計算「上調事件次數 / 下調事件次數」(不要求連續)。
    2) 異動排行(#8)：今日 vs 前日的 Top 變動（張數/權重，上/下）、今日新增/出清。
    3) 異動比對彙總(#15)：新增/刪除家數、加/減持家數、淨增減張數與權重。

    參數：
      trend_window: 觀察視窗天數 (預設 10)
      min_increase_events: 在視窗內最少上調事件次數才列入 increasing (預設 3)
      min_decrease_events: 在視窗內最少下調事件次數才列入 decreasing (預設 3)
      entry_threshold: 視為「有部位」的最低股數門檻（以股數計，預設 1000 股 => 1 張）
      ranks_top_n: 異動排行輸出筆數
    """
    def __init__(
        self,
        data_dir: str,
        trend_window: int = 10,
        min_increase_events: int = 3,
        min_decrease_events: int = 3,
        entry_threshold: int = 1000,
        ranks_top_n: int = 20,
    ):
        self.data_dir = data_dir
        self.etf_files = {
            "00980A": "00980A_holdings.json",
            "00981A": "00981A_holdings.json",
            "00982A": "00982A_holdings.json",
        }
        self.etf_names = {
            "00980A": "野村臺灣智慧優選主動式ETF",
            "00981A": "統一台股增長",
            "00982A": "群益台灣精選強棒主動式ETF基金",
        }
        self.raw_data: Dict[str, List[Dict]] = {}
        self.stock_name_map = {
            "1210": "大立光", "1303": "南亞", "1319": "東陽", "1326": "磨石", "1560": "中砂",
            "2317": "鴻海", "2330": "台積電", "2345": "智邦", "2354": "鴻準", "2357": "華碩",
            "2368": "金像電", "2383": "台光電", "2454": "聯發科", "2618": "長榮",
            "2808": "豐祥", "3017": "奇鋐", "3037": "欣興", "3264": "欣銓", "3293": "鈺漲",
            "3376": "新日興", "3529": "新美亞", "3583": "辛耘", "3665": "貿聯", "3711": "日月光",
            "5347": "世界", "5434": "崇義", "6121": "新巨", "6223": "旺矽", "6257": "宏科",
            "6274": "台燿", "6515": "力晶", "6670": "宏達", "8046": "南電", "8069": "瑞銀",
            "8114": "振樺", "2884": "玉山金", "2308": "台達電", "2344": "華邦電", "2449": "京元電",
            "2027": "大成鋼", "6669": "緯穎", "2024": "鴨肉王", "1476": "儒鴻", "3034": "聯詠"
        }

        # 參數設定
        self.trend_window = trend_window
        self.min_increase_events = min_increase_events
        self.min_decrease_events = min_decrease_events
        self.entry_threshold = entry_threshold  # 以股數為單位
        self.ranks_top_n = ranks_top_n

    # ========== 基本 I/O ==========
    def load_raw_data(self) -> bool:
        print(f"【1】載入原始數據...\n   目錄: {self.data_dir}\n")
        for code, fname in self.etf_files.items():
            filepath = os.path.join(self.data_dir, fname)
            if not os.path.exists(filepath):
                print(f"✗ 檔案不存在：{filepath}")
                return False
            with open(filepath, "r", encoding="utf-8") as f:
                self.raw_data[code] = json.load(f)
            print(f"✓ 已載入 {code} ({self.etf_names[code]}) 共 {len(self.raw_data[code])} 筆日期資料")
        return True

    def _sorted_records(self, etf_code: str) -> List[Dict]:
        return sorted(self.raw_data[etf_code], key=lambda x: x["data_date"])

    def get_latest_two_dates(self, etf_code: str) -> Tuple[Dict, Dict]:
        sorted_data = self._sorted_records(etf_code)
        latest = sorted_data[-1]
        prev = sorted_data[-2]
        return latest, prev

    # ========== 小工具 ==========
    def get_stock_name(self, code: str, name_from_data: str = "") -> str:
        if name_from_data and len(name_from_data) > 1:
            return name_from_data
        return self.stock_name_map.get(code, f"({code})")

    def _thousand_shares(self, shares: int) -> int:
        """將股數轉為張（無條件捨去整張）。"""
        return shares // 1000

    # ========== #8 異動排行 + #15 彙總 ==========
    def _build_daily_delta(self, etf_code: str):
        latest, prev = self.get_latest_two_dates(etf_code)
        L = latest["holdings"]
        P = prev["holdings"]

        all_codes = set(L) | set(P)
        deltas = []
        summary = {
            "date": latest["data_date"],
            "prev_date": prev["data_date"],
            "new_count": 0,
            "closed_count": 0,
            "add_count": 0,
            "reduce_count": 0,
            "net_count_change": 0,    # 以張為單位
            "net_weight_change": 0.0,
        }

        for code in all_codes:
            l = L.get(code, {})
            p = P.get(code, {})
            lc = l.get("count", 0)
            lw = l.get("weight", 0.0)
            pc = p.get("count", 0)
            pw = p.get("weight", 0.0)

            cc = lc - pc
            cw = round(lw - pw, 6)

            # 事件分類
            if pc == 0 and lc > 0:
                summary["new_count"] += 1
                evt = "新增"
            elif lc == 0 and pc > 0:
                summary["closed_count"] += 1
                evt = "刪除"
            elif cc > 0:
                summary["add_count"] += 1
                evt = "增持"
            elif cc < 0:
                summary["reduce_count"] += 1
                evt = "減持"
            else:
                evt = "持平"

            deltas.append({
                "code": code,
                "name": self.get_stock_name(code, l.get("name") or p.get("name") or ""),
                "count_change": self._thousand_shares(cc),
                "weight_change": cw,
                "prev_count": self._thousand_shares(pc),
                "prev_weight": pw,
                "current_count": self._thousand_shares(lc),
                "current_weight": lw,
                "event": evt
            })

            summary["net_count_change"] += self._thousand_shares(cc)
            summary["net_weight_change"] += cw

        # Ranks
        topN = self.ranks_top_n
        # 排行以「張數變動」與「權重變動」分別挑前 N 名（漲 / 跌）
        rank = {
            "top_count_up":   sorted([d for d in deltas if d["count_change"] > 0],
                                     key=lambda x: x["count_change"], reverse=True)[:topN],
            "top_count_down": sorted([d for d in deltas if d["count_change"] < 0],
                                     key=lambda x: x["count_change"])[:topN],
            "top_weight_up":  sorted([d for d in deltas if d["weight_change"] > 0],
                                     key=lambda x: x["weight_change"], reverse=True)[:topN],
            "top_weight_down":sorted([d for d in deltas if d["weight_change"] < 0],
                                     key=lambda x: x["weight_change"])[:topN],
            "new_positions":  [d for d in deltas if d["event"] == "新增"][:topN],
            "closed_positions":[d for d in deltas if d["event"] == "刪除"][:topN],
        }

        return deltas, rank, summary

    def calculate_daily_changes(self, etf_code: str) -> List[Dict]:
        """
        沿用你原有的 daily_changes 規則，但先重算一遍 delta 再套過濾條件：
        - 顯示條件：事件為「新增/刪除」或 張數變動≥50 張 或 權重變動≥0.25%
        """
        deltas, _, _ = self._build_daily_delta(etf_code)
        changes = []
        for d in deltas:
            t = d["event"]
            cc = d["count_change"]
            cw = d["weight_change"]
            if t in ("新增", "刪除") or (cc >= 50) or (cw >= 0.25):
                changes.append({
                    "code": d["code"],
                    "name": d["name"],
                    "type": t,
                    "count_change": cc,
                    "weight_change": cw,
                    "prev_count": d["prev_count"],
                    "prev_weight": d["prev_weight"],
                    "current_count": d["current_count"],
                    "current_weight": d["current_weight"],
                })
        changes.sort(key=lambda x: abs(x["count_change"]), reverse=True)
        return changes

    # ========== #7 修正版：事件次數法 ==========
    def analyze_etf_strategy(self, etf_code: str) -> Dict:
        """
        最近 self.trend_window 個交易日中：
          - increasing: 上調事件次數 >= self.min_increase_events
          - decreasing: 下調事件次數 >= self.min_decrease_events
        事件：counts[i] > counts[i-1] 視為「上調事件一次」(相反為下調)
        另外輸出 new_positions / closed_positions（視窗首尾判定）
        """
        if etf_code not in self.raw_data:
            return {"increasing": [], "decreasing": [], "new_positions": [], "closed_positions": []}

        sorted_data = self._sorted_records(etf_code)
        window_records = sorted_data[-self.trend_window:] if len(sorted_data) >= self.trend_window else sorted_data

        # 收集視窗內每檔股票的 counts & dates（若某日未持有，視為 0）
        dates = [r["data_date"] for r in window_records]
        # 先收集所有出現過的股票代碼
        codes = set()
        for r in window_records:
            codes |= set(r.get("holdings", {}).keys())

        result = {
            "increasing": [],
            "decreasing": [],
            "new_positions": [],
            "closed_positions": []
        }

        for code in codes:
            counts = []
            weights = []
            namestr = ""
            for r in window_records:
                info = r.get("holdings", {}).get(code)
                if info:
                    c = info.get("count", 0)
                    w = info.get("weight", 0.0)
                    namestr = namestr or info.get("name", "")
                else:
                    c, w = 0, 0.0
                counts.append(c)
                weights.append(w)

            name = self.get_stock_name(code, namestr)
            # 計算事件次數
            up_events = 0
            down_events = 0
            for i in range(1, len(counts)):
                if counts[i] > counts[i-1]:
                    up_events += 1
                elif counts[i] < counts[i-1]:
                    down_events += 1

            first_has = counts[0] > self.entry_threshold
            last_has = counts[-1] > self.entry_threshold

            # 上調/下調結果
            if up_events >= self.min_increase_events and last_has:
                result["increasing"].append({
                    "code": code,
                    "name": name,
                    "events": up_events,
                    "window_days": len(counts),
                    "net_increase": self._thousand_shares(counts[-1] - counts[0]),
                    "first_date": dates[0],
                    "last_date": dates[-1],
                    "current_count": self._thousand_shares(counts[-1]),
                })
            if down_events >= self.min_decrease_events and (counts[-1] >= 0):
                result["decreasing"].append({
                    "code": code,
                    "name": name,
                    "events": down_events,
                    "window_days": len(counts),
                    "net_decrease": self._thousand_shares(counts[0] - counts[-1]),
                    "first_date": dates[0],
                    "last_date": dates[-1],
                    "current_count": self._thousand_shares(counts[-1]),
                })

            # 新建立 / 已平倉（視窗首尾判斷）
            if (not first_has) and last_has:
                result["new_positions"].append({
                    "code": code,
                    "name": name,
                    "entry_date": dates[-1],  # 嚴格說應找第一個 > threshold 的日期；這裡簡化為視窗尾日
                    "current_count": self._thousand_shares(counts[-1])
                })
            if first_has and (not last_has):
                # 同上，嚴格應找最後一次 > threshold 的日期；這裡以視窗尾判出清
                result["closed_positions"].append({
                    "code": code,
                    "name": name,
                    "exit_date": dates[-1],
                })

        # 排序：就以事件次數 / 當前張數做主要排序
        result["increasing"].sort(key=lambda x: (x["events"], x["current_count"]), reverse=True)
        result["decreasing"].sort(key=lambda x: (x["events"], x["current_count"]), reverse=True)
        return result

    # ========== 產出 processed_etf_data.json ==========
    def save_processed_data(self):
        processed = {}
        for etf_code in self.etf_files:
            if etf_code not in self.raw_data:
                continue

            latest, prev = self.get_latest_two_dates(etf_code)
            price_info = latest.get("price_info", {}) or {}

            # #8 + #15
            _, ranks, summary = self._build_daily_delta(etf_code)
            # #7 新定義
            strategy = self.analyze_etf_strategy(etf_code)
            # 今日重點異動卡片（沿用你原始規則）
            daily_changes = self.calculate_daily_changes(etf_code)

            processed[etf_code] = {
                "name": self.etf_names[etf_code],
                "latest_date": latest["data_date"],
                "previous_date": prev["data_date"],
                "price": price_info.get("price"),
                "change_value": price_info.get("change_value"),
                "change_percent": price_info.get("change_percent"),
                "daily_changes": daily_changes,   # 卡片用
                "strategy_params": {
                    "trend_window": self.trend_window,
                    "min_increase_events": self.min_increase_events,
                    "min_decrease_events": self.min_decrease_events,
                    "entry_threshold_shares": self.entry_threshold,
                },
                "strategy": strategy,             # #7 輸出
                "ranks": ranks,                   # #8 輸出
                "summary": summary,               # #15 輸出
            }

        outpath = os.path.join(self.data_dir, "processed_etf_data.json")
        with open(outpath, "w", encoding="utf-8") as f:
            json.dump(processed, f, ensure_ascii=False, indent=2)
        print(f"✓ 已保存 {outpath}")

    # ========== 前端詳細頁歷史 ==========
    def get_stock_full_history(self, etf_code: str, stock_code: str) -> List[Dict]:
        if etf_code not in self.raw_data:
            return []

        sorted_data = self._sorted_records(etf_code)

        history = []
        prev_count = None
        prev_weight = 0.0

        for record in sorted_data:
            holdings = record.get('holdings', {})
            stock_info = holdings.get(stock_code)

            current_count = stock_info.get('count', 0) if stock_info else 0
            current_weight = stock_info.get('weight', 0.0) if stock_info else 0.0

            # 只有在「部位顯著變化或首次/出清」時紀錄，以減少前端繪圖點數
            if current_count > self.entry_threshold and (prev_count is None or current_count != prev_count):
                count_change = current_count - (prev_count if prev_count is not None else current_count)
                weight_change = round(current_weight - (prev_weight if history else 0.0), 4)

                if prev_count is None:
                    status = "首次出現"
                elif count_change > 0:
                    status = "增持"
                elif count_change < 0:
                    status = "減持"
                else:
                    status = "持平"

                history.append({
                    "date": record['data_date'],
                    "count": self._thousand_shares(current_count),
                    "weight": current_weight,
                    "count_change": self._thousand_shares(count_change),
                    "weight_change": weight_change,
                    "status": status
                })
                prev_count = current_count
                prev_weight = current_weight

            elif current_count <= self.entry_threshold and prev_count and prev_count > self.entry_threshold:
                count_change = current_count - prev_count
                weight_change = round(current_weight - prev_weight, 4)
                history.append({
                    "date": record['data_date'],
                    "count": self._thousand_shares(current_count),
                    "weight": current_weight,
                    "count_change": self._thousand_shares(count_change),
                    "weight_change": weight_change,
                    "status": "出清"
                })
                prev_count = None
                prev_weight = 0.0

        return history

    def build_all_stock_history(self) -> Dict:
        all_stocks: Dict[str, Dict] = {}

        for etf_code in self.etf_files.keys():
            data = self.raw_data.get(etf_code, [])
            stock_codes = set()
            for record in data:
                stock_codes |= set(record.get('holdings', {}).keys())

            for stock_code in stock_codes:
                if stock_code not in all_stocks:
                    all_stocks[stock_code] = {
                        "code": stock_code,
                        "name": "",
                        "etf_holdings": {}
                    }

                history = self.get_stock_full_history(etf_code, stock_code)
                if not history:
                    continue

                # 找名稱
                if all_stocks[stock_code]["name"] == "":
                    try:
                        for rec in reversed(self.raw_data[etf_code]):
                            if stock_code in rec.get('holdings', {}):
                                name = rec['holdings'][stock_code].get('name', '')
                                if name and len(name) > 1:
                                    all_stocks[stock_code]["name"] = name
                                    break
                    except Exception:
                        pass
                if not all_stocks[stock_code]["name"]:
                    all_stocks[stock_code]["name"] = self.get_stock_name(stock_code, "")

                max_record = max(history, key=lambda x: x['count'])
                min_record = min(history, key=lambda x: x['count'])
                current_record = history[-1]

                all_stocks[stock_code]["etf_holdings"][etf_code] = {
                    "etf_name": self.etf_names[etf_code],
                    "current_count": current_record['count'],
                    "current_weight": current_record['weight'],
                    "max_count": max_record['count'],
                    "max_count_date": max_record['date'],
                    "min_count": min_record['count'],
                    "min_count_date": min_record['date'],
                    "history": history
                }

        return all_stocks

    def save_stock_history_data(self, filename="stock_history_data.json"):
        history_data = self.build_all_stock_history()
        filepath = os.path.join(self.data_dir, filename)
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(history_data, f, ensure_ascii=False, indent=2)
        print(f"✓ 已保存精簡版股票歷史資料至: {filepath}")


if __name__ == "__main__":
    data_dir = r"C:/Users/j3210/OneDrive/桌面/主動型ETF追蹤/etf_holdings"
    processor = ETFDataProcessor(
        data_dir=data_dir,
        trend_window=10,          # ← 最近10個交易日
        min_increase_events=3,    # ← 至少3次上調事件
        min_decrease_events=3,    # ← 至少3次下調事件
        entry_threshold=1000,     # ← 視為有部位的門檻（股數）
        ranks_top_n=20            # ← 排行輸出前N名
    )
    if processor.load_raw_data():
        processor.save_processed_data()
        processor.save_stock_history_data()
    else:
        print("無法載入數據，請確認目錄與JSON檔案名稱")

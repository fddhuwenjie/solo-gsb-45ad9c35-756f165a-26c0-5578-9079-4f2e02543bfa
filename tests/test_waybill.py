"""运输联单风险快照 API 测试：逐批重算核对、案件归属/覆盖、原子性与幂等。"""

import json
import os
import sqlite3
import tempfile
import time
import unittest

_TMP_DB = os.path.join(tempfile.mkdtemp(prefix="coldchain-waybill-"), "waybill.db")
os.environ["COLDCHAIN_DB"] = _TMP_DB

from fastapi.testclient import TestClient  # noqa: E402

from app import db  # noqa: E402
from app.main import app  # noqa: E402


def iso(epoch_seconds: int) -> str:
    from datetime import datetime, timezone

    return (
        datetime.fromtimestamp(epoch_seconds, tz=timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z")
    )


def iso_out(epoch_seconds: int) -> str:
    return iso(epoch_seconds).replace("Z", ".000Z")


def make_two_anomaly_payload(batch_id):
    """两条异常：0–240（装货段）、1200–1260（运输段）。"""
    return {
        "batch_id": batch_id,
        "device_id": "reefer-01",
        "gap_seconds": 600,
        "dip_tolerance_seconds": 120,
        "samples": [
            {"timestamp": iso(0), "temperature": 9.0},
            {"timestamp": iso(60), "temperature": 9.5},
            {"timestamp": iso(120), "temperature": 5.0},
            {"timestamp": iso(180), "temperature": 10.0},
            {"timestamp": iso(240), "temperature": 11.0},
            {"timestamp": iso(1200), "temperature": 9.0},
            {"timestamp": iso(1260), "temperature": 9.0},
        ],
        "segments": [
            {"segment_id": "seg-1", "segment_type": "loading",
             "start": iso(0), "end": iso(600),
             "lower_limit": 0.0, "upper_limit": 8.0},
            {"segment_id": "seg-2", "segment_type": "transport",
             "start": iso(600), "end": iso(1800),
             "lower_limit": 0.0, "upper_limit": 8.0},
        ],
    }


def make_one_anomaly_payload(batch_id):
    """一条异常：0–60。"""
    return {
        "batch_id": batch_id,
        "device_id": "reefer-02",
        "samples": [
            {"timestamp": iso(0), "temperature": 9.0},
            {"timestamp": iso(60), "temperature": 9.5},
        ],
        "segments": [
            {"segment_id": "seg-1", "segment_type": "transport",
             "start": iso(0), "end": iso(600),
             "lower_limit": 0.0, "upper_limit": 8.0},
        ],
    }


def make_normal_payload(batch_id):
    """无异常批次。"""
    return {
        "batch_id": batch_id,
        "device_id": "reefer-03",
        "samples": [
            {"timestamp": iso(0), "temperature": 5.0},
            {"timestamp": iso(60), "temperature": 6.0},
        ],
        "segments": [
            {"segment_id": "seg-1", "segment_type": "loading",
             "start": iso(0), "end": iso(600),
             "lower_limit": 0.0, "upper_limit": 8.0},
        ],
    }


def item(batch_id, digest, case_id, handoff):
    return {
        "batch_id": batch_id,
        "analysis_digest": digest,
        "case_id": case_id,
        "handoff_at": iso(handoff),
    }


def waybill_body(shipment_id, items):
    return {"shipment_id": shipment_id, "batches": items}


class WaybillTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # 与其他测试模块并行运行时库路径可能已被先导入的模块固定，这里显式指向
        # 本模块独立库，保证用例隔离。
        db._DB_PATH = _TMP_DB
        db.init_db()
        cls.client = TestClient(app)

        # W-001：两条异常，案件 CASE-W1 closed（resolved + dismissed）
        r = cls.client.post(
            "/api/v1/batches/analyze", json=make_two_anomaly_payload("W-001")
        )
        assert r.status_code == 200, r.text
        cls.digest1 = r.json()["analysis_digest"]
        r = cls.client.post(
            "/api/v1/cases/review",
            json={
                "batch_id": "W-001",
                "case_id": "CASE-W1",
                "analysis_digest": cls.digest1,
                "decisions": [
                    {"seq": 0, "decision": "resolved",
                     "handled_at": iso(300), "note": "已复冷"},
                    {"seq": 1, "decision": "dismissed",
                     "handled_at": iso(1300), "note": "传感器误报"},
                ],
            },
        )
        assert r.status_code == 200, r.text
        assert r.json()["status"] == "closed"

        # W-002：一条异常，案件 CASE-W2 open（accepted）
        r = cls.client.post(
            "/api/v1/batches/analyze", json=make_one_anomaly_payload("W-002")
        )
        assert r.status_code == 200, r.text
        cls.digest2 = r.json()["analysis_digest"]
        r = cls.client.post(
            "/api/v1/cases/review",
            json={
                "batch_id": "W-002",
                "case_id": "CASE-W2",
                "analysis_digest": cls.digest2,
                "decisions": [
                    {"seq": 0, "decision": "accepted", "handled_at": iso(120)},
                ],
            },
        )
        assert r.status_code == 200, r.text
        assert r.json()["status"] == "open"

        # W-003：无异常，案件 CASE-W3 closed（空处置）
        r = cls.client.post(
            "/api/v1/batches/analyze", json=make_normal_payload("W-003")
        )
        assert r.status_code == 200, r.text
        cls.digest3 = r.json()["analysis_digest"]
        r = cls.client.post(
            "/api/v1/cases/review",
            json={
                "batch_id": "W-003",
                "case_id": "CASE-W3",
                "analysis_digest": cls.digest3,
                "decisions": [],
            },
        )
        assert r.status_code == 200, r.text
        assert r.json()["status"] == "closed"

    # ---------- 基础放行/扣留 ----------

    def test_01_released_when_all_cases_closed(self):
        body = waybill_body("SHIP-R", [
            item("W-001", self.digest1, "CASE-W1", 1000),
            item("W-003", self.digest3, "CASE-W3", 2000),
        ])
        r = self.client.post("/api/v1/shipments/waybills", json=body)
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(r.headers.get("X-Cache"), "MISS")
        resp = r.json()
        self.assertFalse(resp["cached"])
        self.assertEqual(resp["shipment_id"], "SHIP-R")
        self.assertEqual(resp["result"], "released")
        self.assertEqual([b["position"] for b in resp["batches"]], [0, 1])
        self.assertEqual(
            [b["batch_id"] for b in resp["batches"]], ["W-001", "W-003"]
        )
        self.assertEqual(
            [b["anomaly_count"] for b in resp["batches"]], [2, 0]
        )
        self.assertEqual(
            [b["case_status"] for b in resp["batches"]], ["closed", "closed"]
        )
        self.assertEqual(resp["batches"][0]["case_id"], "CASE-W1")
        self.assertEqual(resp["batches"][0]["handoff_at"], iso_out(1000))
        self.assertEqual(resp["batches"][1]["handoff_at"], iso_out(2000))

    def test_02_held_when_any_case_open(self):
        body = waybill_body("SHIP-H", [
            item("W-001", self.digest1, "CASE-W1", 1000),
            item("W-002", self.digest2, "CASE-W2", 2000),
        ])
        r = self.client.post("/api/v1/shipments/waybills", json=body)
        self.assertEqual(r.status_code, 200, r.text)
        resp = r.json()
        self.assertEqual(resp["result"], "held")
        self.assertEqual(
            [b["case_status"] for b in resp["batches"]], ["closed", "open"]
        )
        self.assertEqual(
            [b["anomaly_count"] for b in resp["batches"]], [2, 1]
        )

    # ---------- 幂等与换内容 ----------

    def test_03_same_shipment_same_content_is_idempotent(self):
        body = waybill_body("SHIP-IDEM", [
            item("W-001", self.digest1, "CASE-W1", 1000),
            item("W-003", self.digest3, "CASE-W3", 2000),
        ])
        r1 = self.client.post("/api/v1/shipments/waybills", json=body)
        self.assertEqual(r1.status_code, 200)
        with sqlite3.connect(_TMP_DB) as conn:
            rows_before = conn.execute(
                "SELECT COUNT(*) FROM waybills"
            ).fetchone()[0]

        r2 = self.client.post("/api/v1/shipments/waybills", json=body)
        self.assertEqual(r2.status_code, 200)
        self.assertEqual(r2.headers.get("X-Cache"), "HIT")
        self.assertTrue(r2.json()["cached"])
        with sqlite3.connect(_TMP_DB) as conn:
            self.assertEqual(
                conn.execute("SELECT COUNT(*) FROM waybills").fetchone()[0],
                rows_before,
            )

        # 除 cached 标记外回放内容完全一致
        j1, j2 = r1.json(), r2.json()
        self.assertFalse(j1.pop("cached"))
        self.assertTrue(j2.pop("cached"))
        self.assertEqual(j1, j2)

    def test_04_same_shipment_changed_content_conflicts(self):
        base = waybill_body("SHIP-R", [
            item("W-001", self.digest1, "CASE-W1", 1000),
            item("W-003", self.digest3, "CASE-W3", 2000),
        ])
        # 改交接时间
        changed = json.loads(json.dumps(base))
        changed["batches"][0]["handoff_at"] = iso(1001)
        r = self.client.post("/api/v1/shipments/waybills", json=changed)
        self.assertEqual(r.status_code, 409)
        # 改案件号
        changed = json.loads(json.dumps(base))
        changed["batches"][0]["case_id"] = "CASE-W3"
        r = self.client.post("/api/v1/shipments/waybills", json=changed)
        self.assertEqual(r.status_code, 409)
        # 换摘要
        changed = json.loads(json.dumps(base))
        changed["batches"][0]["analysis_digest"] = "0" * 64
        r = self.client.post("/api/v1/shipments/waybills", json=changed)
        self.assertEqual(r.status_code, 409)
        # 增减批次
        changed = json.loads(json.dumps(base))
        changed["batches"] = changed["batches"][:1]
        r = self.client.post("/api/v1/shipments/waybills", json=changed)
        self.assertEqual(r.status_code, 409)

        # 原联单不被覆盖
        with sqlite3.connect(_TMP_DB) as conn:
            row = conn.execute(
                "SELECT result FROM waybills WHERE shipment_id = 'SHIP-R'"
            ).fetchone()
            count = conn.execute(
                "SELECT COUNT(*) FROM waybill_batches"
                " WHERE shipment_id = 'SHIP-R'"
            ).fetchone()[0]
        self.assertEqual(row[0], "released")
        self.assertEqual(count, 2)

    # ---------- 可定位冲突与原子性 ----------

    def test_05_stale_digest_409_locatable_and_no_partial_write(self):
        body = waybill_body("SHIP-STALE", [
            item("W-001", self.digest1, "CASE-W1", 1000),
            item("W-002", "0" * 64, "CASE-W2", 2000),
        ])
        r = self.client.post("/api/v1/shipments/waybills", json=body)
        self.assertEqual(r.status_code, 409)
        detail = r.json()["detail"]
        self.assertIn("W-002", detail)
        self.assertIn("第 1 项", detail)
        self.assertIn("过期", detail)
        # 不得部分写入
        with sqlite3.connect(_TMP_DB) as conn:
            self.assertIsNone(
                conn.execute(
                    "SELECT 1 FROM waybills WHERE shipment_id = 'SHIP-STALE'"
                ).fetchone()
            )
            self.assertEqual(
                conn.execute(
                    "SELECT COUNT(*) FROM waybill_batches"
                    " WHERE shipment_id = 'SHIP-STALE'"
                ).fetchone()[0],
                0,
            )

    def test_06_unknown_batch_404_locatable(self):
        body = waybill_body("SHIP-U1", [
            item("W-NOPE", "0" * 64, "CASE-X", 1000),
        ])
        r = self.client.post("/api/v1/shipments/waybills", json=body)
        self.assertEqual(r.status_code, 404)
        self.assertIn("W-NOPE", r.json()["detail"])

    def test_07_unknown_case_409_missing_disposition(self):
        body = waybill_body("SHIP-U2", [
            item("W-001", self.digest1, "CASE-NOPE", 1000),
        ])
        r = self.client.post("/api/v1/shipments/waybills", json=body)
        self.assertEqual(r.status_code, 409)
        detail = r.json()["detail"]
        self.assertIn("CASE-NOPE", detail)
        self.assertIn("W-001", detail)

    def test_08_case_belonging_to_other_batch_409(self):
        body = waybill_body("SHIP-U3", [
            item("W-001", self.digest1, "CASE-W2", 1000),
        ])
        r = self.client.post("/api/v1/shipments/waybills", json=body)
        self.assertEqual(r.status_code, 409)
        detail = r.json()["detail"]
        self.assertIn("CASE-W2", detail)
        self.assertIn("不匹配", detail)

    def test_09_case_bound_to_other_snapshot_409(self):
        """案件存在且属于该批次，但绑定的分析摘要与当前重算结果不一致。"""
        with sqlite3.connect(_TMP_DB) as conn:
            conn.execute(
                """INSERT INTO cases
                   (case_id, batch_id, analysis_digest, content_hash,
                    status, created_at)
                   VALUES ('CASE-BADDIGEST', 'W-001', ?, 'x', 'closed', ?)""",
                ("0" * 64, time.time()),
            )
            conn.execute(
                """INSERT INTO case_decisions
                   (case_id, seq, decision, handled_at, note)
                   VALUES ('CASE-BADDIGEST', 0, 'resolved', 300, 'x')""",
            )
        body = waybill_body("SHIP-U4", [
            item("W-001", self.digest1, "CASE-BADDIGEST", 1000),
        ])
        r = self.client.post("/api/v1/shipments/waybills", json=body)
        self.assertEqual(r.status_code, 409)
        self.assertIn("CASE-BADDIGEST", r.json()["detail"])

    def test_10_case_not_covering_current_anomalies_409(self):
        """案件处置行未覆盖当前全部异常（模拟历史遗留数据）→ 缺少处置。"""
        with sqlite3.connect(_TMP_DB) as conn:
            conn.execute(
                """INSERT INTO cases
                   (case_id, batch_id, analysis_digest, content_hash,
                    status, created_at)
                   VALUES ('CASE-PARTIAL', 'W-001', ?, 'y', 'closed', ?)""",
                (self.digest1, time.time()),
            )
            # 只覆盖 seq 0，缺 seq 1
            conn.execute(
                """INSERT INTO case_decisions
                   (case_id, seq, decision, handled_at, note)
                   VALUES ('CASE-PARTIAL', 0, 'resolved', 300, 'x')""",
            )
        body = waybill_body("SHIP-U5", [
            item("W-001", self.digest1, "CASE-PARTIAL", 1000),
        ])
        r = self.client.post("/api/v1/shipments/waybills", json=body)
        self.assertEqual(r.status_code, 409)
        detail = r.json()["detail"]
        self.assertIn("CASE-PARTIAL", detail)
        self.assertIn("缺少处置", detail)
        self.assertIn("缺失序号=[1]", detail)

    # ---------- 请求层校验 ----------

    def test_11_duplicate_batch_rejected(self):
        body = waybill_body("SHIP-DUP", [
            item("W-001", self.digest1, "CASE-W1", 1000),
            item("W-001", self.digest1, "CASE-W1", 2000),
        ])
        r = self.client.post("/api/v1/shipments/waybills", json=body)
        self.assertEqual(r.status_code, 422)

    def test_12_handoff_must_be_strictly_increasing(self):
        # 相等不允许
        body = waybill_body("SHIP-EQ", [
            item("W-001", self.digest1, "CASE-W1", 1000),
            item("W-003", self.digest3, "CASE-W3", 1000),
        ])
        r = self.client.post("/api/v1/shipments/waybills", json=body)
        self.assertEqual(r.status_code, 422)
        # 递减不允许
        body = waybill_body("SHIP-DESC", [
            item("W-001", self.digest1, "CASE-W1", 2000),
            item("W-003", self.digest3, "CASE-W3", 1000),
        ])
        r = self.client.post("/api/v1/shipments/waybills", json=body)
        self.assertEqual(r.status_code, 422)
        # 换顺序同样破坏递增（顺序即交接语义）
        body = waybill_body("SHIP-SWAP", [
            item("W-003", self.digest3, "CASE-W3", 2000),
            item("W-001", self.digest1, "CASE-W1", 1000),
        ])
        r = self.client.post("/api/v1/shipments/waybills", json=body)
        self.assertEqual(r.status_code, 422)

    def test_13_empty_batches_rejected(self):
        r = self.client.post(
            "/api/v1/shipments/waybills",
            json=waybill_body("SHIP-EMPTY", []),
        )
        self.assertEqual(r.status_code, 422)

    # ---------- 事务内重算与回滚 ----------

    def test_14_failed_waybill_rolls_back_in_transaction_recompute(self):
        """第一项旧口径批次在事务内被重算覆盖，第二项过期 → 整体回滚。

        409 后：联单未落库；旧口径批次的异常行保持原样（重算覆盖随事务
        回滚），随后单独组单成功时才真正修正。
        """
        legacy_id = "W-LEGACY"
        payload = self._seed_legacy_batch(legacy_id)

        from app.main import _analysis_digest, _run_detection
        from app.models import AnalysisRequest

        fresh = _run_detection(AnalysisRequest.model_validate(payload))
        digest = _analysis_digest(fresh)
        # 直接落一个绑定当前重算摘要的案件（不经过复核接口，避免提前修正异常行）
        with sqlite3.connect(_TMP_DB) as conn:
            conn.execute(
                """INSERT INTO cases
                   (case_id, batch_id, analysis_digest, content_hash,
                    status, created_at)
                   VALUES ('CASE-WLEG', ?, ?, 'z', 'closed', ?)""",
                (legacy_id, digest, time.time()),
            )
            conn.execute(
                """INSERT INTO case_decisions
                   (case_id, seq, decision, handled_at, note)
                   VALUES ('CASE-WLEG', 0, 'resolved', 1100, 'ok')""",
            )

        body = waybill_body("SHIP-LEG", [
            item(legacy_id, digest, "CASE-WLEG", 1000),
            item("W-001", "0" * 64, "CASE-W1", 2000),  # 过期 → 409
        ])
        r = self.client.post("/api/v1/shipments/waybills", json=body)
        self.assertEqual(r.status_code, 409)
        self.assertIn("W-001", r.json()["detail"])

        with sqlite3.connect(_TMP_DB) as conn:
            # 联单未落库
            self.assertIsNone(
                conn.execute(
                    "SELECT 1 FROM waybills WHERE shipment_id = 'SHIP-LEG'"
                ).fetchone()
            )
            # 第一项的事务内重算覆盖已回滚：仍是旧口径误归因 seg-2
            row = conn.execute(
                """SELECT attributed_segment_id FROM anomalies
                   WHERE batch_id = ? AND seq = 0""",
                (legacy_id,),
            ).fetchone()
        self.assertEqual(row[0], "seg-2")

        # 单独组单成功：同一事务内重算修正旧口径行并写入联单
        ok = waybill_body("SHIP-LEG-OK", [
            item(legacy_id, digest, "CASE-WLEG", 1000),
        ])
        r2 = self.client.post("/api/v1/shipments/waybills", json=ok)
        self.assertEqual(r2.status_code, 200, r2.text)
        self.assertEqual(r2.json()["result"], "released")
        self.assertEqual(r2.json()["batches"][0]["anomaly_count"], 1)
        with sqlite3.connect(_TMP_DB) as conn:
            row = conn.execute(
                """SELECT attributed_segment_id, sample_counts_json
                   FROM anomalies WHERE batch_id = ?""",
                (legacy_id,),
            ).fetchone()
        self.assertEqual(row[0], "seg-1")
        self.assertEqual(json.loads(row[1]), {"seg-1": 3, "seg-2": 1})

    # ---------- 持久化 ----------

    def test_15_waybill_persisted(self):
        with sqlite3.connect(_TMP_DB) as conn:
            row = conn.execute(
                "SELECT result FROM waybills WHERE shipment_id = 'SHIP-H'"
            ).fetchone()
            self.assertEqual(row[0], "held")
            rows = conn.execute(
                """SELECT position, batch_id, case_id, anomaly_count,
                          case_status
                   FROM waybill_batches
                   WHERE shipment_id = 'SHIP-H' ORDER BY position"""
            ).fetchall()
        self.assertEqual(
            [tuple(r) for r in rows],
            [
                (0, "W-001", "CASE-W1", 2, "closed"),
                (1, "W-002", "CASE-W2", 1, "open"),
            ],
        )

    # ---------- 工具 ----------

    def _seed_legacy_batch(self, batch_id):
        """写一批旧口径数据：异常行误归因 seg-2、sample_counts_json 为空。"""
        payload = {
            "batch_id": batch_id,
            "device_id": "reefer-01",
            "gap_seconds": 600,
            "dip_tolerance_seconds": 120,
            "samples": [
                {"timestamp": iso(990), "temperature": 9.0},
                {"timestamp": iso(995), "temperature": 9.0},
                {"timestamp": iso(999), "temperature": 9.0},
                {"timestamp": iso(1100), "temperature": 9.0},
            ],
            "segments": [
                {"segment_id": "seg-1", "segment_type": "loading",
                 "start": iso(0), "end": iso(1000),
                 "lower_limit": 0.0, "upper_limit": 8.0},
                {"segment_id": "seg-2", "segment_type": "transport",
                 "start": iso(1000), "end": iso(1200),
                 "lower_limit": 0.0, "upper_limit": 8.0},
            ],
        }
        from app.main import _canonical_hash
        from app.models import AnalysisRequest

        request_hash = _canonical_hash(
            AnalysisRequest.model_validate(payload)
        )
        request_json = json.dumps(payload, ensure_ascii=False, sort_keys=True)
        with sqlite3.connect(_TMP_DB) as conn:
            conn.execute(
                """INSERT INTO batches
                   (batch_id, device_id, request_hash, request_json,
                    anomaly_count, created_at)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (batch_id, payload["device_id"], request_hash,
                 request_json, 1, time.time()),
            )
            conn.execute(
                """INSERT INTO anomalies
                   (batch_id, seq, start_ts, end_ts, peak_temperature, peak_ts,
                    direction, peak_segment_id, attributed_segment_id,
                    attributed_segment_type, duration_seconds, coverage_json,
                    sample_counts_json)
                   VALUES (?, 0, 990, 1100, 9.0, 990, 'high', 'seg-1', 'seg-2',
                           'transport', 110.0,
                           '{"seg-2": 100.0, "seg-1": 10.0}', '{}')""",
                (batch_id,),
            )
        return payload


if __name__ == "__main__":
    unittest.main()

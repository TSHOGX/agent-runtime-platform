import hashlib
import hmac
import json


def canonical_manifest_json(payload):
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def manifest_digest(payload):
    return hashlib.sha256(canonical_manifest_json(payload)).hexdigest()


def load_control_manifest(path):
    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)

    if not isinstance(raw, dict) or "payload" not in raw or "digest" not in raw:
        raise SystemExit("control manifest must be {payload,digest}")
    payload = raw["payload"]
    if not isinstance(payload, dict):
        raise SystemExit("control manifest payload must be an object")

    computed = manifest_digest(payload)
    if not hmac.compare_digest(computed, str(raw["digest"])):
        raise SystemExit("control manifest digest mismatch")
    return payload

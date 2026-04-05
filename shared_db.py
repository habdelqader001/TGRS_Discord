# shared_db.py — Firestore bootstrap, guild config, and shared Firestore helpers.
# Imported by Official_Records_Officer, VVIP_Sync_Bot, and master_bot.

import os
import json
import base64
from typing import Optional

import discord
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore

from dotenv import load_dotenv

# ---------------- OPTIONAL KEEP-ALIVE ----------------
if os.getenv("RENDER") or os.getenv("PORT"):
    try:
        from keep_alive import keep_alive
    except Exception as e:
        print(f"[BOOT] keep_alive import failed: {e}", flush=True)
        keep_alive = None
else:
    keep_alive = None


def log(msg: str):
    print(msg, flush=True)


log("[BOOT] Loading environment...")
load_dotenv()

if keep_alive:
    try:
        keep_alive()
        log("[BOOT] keep_alive started.")
    except Exception as e:
        log(f"[BOOT] keep_alive failed: {e}")


# ---------------- CONFIG ----------------
def _int_env(name: str, default: int) -> int:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    return int(raw)


GUILD_ID = int(os.getenv("GUILD_ID", "1411337568691421234"))
# Stats feed (webhook) channel — must match where reports arrive.
STATS_CHANNEL_ID = _int_env("STATS_CHANNEL_ID", 1470111152183709826)
# Mission info / AAR repost destination (e.g. #mission-information-aar).
REPOST_CHANNEL_ID = _int_env("REPOST_CHANNEL_ID", 1467110703012774021)

# If 1/true: still repost to REPOST_CHANNEL_ID when player lines fail to parse (no stats update).
_repost_raw = os.getenv("REPOST_WITHOUT_PARSED_PLAYERS", "").strip().lower()
REPOST_WITHOUT_PARSED_PLAYERS = _repost_raw in ("1", "true", "yes", "on")

WEBHOOK_ID_ALLOWED = 1467513629791490121
CHIEF_DEV_ROLE_ID = 1467855407065071637
UNIT_HEAD_ROLE_ID = 1470995983746990122

_vvip = os.getenv("VVIP_ROLE_ID", "").strip()
VVIP_ROLE_ID: Optional[int] = int(_vvip) if _vvip else None

KILL_TIERS = [
    (25, 1473038917946183803),
    (50, 1473038228381761701),
    (100, 1473038671006400592),
    (200, 1473039071671746660),
    (500, 1474140405187481670),
    (1000, 1477067909544284181),
    (2000, 1482061899175563426),
]
ALL_KILL_ROLE_IDS = [rid for _, rid in KILL_TIERS]

OP_TIERS = [
    (5, 1474156917717864624),
    (10, 1474157240289329376),
    (25, 1474157331603263691),
    (50, 1474157578240921671),
    (100, 1477068112812572733),
    (200, 1482062054977437737),
]
ALL_OP_ROLE_IDS = [rid for _, rid in OP_TIERS]

USERS_COL = "users"
LINKS_COL = "links"
DISCORD_LINKS_COL = "discord_links"
AAR_ORIGINALS_COL = "aar_originals"

log(
    f"[Config] GUILD_ID={GUILD_ID} STATS_CHANNEL_ID={STATS_CHANNEL_ID} "
    f"REPOST_CHANNEL_ID={REPOST_CHANNEL_ID} repost_without_players={REPOST_WITHOUT_PARSED_PLAYERS}"
)


def _load_service_account_info() -> dict:
    raw = os.getenv("FIREBASE_SERVICE_ACCOUNT_JSON")
    b64 = os.getenv("FIREBASE_SERVICE_ACCOUNT_B64")

    if b64 and b64.strip():
        log("[Firestore] Using FIREBASE_SERVICE_ACCOUNT_B64")
        decoded = base64.b64decode(b64.strip().encode("utf-8")).decode("utf-8")
        info = json.loads(decoded)
        if "private_key" in info and isinstance(info["private_key"], str):
            info["private_key"] = info["private_key"].replace("\\n", "\n")
        return info

    if raw and raw.strip():
        log("[Firestore] Using FIREBASE_SERVICE_ACCOUNT_JSON")
        info = json.loads(raw.strip())
        if "private_key" in info and isinstance(info["private_key"], str):
            info["private_key"] = info["private_key"].replace("\\n", "\n")
        return info

    gac = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if gac and os.path.exists(gac):
        log(f"[Firestore] Using GOOGLE_APPLICATION_CREDENTIALS={gac}")
        with open(gac, "r", encoding="utf-8") as f:
            info = json.load(f)
        if "private_key" in info and isinstance(info["private_key"], str):
            info["private_key"] = info["private_key"].replace("\\n", "\n")
        return info

    raise RuntimeError(
        "Missing Firebase credentials. Set FIREBASE_SERVICE_ACCOUNT_B64 (recommended) "
        "or FIREBASE_SERVICE_ACCOUNT_JSON or GOOGLE_APPLICATION_CREDENTIALS."
    )


def init_firestore():
    info = _load_service_account_info()
    cred = credentials.Certificate(info)

    if not firebase_admin._apps:
        firebase_admin.initialize_app(cred)

    client = firestore.client()

    client.collection("meta").document("bootstrap").set(
        {
            "bootstrappedAt": firestore.SERVER_TIMESTAMP,
            "note": "If you can see this, Firestore writes work.",
            "updatedAt": firestore.SERVER_TIMESTAMP,
        },
        merge=True,
    )

    pid = info.get("project_id", "(unknown)")
    log(f"[Firestore] Initialized. project_id={pid}")
    return client


log("[BOOT] Initializing Firestore...")
db = init_firestore()


def get_user_doc(uid: str):
    return db.collection(USERS_COL).document(uid)


def get_op_doc(uid: str, op_key: str):
    safe_key = op_key.replace("/", "_")
    return db.collection(USERS_COL).document(uid).collection("ops").document(safe_key)


def get_discord_display_name(user: discord.abc.User) -> str:
    return getattr(user, "display_name", None) or user.name


def link_uid_to_discord(uid: str, discord_user):
    discord_user_id = str(discord_user.id)
    discord_display_name = get_discord_display_name(discord_user)

    db.collection(LINKS_COL).document(uid).set(
        {
            "discordUserId": discord_user_id,
            "discordDisplayName": discord_display_name,
            "updatedAt": firestore.SERVER_TIMESTAMP,
        },
        merge=True,
    )

    db.collection(DISCORD_LINKS_COL).document(discord_user_id).set(
        {
            "uid": uid,
            "discordDisplayName": discord_display_name,
            "updatedAt": firestore.SERVER_TIMESTAMP,
        },
        merge=True,
    )

    log(f"[Link] uid={uid} discordUserId={discord_user_id} displayName={discord_display_name}")


def refresh_linked_discord_name(discord_user):
    discord_user_id = str(discord_user.id)
    new_name = get_discord_display_name(discord_user)

    reverse_doc = db.collection(DISCORD_LINKS_COL).document(discord_user_id).get()
    if not reverse_doc.exists:
        return None, None

    reverse_data = reverse_doc.to_dict() or {}
    uid = reverse_data.get("uid")
    if not uid:
        return None, None

    db.collection(DISCORD_LINKS_COL).document(discord_user_id).set(
        {
            "discordDisplayName": new_name,
            "updatedAt": firestore.SERVER_TIMESTAMP,
        },
        merge=True,
    )

    db.collection(LINKS_COL).document(uid).set(
        {
            "discordDisplayName": new_name,
            "updatedAt": firestore.SERVER_TIMESTAMP,
        },
        merge=True,
    )

    log(f"[LinkRefresh] uid={uid} discordUserId={discord_user_id} newName={new_name}")
    return uid, new_name


def lookup_uid_by_discord(discord_user_id: str):
    doc = db.collection(DISCORD_LINKS_COL).document(discord_user_id).get()
    if doc.exists:
        data = doc.to_dict() or {}
        return data.get("uid")
    return None


def lookup_discord_by_uid(uid: str):
    doc = db.collection(LINKS_COL).document(uid).get()
    if doc.exists:
        data = doc.to_dict() or {}
        return data.get("discordUserId")
    return None


def apply_report_to_uid(uid: str, ai_kills: int, op_key: str):
    user_ref = get_user_doc(uid)
    op_ref = get_op_doc(uid, op_key)

    @firestore.transactional
    def _txn(txn: firestore.Transaction):
        user_snap = user_ref.get(transaction=txn)
        op_snap = op_ref.get(transaction=txn)

        current = user_snap.to_dict() if user_snap.exists else {}
        cur_kills = int(current.get("kills", 0))
        cur_ops = int(current.get("operations", 0))

        new_kills = cur_kills + int(ai_kills)
        ops_inc = False
        new_ops = cur_ops

        if not op_snap.exists:
            ops_inc = True
            new_ops = cur_ops + 1
            txn.set(op_ref, {"seenAt": firestore.SERVER_TIMESTAMP}, merge=True)

        txn.set(
            user_ref,
            {
                "kills": new_kills,
                "operations": new_ops,
                "updatedAt": firestore.SERVER_TIMESTAMP,
            },
            merge=True,
        )

        return new_kills, new_ops, ops_inc

    txn = db.transaction()
    return _txn(txn)

# Official_Records_Officer.py
# Firestore-backed Discord bot for Arma Reforger stats.
#
# Features:
# - /link uid:<UID>  links a Bohemia/Arma UID to the Discord user
# - Watches STATS_CHANNEL_ID for webhook reports (content or embeds)
# - Parses player lines, increments lifetime AI kills
# - Tracks "operations participated" with dedupe per operation (per UID)
# - Assigns kill-tier + operation-tier roles (exclusive within each tier group)
# - Reposts the processed report to REPOST_CHANNEL_ID as-is
# - On startup: initializes Firestore "meta/bootstrap" doc so DB is not empty
#
# Install:
#   pip install -U discord.py Flask firebase-admin python-dotenv
#
# Discord:
# - Invite bot with scopes: bot + applications.commands
# - Enable intents in Dev Portal: Server Members + Message Content
# - Bot needs "Manage Roles" and its top role must be ABOVE the tier roles
#
# Render:
# - Provide DISCORD_BOT_TOKEN
# - Provide FIREBASE_SERVICE_ACCOUNT_JSON (or FIREBASE_SERVICE_ACCOUNT_B64)
# - keep_alive binds to PORT env var

import os
import re
import json
import base64
from datetime import datetime, timezone

import discord
from discord import app_commands
from discord.ext import commands

from dotenv import load_dotenv
from keep_alive import keep_alive

import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore

# ---------------- BOOTSTRAP ----------------
load_dotenv()
keep_alive()

# ---------------- CONFIG ----------------
DISCORD_TOKEN = os.getenv("DISCORD_BOT_TOKEN")
if not DISCORD_TOKEN:
    raise RuntimeError("Set DISCORD_BOT_TOKEN env var.")

GUILD_ID = 1411337568691421234
STATS_CHANNEL_ID = 1470111152183709826
REPOST_CHANNEL_ID = 1467110703012774021

WEBHOOK_ID_ALLOWED = None  # set to webhook id int if you want to restrict

# ---------------- ROLE TIERS ----------------
# Kill tiers (exclusive among themselves)
KILL_TIERS = [
    (25,  1473038917946183803),
    (50,  1473038228381761701),
    (100, 1473038671006400592),
    (200, 1473039071671746660),
    (500, 1474140405187481670),
    (1000, 1477067909544284181),
]
ALL_KILL_ROLE_IDS = [rid for _, rid in KILL_TIERS]

# Operation tiers (exclusive among themselves)
OP_TIERS = [
    (5,  1474156917717864624),
    (10, 1474157240289329376),
    (25, 1474157331603263691),
    (50, 1474157578240921671),
    (100, 1477068112812572733),
]
ALL_OP_ROLE_IDS = [rid for _, rid in OP_TIERS]

# ---------------- FIRESTORE INIT ----------------
import base64, json, os

def _load_service_account_info() -> dict:
    raw = os.getenv("FIREBASE_SERVICE_ACCOUNT_JSON")
    b64 = os.getenv("FIREBASE_SERVICE_ACCOUNT_B64")

    # Prefer B64 (and ignore empties)
    if b64 and b64.strip():
        decoded = base64.b64decode(b64.strip().encode("utf-8")).decode("utf-8")
        info = json.loads(decoded)
        if "private_key" in info and isinstance(info["private_key"], str):
            info["private_key"] = info["private_key"].replace("\\n", "\n")
        return info

    # Only use raw JSON if it's non-empty
    if raw and raw.strip():
        info = json.loads(raw.strip())
        if "private_key" in info and isinstance(info["private_key"], str):
            info["private_key"] = info["private_key"].replace("\\n", "\n")
        return info

    gac = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if gac and os.path.exists(gac):
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

    db = firestore.client()

    # Bootstrap doc (so you can SEE the DB isn't empty)
    db.collection("meta").document("bootstrap").set(
        {
            "bootstrappedAt": firestore.SERVER_TIMESTAMP,
            "note": "If you can see this, Firestore writes work.",
        },
        merge=True,
    )

    # Log project id (helps catch "writing to the wrong project" mistakes)
    pid = info.get("project_id", "(unknown)")
    print(f"[Firestore] Initialized. project_id={pid}")
    return db

db = init_firestore()

# Collections:
# users/{uid} -> { kills: int, operations: int, updatedAt: ts }
# users/{uid}/ops/{opKey} -> { seenAt: ts }  (dedupe)
# links/{uid} -> { discordUserId: str, discordDisplayName: str, updatedAt: ts }
# discord_links/{discordUserId} -> { uid: str, discordDisplayName: str, updatedAt: ts }

USERS_COL = "users"
LINKS_COL = "links"
DISCORD_LINKS_COL = "discord_links"

# ---------------- TEXT EXTRACTION ----------------
def get_report_text(message: discord.Message) -> str:
    parts = []
    if message.content:
        parts.append(message.content)

    for e in message.embeds:
        if e.description:
            parts.append(e.description)
        for f in getattr(e, "fields", []):
            if f.name:
                parts.append(str(f.name))
            if f.value:
                parts.append(str(f.value))

    return "\n".join(parts).strip()

# ---------------- PARSING ----------------
_PIPE = r"[|│]"

UUID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)

# ---- NEW: sanitize repost (remove UID segments) ----
_UUID_BODY = UUID_RE.pattern[1:-1]  # remove ^ $
_UID_SEGMENT_RE = re.compile(
    rf"\s*{_PIPE}\s*(?:UID|BohemiaID)\s*:\s*\{{?\s*{_UUID_BODY}\s*\}}?\s*",
    re.IGNORECASE,
)

def sanitize_report_for_repost(text: str) -> str:
    if not text:
        return text
    t = text.replace("\r\n", "\n").replace("\r", "\n")

    # Remove "| UID: <uuid>" (or BohemiaID) segments
    t = _UID_SEGMENT_RE.sub(" ", t)

    # Normalize double pipes and spacing around pipes
    t = re.sub(r"\s*\|\s*\|\s*", " | ", t)
    t = re.sub(r"\s*\|\s*", " | ", t)

    # Clean excessive spaces
    t = re.sub(r"[ \t]{2,}", " ", t)

    # Also handle unicode pipe variant spacing
    t = re.sub(r"\s*│\s*", " | ", t)

    return t.strip()

def parse_operation_key(report_text: str, message: discord.Message) -> str:
    """
    Tries: YYYY-MM-DD|HH:MM:SSZ|Scenario
    If parsing fails, uses unique Discord message id so ops still count reliably.
    """
    t = report_text.replace("\r\n", "\n").replace("\r", "\n")

    date_match = re.search(
        r"Date:\s*([0-9]{4}-[0-9]{2}-[0-9]{2})\s*" + _PIPE + r"\s*Time:\s*([0-9]{2}:[0-9]{2}:[0-9]{2}Z)",
        t,
        re.IGNORECASE,
    )
    scen_match = re.search(r"Scenario Name:\s*(.+)", t, re.IGNORECASE)

    if date_match and scen_match:
        date_str = date_match.group(1).strip()
        time_str = date_match.group(2).strip()
        scenario = scen_match.group(1).strip()
        return f"{date_str}|{time_str}|{scenario}"

    return f"msg:{message.id}"

def parse_players_from_report(report_text: str):
    """
    Supports:
      Players:
      Name | UID: ... | AI Kills: ...
    and markdown:
      **Players:**
      - Name | UID: ... | AI Kills: ...
    """
    t = report_text.replace("\r\n", "\n").replace("\r", "\n")
    lines = [ln.strip() for ln in t.split("\n") if ln.strip()]

    start = -1
    for i, ln in enumerate(lines):
        clean = ln.replace("*", "").strip()
        if clean.lower().startswith("players"):
            start = i
            break
    if start == -1:
        return []

    players = []
    for ln in lines[start + 1:]:
        clean = ln.replace("*", "").strip()
        if clean.lower().startswith("objectives"):
            break

        ln2 = re.sub(r"^\s*-\s*", "", ln)  # remove "- " bullets

        m = re.match(
            rf"^(.+?)\s*{_PIPE}\s*(?:UID|BohemiaID)\s*:\s*([A-Za-z0-9\-_:.\{{\}}]+)\s*{_PIPE}\s*AI Kills\s*:\s*(\d+)\b",
            ln2,
            re.IGNORECASE,
        )
        if not m:
            continue
        uid = m.group(2).strip().strip("{}")
        if not UUID_RE.fullmatch(uid):
            continue

        players.append({
            "name": m.group(1).strip(),
            "uid": uid,
            "aiKills": int(m.group(3)),
        })

    return players

# ---------------- ROLE HELPERS ----------------
def tier_for_value(value: int, tiers):
    best = None
    for min_v, role_id in sorted(tiers, key=lambda x: x[0]):
        if value >= min_v:
            best = (min_v, role_id)
    return best

async def apply_exclusive_tier_role(guild, member, value, tiers, all_role_ids, reason):
    chosen = tier_for_value(value, tiers)
    if not chosen:
        return

    _, target_role_id = chosen
    target_role = guild.get_role(target_role_id)
    if target_role is None:
        return

    roles_to_remove = [guild.get_role(rid) for rid in all_role_ids if rid != target_role_id]
    roles_to_remove = [r for r in roles_to_remove if r is not None and r in member.roles]

    try:
        if roles_to_remove:
            await member.remove_roles(*roles_to_remove, reason=reason)
        if target_role not in member.roles:
            await member.add_roles(target_role, reason=reason)
    except (discord.Forbidden, discord.HTTPException):
        return

# ---------------- FIRESTORE DATA OPS ----------------
def get_user_doc(uid: str):
    return db.collection(USERS_COL).document(uid)

def get_op_doc(uid: str, op_key: str):
    # op_key contains '|' which is allowed in Firestore doc IDs, but to be safe you can replace it.
    safe_key = op_key.replace("/", "_")
    return db.collection(USERS_COL).document(uid).collection("ops").document(safe_key)

def link_uid_to_discord(uid: str, discord_user_id: str, discord_display_name: str):
    # Forward
    db.collection(LINKS_COL).document(uid).set(
        {
            "discordUserId": discord_user_id,
            "discordDisplayName": discord_display_name,
            "updatedAt": firestore.SERVER_TIMESTAMP
        },
        merge=True,
    )
    # Reverse
    db.collection(DISCORD_LINKS_COL).document(discord_user_id).set(
        {
            "uid": uid,
            "discordDisplayName": discord_display_name,
            "updatedAt": firestore.SERVER_TIMESTAMP
        },
        merge=True,
    )

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

def update_display_name_by_discord(discord_user_id: str, new_display_name: str):
    rev = db.collection(DISCORD_LINKS_COL).document(discord_user_id).get()
    if not rev.exists:
        return False

    uid = (rev.to_dict() or {}).get("uid")
    if not uid:
        return False

    db.collection(DISCORD_LINKS_COL).document(discord_user_id).set(
        {"discordDisplayName": new_display_name, "updatedAt": firestore.SERVER_TIMESTAMP},
        merge=True,
    )
    db.collection(LINKS_COL).document(uid).set(
        {"discordDisplayName": new_display_name, "updatedAt": firestore.SERVER_TIMESTAMP},
        merge=True,
    )
    return True

def apply_report_to_uid(uid: str, ai_kills: int, op_key: str):
    """
    Transaction:
    - If users/{uid}/ops/{op_key} doesn't exist -> create it and increment operations by 1
    - Always increment kills by ai_kills
    Returns: (new_kills_total, new_ops_total, ops_incremented_bool)
    """
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
            {"kills": new_kills, "operations": new_ops, "updatedAt": firestore.SERVER_TIMESTAMP},
            merge=True,
        )

        return new_kills, new_ops, ops_inc

    txn = db.transaction()
    return _txn(txn)

# ---------------- DISCORD BOT ----------------
intents = discord.Intents.default()
intents.guilds = True
intents.members = True
intents.messages = True
intents.message_content = True

bot = commands.Bot(command_prefix="!", intents=intents)

@bot.event
async def setup_hook():
    guild_obj = discord.Object(id=GUILD_ID)
    bot.tree.copy_global_to(guild=guild_obj)
    synced = await bot.tree.sync(guild=guild_obj)
    print(f"Synced {len(synced)} commands to guild {GUILD_ID}")

@bot.event
async def on_ready():
    print(f"Logged in as {bot.user} (ID: {bot.user.id})")
@bot.tree.command(
    name="admin_refresh_names",
    description="ADMIN: Refresh Discord display names for all linked users in Firestore.",
    guild=discord.Object(id=GUILD_ID),
)
@app_commands.checks.has_permissions(administrator=True)
async def admin_refresh_names_cmd(interaction: discord.Interaction):
    await interaction.response.send_message(
        "Starting refresh… this may take a bit depending on how many links you have.",
        ephemeral=True,
    )

    guild = bot.get_guild(GUILD_ID)
    if guild is None:
        await interaction.followup.send("Guild not found.", ephemeral=True)
        return

    updated = 0
    missing_member = 0
    missing_uid = 0
    errors = 0

    try:
        docs = db.collection(DISCORD_LINKS_COL).stream()
        for doc in docs:
            discord_user_id = doc.id
            data = doc.to_dict() or {}
            uid = data.get("uid")
            if not uid:
                missing_uid += 1
                continue

            # fetch member
            member = guild.get_member(int(discord_user_id))
            if member is None:
                try:
                    member = await guild.fetch_member(int(discord_user_id))
                except discord.NotFound:
                    missing_member += 1
                    continue
                except Exception:
                    errors += 1
                    continue

            display_name = member.display_name

            try:
                # update both forward + reverse docs
                db.collection(DISCORD_LINKS_COL).document(discord_user_id).set(
                    {"discordDisplayName": display_name, "updatedAt": firestore.SERVER_TIMESTAMP},
                    merge=True,
                )
                db.collection(LINKS_COL).document(uid).set(
                    {"discordDisplayName": display_name, "updatedAt": firestore.SERVER_TIMESTAMP},
                    merge=True,
                )
                updated += 1
            except Exception:
                errors += 1

    except Exception as e:
        await interaction.followup.send(f"Failed while streaming Firestore docs: {e}", ephemeral=True)
        return

    await interaction.followup.send(
        f"Done.\n"
        f"✅ Updated: {updated}\n"
        f"⚠️ Missing members: {missing_member}\n"
        f"⚠️ Missing uid in discord_links: {missing_uid}\n"
        f"❌ Errors: {errors}",
        ephemeral=True,
    )
@admin_refresh_names_cmd.error
async def admin_refresh_names_cmd_error(interaction: discord.Interaction, error: Exception):
    if isinstance(error, app_commands.MissingPermissions):
        await interaction.response.send_message("You must be an Administrator to use this command.", ephemeral=True)
    else:
        try:
            await interaction.response.send_message(f"Error: {error}", ephemeral=True)
        except discord.InteractionResponded:
            await interaction.followup.send(f"Error: {error}", ephemeral=True)
@bot.tree.command(
    name="link",
    description="Link your UID (from the stats report) to your Discord account for role rewards.",
    guild=discord.Object(id=GUILD_ID),
)
@app_commands.describe(uid="Your UID exactly as shown in the stats report (copy/paste).")
async def link_cmd(interaction: discord.Interaction, uid: str):
    uid = uid.strip()

    if not UUID_RE.fullmatch(uid):
        await interaction.response.send_message(
            "Invalid UID format. It must be 36 chars in 8-4-4-4-12 form (example: 123e4567-e89b-12d3-a456-426614174000).",
            ephemeral=True,
        )
        return

    link_uid_to_discord(uid, str(interaction.user.id), interaction.user.display_name)

    # Ensure user doc exists (initialize)
    get_user_doc(uid).set(
        {"kills": firestore.Increment(0), "operations": firestore.Increment(0), "updatedAt": firestore.SERVER_TIMESTAMP},
        merge=True,
    )

    await interaction.response.send_message(
        f"Linked UID `{uid}` to your Discord account.",
        ephemeral=True,
    )

@bot.tree.command(
    name="update_name",
    description="Update your linked Discord display name in Firestore.",
    guild=discord.Object(id=GUILD_ID),
)
async def update_name_cmd(interaction: discord.Interaction):
    ok = update_display_name_by_discord(str(interaction.user.id), interaction.user.display_name)
    if not ok:
        await interaction.response.send_message(
            "You are not linked yet. Use `/link uid:<your-uid>` first.",
            ephemeral=True,
        )
        return

    await interaction.response.send_message(
        f"Updated your stored display name to **{interaction.user.display_name}**.",
        ephemeral=True,
    )

@bot.tree.command(
    name="stats",
    description="Show your lifetime AI kills and operations participated in.",
    guild=discord.Object(id=GUILD_ID),
)
async def stats_cmd(interaction: discord.Interaction):
    discord_id = str(interaction.user.id)
    uid = lookup_uid_by_discord(discord_id)

    if not uid:
        await interaction.response.send_message(
            "You are not linked yet. Use `/link uid:<your-uid>` first.",
            ephemeral=True,
        )
        return

    snap = get_user_doc(uid).get()
    data = snap.to_dict() if snap.exists else {}
    kills = int(data.get("kills", 0))
    ops = int(data.get("operations", 0))

    await interaction.response.send_message(
        f"UID `{uid}`\nAI Kills: **{kills}**\nOperations: **{ops}**",
        ephemeral=True,
    )

@bot.event
async def on_message(message: discord.Message):
    await bot.process_commands(message)

    if message.author == bot.user:
        return

    # avoid repost loops
    if message.channel.id == REPOST_CHANNEL_ID:
        return

    if message.channel.id != STATS_CHANNEL_ID:
        return

    if WEBHOOK_ID_ALLOWED is not None and message.webhook_id != WEBHOOK_ID_ALLOWED:
        return

    report_text = get_report_text(message)
    if not report_text:
        return

    players = parse_players_from_report(report_text)
    if not players:
        return

    op_key = parse_operation_key(report_text, message)
    print("op_key =", op_key)

    guild = bot.get_guild(GUILD_ID)
    if guild is None:
        return

    # 1) Update Firestore totals + ops for ALL players
    #    (transaction + per-uid per-op dedupe)
    updated = []  # (uid, new_kills, new_ops, ops_inc)
    for p in players:
        uid = p["uid"]
        ai_kills = p["aiKills"]
        try:
            new_kills, new_ops, ops_inc = apply_report_to_uid(uid, ai_kills, op_key)
            print(f"uid={uid} kills+={ai_kills} ops_inc={ops_inc} ops_now={new_ops}")
            updated.append((uid, new_kills, new_ops))
        except Exception as e:
            print(f"[Firestore] Failed updating uid={uid}: {e}")

    # 2) Assign roles ONLY for linked users
    for uid, new_kills, new_ops in updated:
        discord_user_id = lookup_discord_by_uid(uid)
        if not discord_user_id:
            continue

        member = guild.get_member(int(discord_user_id))
        if member is None:
            try:
                member = await guild.fetch_member(int(discord_user_id))
            except discord.NotFound:
                continue

        await apply_exclusive_tier_role(
            guild=guild,
            member=member,
            value=int(new_kills),
            tiers=KILL_TIERS,
            all_role_ids=ALL_KILL_ROLE_IDS,
            reason="AI kills tier update",
        )

        await apply_exclusive_tier_role(
            guild=guild,
            member=member,
            value=int(new_ops),
            tiers=OP_TIERS,
            all_role_ids=ALL_OP_ROLE_IDS,
            reason="Operations tier update",
        )

    # 3) Repost the report as-is (BUT remove UID from player lines)
    try:
        dest = bot.get_channel(REPOST_CHANNEL_ID) or await bot.fetch_channel(REPOST_CHANNEL_ID)

        if message.embeds:
            for e in message.embeds:
                ed = e.to_dict()
                new_e = discord.Embed.from_dict(ed)

                if new_e.description:
                    new_e.description = sanitize_report_for_repost(new_e.description)

                # sanitize fields (some webhooks put the Players list in fields)
                if getattr(new_e, "fields", None):
                    old_fields = list(new_e.fields)
                    new_e.clear_fields()
                    for f in old_fields:
                        fname = sanitize_report_for_repost(f.name) if f.name else f.name
                        fval = sanitize_report_for_repost(f.value) if f.value else f.value
                        new_e.add_field(name=fname, value=fval, inline=f.inline)

                await dest.send(embed=new_e)
        else:
            await dest.send(sanitize_report_for_repost(report_text))

    except (discord.Forbidden, discord.HTTPException) as e:
        print(f"[Repost] Failed: {e}")

bot.run(DISCORD_TOKEN)
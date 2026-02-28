# Official_Records_Officer.py
# Firestore-backed Discord bot for Arma Reforger stats.
# Adds:
# - Better Render/runtime diagnostics
# - Safer startup logging
# - Discord login retry loop
# - Repost sanitization (removes UID from reposted player lines)
# - Discord display name storage + refresh command

import os
import re
import json
import base64
import asyncio
import traceback
from datetime import datetime, timezone

import discord
from discord import app_commands
from discord.ext import commands

from dotenv import load_dotenv

import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore

# ---------------- OPTIONAL KEEP-ALIVE ----------------
# Only import/start Flask keep_alive in hosting environments that actually need it.
if os.getenv("RENDER") or os.getenv("PORT"):
    try:
        from keep_alive import keep_alive
    except Exception as e:
        print(f"[BOOT] keep_alive import failed: {e}", flush=True)
        keep_alive = None
else:
    keep_alive = None


# ---------------- LOGGING ----------------
def log(msg: str):
    print(msg, flush=True)


# ---------------- BOOTSTRAP ----------------
log("[BOOT] Loading environment...")
load_dotenv()

if keep_alive:
    try:
        keep_alive()
        log("[BOOT] keep_alive started.")
    except Exception as e:
        log(f"[BOOT] keep_alive failed: {e}")


# ---------------- CONFIG ----------------
DISCORD_TOKEN = os.getenv("DISCORD_BOT_TOKEN")
if not DISCORD_TOKEN:
    raise RuntimeError("Set DISCORD_BOT_TOKEN env var.")

GUILD_ID = 1411337568691421234
STATS_CHANNEL_ID = 1470111152183709826
REPOST_CHANNEL_ID = 1467110703012774021

WEBHOOK_ID_ALLOWED = None  # set to webhook id int if you want to restrict

# ---------------- ROLE TIERS ----------------
KILL_TIERS = [
    (25,  1473038917946183803),
    (50,  1473038228381761701),
    (100, 1473038671006400592),
    (200, 1473039071671746660),
    (500, 1474140405187481670),
]
ALL_KILL_ROLE_IDS = [rid for _, rid in KILL_TIERS]

OP_TIERS = [
    (5,  1474156917717864624),
    (10, 1474157240289329376),
    (25, 1474157331603263691),
    (50, 1474157578240921671),
]
ALL_OP_ROLE_IDS = [rid for _, rid in OP_TIERS]

# ---------------- FIRESTORE INIT ----------------
UUID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)

_PIPE = r"[|│]"


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

    db = firestore.client()

    db.collection("meta").document("bootstrap").set(
        {
            "bootstrappedAt": firestore.SERVER_TIMESTAMP,
            "note": "If you can see this, Firestore writes work.",
            "updatedAt": firestore.SERVER_TIMESTAMP,
        },
        merge=True,
    )

    pid = info.get("project_id", "(unknown)")
    log(f"[Firestore] Initialized. project_id={pid}")
    return db


log("[BOOT] Initializing Firestore...")
db = init_firestore()

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


# ---------------- REPOST SANITIZER ----------------
_UUID_BODY = UUID_RE.pattern[1:-1]
_UID_SEGMENT_RE = re.compile(
    rf"\s*{_PIPE}\s*(?:UID|BohemiaID)\s*:\s*\{{?\s*{_UUID_BODY}\s*\}}?\s*",
    re.IGNORECASE,
)

def sanitize_report_for_repost(text: str) -> str:
    if not text:
        return text

    t = text.replace("\r\n", "\n").replace("\r", "\n")
    t = _UID_SEGMENT_RE.sub(" ", t)
    t = re.sub(r"\s*\|\s*\|\s*", " | ", t)
    t = re.sub(r"\s*\|\s*", " | ", t)
    t = re.sub(r"\s*│\s*", " | ", t)
    t = re.sub(r"[ \t]{2,}", " ", t)
    return t.strip()


# ---------------- PARSING ----------------
def parse_operation_key(report_text: str, message: discord.Message) -> str:
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

        ln2 = re.sub(r"^\s*-\s*", "", ln)

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
        log(f"[Roles] Target role not found: {target_role_id}")
        return

    roles_to_remove = [guild.get_role(rid) for rid in all_role_ids if rid != target_role_id]
    roles_to_remove = [r for r in roles_to_remove if r is not None and r in member.roles]

    try:
        if roles_to_remove:
            await member.remove_roles(*roles_to_remove, reason=reason)
        if target_role not in member.roles:
            await member.add_roles(target_role, reason=reason)
    except (discord.Forbidden, discord.HTTPException) as e:
        log(f"[Roles] Failed for member={member.id}: {e}")


# ---------------- FIRESTORE OPS ----------------
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


# ---------------- DISCORD BOT ----------------
intents = discord.Intents.default()
intents.guilds = True
intents.members = True
intents.messages = True
intents.message_content = True

bot = commands.Bot(command_prefix="!", intents=intents)


@bot.event
async def setup_hook():
    log("[Discord] setup_hook starting...")
    guild_obj = discord.Object(id=GUILD_ID)
    synced = await bot.tree.sync(guild=guild_obj)
    log(f"[Discord] Synced {len(synced)} commands to guild {GUILD_ID}")


@bot.event
async def on_ready():
    log(f"[Discord] Logged in as {bot.user} (ID: {bot.user.id})")

    guild = bot.get_guild(GUILD_ID)
    if guild:
        log(f"[Discord] Guild found: {guild.name} ({guild.id})")
    else:
        log(f"[Discord] Guild NOT found in cache: {GUILD_ID}")

    stats_channel = bot.get_channel(STATS_CHANNEL_ID)
    repost_channel = bot.get_channel(REPOST_CHANNEL_ID)
    log(f"[Discord] Stats channel cache lookup: {stats_channel}")
    log(f"[Discord] Repost channel cache lookup: {repost_channel}")


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
            "Invalid UID format. It must be 36 chars in 8-4-4-4-12 form.",
            ephemeral=True,
        )
        return

    link_uid_to_discord(uid, interaction.user)

    get_user_doc(uid).set(
        {
            "kills": firestore.Increment(0),
            "operations": firestore.Increment(0),
            "updatedAt": firestore.SERVER_TIMESTAMP,
        },
        merge=True,
    )

    await interaction.response.send_message(
        f"Linked UID `{uid}` to your Discord account.",
        ephemeral=True,
    )


@bot.tree.command(
    name="refreshname",
    description="Refresh your saved Discord display name in Firestore.",
    guild=discord.Object(id=GUILD_ID),
)
async def refreshname_cmd(interaction: discord.Interaction):
    uid, new_name = refresh_linked_discord_name(interaction.user)
    if not uid:
        await interaction.response.send_message(
            "You are not linked yet. Use `/link uid:<your-uid>` first.",
            ephemeral=True,
        )
        return

    await interaction.response.send_message(
        f"Updated linked display name to **{new_name}** for UID `{uid}`.",
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


@bot.tree.command(
    name="admin_refresh_all_names",
    description="Admin: refresh linked Discord display names for all linked members in this server.",
    guild=discord.Object(id=GUILD_ID),
)
@app_commands.checks.has_permissions(administrator=True)
async def admin_refresh_all_names_cmd(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)

    refreshed = 0
    skipped = 0

    guild = interaction.guild
    if guild is None:
        await interaction.followup.send("This command must be used inside the server.", ephemeral=True)
        return

    docs = db.collection(DISCORD_LINKS_COL).stream()
    for doc in docs:
        data = doc.to_dict() or {}
        discord_user_id = doc.id
        uid = data.get("uid")
        if not uid:
            skipped += 1
            continue

        member = guild.get_member(int(discord_user_id))
        if member is None:
            try:
                member = await guild.fetch_member(int(discord_user_id))
            except Exception:
                skipped += 1
                continue

        display_name = get_discord_display_name(member)

        db.collection(DISCORD_LINKS_COL).document(discord_user_id).set(
            {
                "discordDisplayName": display_name,
                "updatedAt": firestore.SERVER_TIMESTAMP,
            },
            merge=True,
        )
        db.collection(LINKS_COL).document(uid).set(
            {
                "discordDisplayName": display_name,
                "updatedAt": firestore.SERVER_TIMESTAMP,
            },
            merge=True,
        )
        refreshed += 1

    await interaction.followup.send(
        f"Refreshed **{refreshed}** linked display names. Skipped **{skipped}**.",
        ephemeral=True,
    )


@admin_refresh_all_names_cmd.error
async def admin_refresh_all_names_error(interaction: discord.Interaction, error):
    if isinstance(error, app_commands.errors.MissingPermissions):
        await interaction.response.send_message("You need administrator permission to use this command.", ephemeral=True)
    else:
        raise error


@bot.event
async def on_message(message: discord.Message):
    await bot.process_commands(message)

    if message.author == bot.user:
        return

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
        log("[Report] No players parsed from report.")
        return

    op_key = parse_operation_key(report_text, message)
    log(f"[Report] op_key={op_key} parsed_players={len(players)}")

    guild = bot.get_guild(GUILD_ID)
    if guild is None:
        log(f"[Discord] Guild not found during report handling: {GUILD_ID}")
        return

    updated = []
    for p in players:
        uid = p["uid"]
        ai_kills = p["aiKills"]
        try:
            new_kills, new_ops, ops_inc = apply_report_to_uid(uid, ai_kills, op_key)
            log(f"[Firestore] uid={uid} kills+={ai_kills} ops_inc={ops_inc} ops_now={new_ops}")
            updated.append((uid, new_kills, new_ops))
        except Exception as e:
            log(f"[Firestore] Failed updating uid={uid}: {e}")
            log(traceback.format_exc())

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
            except Exception as e:
                log(f"[Discord] Failed fetching member {discord_user_id}: {e}")
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

    try:
        dest = bot.get_channel(REPOST_CHANNEL_ID)
        if dest is None:
            dest = await bot.fetch_channel(REPOST_CHANNEL_ID)

        if message.embeds:
            for e in message.embeds:
                ed = e.to_dict()
                new_e = discord.Embed.from_dict(ed)

                if new_e.description:
                    new_e.description = sanitize_report_for_repost(new_e.description)

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

        log("[Repost] Report reposted successfully.")
    except (discord.Forbidden, discord.HTTPException) as e:
        log(f"[Repost] Failed: {e}")


# ---------------- STARTUP / RETRY ----------------
async def run_bot_with_retry():
    max_attempts = 5
    delay_seconds = 15

    for attempt in range(1, max_attempts + 1):
        try:
            log(f"[BOOT] Discord start attempt {attempt}/{max_attempts} ...")
            await bot.start(DISCORD_TOKEN)
            return
        except discord.HTTPException as e:
            log(f"[BOOT] Discord HTTPException on startup: {e}")
            log(traceback.format_exc())

            if attempt == max_attempts:
                raise

            log(f"[BOOT] Retrying in {delay_seconds} seconds...")
            await asyncio.sleep(delay_seconds)

        except Exception as e:
            log(f"[BOOT] Fatal startup exception: {e}")
            log(traceback.format_exc())
            raise


def main():
    log("[BOOT] Starting Official Records Officer...")
    try:
        asyncio.run(run_bot_with_retry())
    except KeyboardInterrupt:
        log("[BOOT] Shutdown requested.")
    except Exception as e:
        log(f"[BOOT] Process exiting due to exception: {e}")
        log(traceback.format_exc())
        raise


if __name__ == "__main__":
    main()
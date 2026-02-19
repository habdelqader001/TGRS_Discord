# Official_Records_Officer.py
# - Kill tiers + Operation tiers
# - Tracks lifetime AI kills + operations participated (deduped per report)
# - Reposts processed report to another channel

import os
import re
import json
from pathlib import Path

import discord
from discord import app_commands
from discord.ext import commands

from dotenv import load_dotenv
from keep_alive import keep_alive

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

# ---------------- STORAGE ----------------
BASE_DIR = Path(__file__).resolve().parent
LINKS_PATH = BASE_DIR / "links.json"         # uid -> discordUserId
TOTALS_PATH = BASE_DIR / "totals.json"       # uid -> totalKills
OPS_PATH = BASE_DIR / "operations.json"      # uid -> operationsCount
SEEN_OPS_PATH = BASE_DIR / "seen_ops.json"   # uid -> [opKey, ...]


def load_json(path: Path, default):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def save_json(path: Path, data):
    """
    Simple write (less moving parts). If you prefer atomic swap, keep your atomic writer,
    but this is easier to troubleshoot.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")


links = load_json(LINKS_PATH, {})
totals = load_json(TOTALS_PATH, {})
operations = load_json(OPS_PATH, {})
seen_ops = load_json(SEEN_OPS_PATH, {})

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


def parse_operation_key(report_text: str, message: discord.Message) -> str:
    """
    Tries to build: YYYY-MM-DD|HH:MM:SSZ|Scenario
    If parsing fails, fall back to unique message.id so operations still count reliably.
    """
    t = report_text.replace("\r\n", "\n").replace("\r", "\n")

    # tolerate emojis, markdown, extra spaces
    date_match = re.search(
        r"Date:\s*([0-9]{4}-[0-9]{2}-[0-9]{2})\s*"+_PIPE+r"\s*Time:\s*([0-9]{2}:[0-9]{2}:[0-9]{2}Z)",
        t,
        re.IGNORECASE,
    )
    scen_match = re.search(r"Scenario Name:\s*(.+)", t, re.IGNORECASE)

    if date_match and scen_match:
        date_str = date_match.group(1).strip()
        time_str = date_match.group(2).strip()
        scenario = scen_match.group(1).strip()
        return f"{date_str}|{time_str}|{scenario}"

    # Fallback: dedupe per Discord message (unique)
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

        ln2 = re.sub(r"^\s*-\s*", "", ln)  # remove markdown bullets

        m = re.match(
            rf"^(.+?)\s*{_PIPE}\s*(?:UID|BohemiaID)\s*:\s*([A-Za-z0-9\-_:.\{{\}}]+)\s*{_PIPE}\s*AI Kills\s*:\s*(\d+)\b",
            ln2,
            re.IGNORECASE,
        )
        if not m:
            continue

        players.append({
            "name": m.group(1).strip(),
            "uid": m.group(2).strip().strip("{}"),
            "aiKills": int(m.group(3)),
        })

    return players

# ---------------- LOGIC ----------------
def ensure_user_buckets(uid: str):
    if uid not in totals:
        totals[uid] = 0
    if uid not in operations:
        operations[uid] = 0
    if uid not in seen_ops:
        seen_ops[uid] = []


def increment_operations_for_player(uid: str, op_key: str) -> bool:
    """
    Dedupes per uid per op_key. Returns True if incremented.
    """
    ensure_user_buckets(uid)

    existing = seen_ops.get(uid, [])
    if op_key in existing:
        return False

    existing.append(op_key)
    seen_ops[uid] = existing
    operations[uid] = int(operations.get(uid, 0)) + 1
    return True


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

# ---------------- BOT ----------------
intents = discord.Intents.default()
intents.guilds = True
intents.members = True
intents.messages = True
intents.message_content = True  # must be enabled in Dev Portal too

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
    name="link",
    description="Link your UID (from the stats report) to your Discord account for role rewards.",
    guild=discord.Object(id=GUILD_ID),
)
@app_commands.describe(uid="Your UID exactly as shown in the stats report (copy/paste).")
async def link_cmd(interaction: discord.Interaction, uid: str):
    uid = uid.strip()

    if not re.fullmatch(r"[A-Za-z0-9\-\_\:\.]{6,128}", uid):
        await interaction.response.send_message(
            "That UID format looks invalid. Please copy/paste it exactly from the report.",
            ephemeral=True,
        )
        return

    links[uid] = str(interaction.user.id)
    save_json(LINKS_PATH, links)

    ensure_user_buckets(uid)
    save_json(TOTALS_PATH, totals)
    save_json(OPS_PATH, operations)
    save_json(SEEN_OPS_PATH, seen_ops)

    await interaction.response.send_message(
        f"Linked UID `{uid}` to your Discord account.",
        ephemeral=True,
    )


@bot.tree.command(
    name="stats",
    description="Show your lifetime AI kills and operations participated in.",
    guild=discord.Object(id=GUILD_ID),
)
async def stats_cmd(interaction: discord.Interaction):
    discord_id = str(interaction.user.id)

    uid = None
    for k, v in links.items():
        if v == discord_id:
            uid = k
            break

    if not uid:
        await interaction.response.send_message(
            "You are not linked yet. Use `/link uid:<your-uid>` first.",
            ephemeral=True,
        )
        return

    k = int(totals.get(uid, 0))
    ops = int(operations.get(uid, 0))
    await interaction.response.send_message(
        f"UID `{uid}`\nAI Kills: **{k}**\nOperations: **{ops}**",
        ephemeral=True,
    )


@bot.event
async def on_message(message: discord.Message):
    await bot.process_commands(message)

    if message.author == bot.user:
        return

    # Avoid repost-loop
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

    # Update totals + operations for ALL participants (linked or not)
    for p in players:
        uid = p["uid"]
        ai_kills = p["aiKills"]

        ensure_user_buckets(uid)
        totals[uid] = int(totals.get(uid, 0)) + ai_kills

        did_inc = increment_operations_for_player(uid, op_key)
        print(f"uid={uid} kills+={ai_kills} ops_inc={did_inc} ops_now={operations.get(uid)}")

    # Always save after processing a valid report
    save_json(TOTALS_PATH, totals)
    save_json(OPS_PATH, operations)
    save_json(SEEN_OPS_PATH, seen_ops)

    # Assign roles only for linked users
    for p in players:
        uid = p["uid"]
        discord_user_id = links.get(uid)
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
            value=int(totals.get(uid, 0)),
            tiers=KILL_TIERS,
            all_role_ids=ALL_KILL_ROLE_IDS,
            reason="AI kills tier update",
        )

        await apply_exclusive_tier_role(
            guild=guild,
            member=member,
            value=int(operations.get(uid, 0)),
            tiers=OP_TIERS,
            all_role_ids=ALL_OP_ROLE_IDS,
            reason="Operations tier update",
        )

    # Repost the report as-is
    try:
        dest = bot.get_channel(REPOST_CHANNEL_ID) or await bot.fetch_channel(REPOST_CHANNEL_ID)
        if message.embeds:
            for e in message.embeds:
                await dest.send(embed=e)
        else:
            await dest.send(report_text)
    except (discord.Forbidden, discord.HTTPException):
        pass


bot.run(DISCORD_TOKEN)

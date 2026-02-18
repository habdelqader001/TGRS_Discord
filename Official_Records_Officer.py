import os
import re
import json
from pathlib import Path
import discord
from discord import app_commands

# ---------------- CONFIG ----------------
DISCORD_TOKEN = os.getenv("DISCORD_BOT_TOKEN")

GUILD_ID = 123456789012345678          # your server ID (int)
STATS_CHANNEL_ID = 123456789012345678  # your stats channel ID (int)

# Optional: only process messages posted by this webhook id
WEBHOOK_ID_ALLOWED = None  # e.g. 123456789012345678

# Tier roles (min kills -> role id)
TIERS = [
    (0,   111111111111111111),  # Recruit
    (50,  222222222222222222),  # Hunter
    (200, 333333333333333333),  # Veteran
    (500, 444444444444444444),  # Elite
]
ALL_TIER_ROLE_IDS = [role_id for _, role_id in TIERS]

# Storage files
BASE_DIR = Path(__file__).resolve().parent
LINKS_PATH = BASE_DIR / "links.json"   # steamId -> discordUserId
TOTALS_PATH = BASE_DIR / "totals.json" # steamId -> totalKills


# ---------------- STORAGE HELPERS ----------------
def load_json(path: Path, default):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default

def save_json(path: Path, data):
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")

links = load_json(LINKS_PATH, {})   # {"7656119...": "123456..."}
totals = load_json(TOTALS_PATH, {}) # {"7656119...": 123}


# ---------------- PARSING ----------------
def parse_players_from_report(text: str):
    """
    Expects a Players block like:

    Players:
    Mr Macca's | SteamID: 76561198000000000 | AI Kills: 11 | Longest AI Kill: 126m | Deaths: 9
    ...

    Returns list of dicts: {name, steamId, aiKills}
    """
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    try:
        start = next(i for i, ln in enumerate(lines) if ln.lower() == "players:")
    except StopIteration:
        return []

    players = []
    # stop when Objectives Completed: starts
    for ln in lines[start + 1:]:
        if ln.lower().startswith("objectives"):
            break

        m = re.match(r"^(.+?)\s*\|\s*SteamID:\s*(\d+)\s*\|\s*AI Kills:\s*(\d+)\b", ln, re.IGNORECASE)
        if not m:
            continue

        name = m.group(1).strip()
        steam_id = m.group(2).strip()
        ai_kills = int(m.group(3))

        players.append({"name": name, "steamId": steam_id, "aiKills": ai_kills})

    return players


def tier_for_kills(kills: int):
    # choose highest min <= kills
    best = None
    for min_k, role_id in sorted(TIERS, key=lambda x: x[0]):
        if kills >= min_k:
            best = (min_k, role_id)
    return best  # (min_k, role_id)


# ---------------- DISCORD CLIENT ----------------
intents = discord.Intents.default()
intents.guilds = True
intents.members = True
intents.messages = True
intents.message_content = True  # needed to read msg.content

client = discord.Client(intents=intents)
tree = app_commands.CommandTree(client)


@tree.command(name="link", description="Link your SteamID64 to your Discord account for role rewards.")
@app_commands.describe(steamid="Your SteamID64 (numbers only, e.g., 7656119...)")
async def link_cmd(interaction: discord.Interaction, steamid: str):
    steamid = steamid.strip()

    if not re.fullmatch(r"\d{16,20}", steamid):
        await interaction.response.send_message(
            "That doesn’t look like a SteamID64 (numbers only).",
            ephemeral=True
        )
        return

    links[steamid] = str(interaction.user.id)
    save_json(LINKS_PATH, links)

    if steamid not in totals:
        totals[steamid] = 0
        save_json(TOTALS_PATH, totals)

    await interaction.response.send_message(
        f"Linked SteamID `{steamid}` to your Discord account.",
        ephemeral=True
    )


async def apply_tier_role(guild: discord.Guild, member: discord.Member, kills: int):
    chosen = tier_for_kills(kills)
    if not chosen:
        return

    _, target_role_id = chosen
    target_role = guild.get_role(target_role_id)
    if target_role is None:
        return

    # remove other tier roles
    roles_to_remove = [guild.get_role(rid) for rid in ALL_TIER_ROLE_IDS if rid != target_role_id]
    roles_to_remove = [r for r in roles_to_remove if r is not None and r in member.roles]

    if roles_to_remove:
        try:
            await member.remove_roles(*roles_to_remove, reason="AI kills tier update")
        except discord.Forbidden:
            # bot lacks permissions / role hierarchy issue
            return

    # add target role
    if target_role not in member.roles:
        try:
            await member.add_roles(target_role, reason="AI kills tier update")
        except discord.Forbidden:
            return


@client.event
async def on_ready():
    # sync commands to your guild (fast for development)
    guild = discord.Object(id=GUILD_ID)
    await tree.sync(guild=guild)
    print(f"Logged in as {client.user} and synced commands.")


@client.event
async def on_message(message: discord.Message):
    if message.author == client.user:
        return

    if message.channel.id != STATS_CHANNEL_ID:
        return

    # Optional: only accept your webhook posts
    if WEBHOOK_ID_ALLOWED is not None:
        if message.webhook_id != WEBHOOK_ID_ALLOWED:
            return

    players = parse_players_from_report(message.content)
    if not players:
        return

    guild = client.get_guild(GUILD_ID)
    if guild is None:
        return

    for p in players:
        steam_id = p["steamId"]
        ai_kills = p["aiKills"]

        # update lifetime totals
        totals[steam_id] = int(totals.get(steam_id, 0)) + ai_kills
        save_json(TOTALS_PATH, totals)

        discord_user_id = links.get(steam_id)
        if not discord_user_id:
            # not linked yet
            continue

        member = guild.get_member(int(discord_user_id))
        if member is None:
            try:
                member = await guild.fetch_member(int(discord_user_id))
            except discord.NotFound:
                continue

        await apply_tier_role(guild, member, totals[steam_id])


if not DISCORD_TOKEN:
    raise RuntimeError("Set DISCORD_BOT_TOKEN env var.")

client.run(DISCORD_TOKEN)

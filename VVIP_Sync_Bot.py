# VVIP_Sync_Bot.py
# VVIP role → Firestore links/{uid}.VVIP. Loaded by master_bot as VVIPCog.
# Requires shared_db.VVIP_ROLE_ID (set VVIP_ROLE_ID in the environment).

import asyncio
import traceback
from typing import Optional

import discord
from discord.ext import commands, tasks

from firebase_admin import firestore

import shared_db
from shared_db import GUILD_ID, LINKS_COL, VVIP_ROLE_ID, db, log, lookup_uid_by_discord


def set_vvip_in_links(uid: str, value: int, reason: str):
    db.collection(LINKS_COL).document(uid).set(
        {
            "VVIP": int(value),
            "updatedAt": firestore.SERVER_TIMESTAMP,
        },
        merge=True,
    )
    log(f"[VVIP] links/{uid} VVIP={value} ({reason})")


async def _get_member(guild: discord.Guild, discord_user_id: str) -> Optional[discord.Member]:
    mid = int(discord_user_id)
    m = guild.get_member(mid)
    if m is not None:
        return m
    try:
        return await guild.fetch_member(mid)
    except discord.NotFound:
        return None
    except discord.HTTPException as e:
        log(f"[VVIP][Daily] fetch_member({discord_user_id}) failed: {e}")
        return None


class VVIPCog(commands.Cog):
    """Sync VVIP Discord role to Firestore links.VVIP and daily reconciliation."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot

    async def reconcile_vvip_roles(self):
        if VVIP_ROLE_ID is None:
            return

        guild = self.bot.get_guild(GUILD_ID)
        if guild is None:
            log(f"[VVIP][Daily] Guild not in cache: {GUILD_ID}")
            return

        role = guild.get_role(VVIP_ROLE_ID)
        if role is None:
            log(f"[VVIP][Daily] Role not found: {VVIP_ROLE_ID}")
            return

        try:
            await guild.chunk()
        except (discord.HTTPException, asyncio.TimeoutError) as e:
            log(f"[VVIP][Daily] guild.chunk() failed (continuing): {e}")

        cleared = 0
        for doc in db.collection(LINKS_COL).where("VVIP", "==", 1).stream():
            data = doc.to_dict() or {}
            did = data.get("discordUserId")
            uid = doc.id
            if not did:
                continue

            member = await _get_member(guild, did)
            if member is None:
                set_vvip_in_links(uid, 0, "daily reconcile (member not in guild)")
                cleared += 1
                continue
            if role not in member.roles:
                set_vvip_in_links(uid, 0, "daily reconcile (role no longer present)")
                cleared += 1

        set_to_one = 0
        for member in role.members:
            uid = lookup_uid_by_discord(str(member.id))
            if not uid:
                continue
            snap = db.collection(LINKS_COL).document(uid).get()
            if not snap.exists:
                continue
            cur = (snap.to_dict() or {}).get("VVIP")
            if cur != 1:
                set_vvip_in_links(uid, 1, "daily reconcile (has role, DB out of sync)")
                set_to_one += 1

        log(
            f"[VVIP][Daily] Done. role.members={len(role.members)} "
            f"cleared={cleared} set_to_1={set_to_one}"
        )

    @tasks.loop(hours=24)
    async def daily_vvip_task(self):
        log("[VVIP][Daily] Starting reconciliation...")
        try:
            await self.reconcile_vvip_roles()
        except Exception as e:
            log(f"[VVIP][Daily] Failed: {e}")
            log(traceback.format_exc())

    @daily_vvip_task.before_loop
    async def before_daily_vvip(self):
        await self.bot.wait_until_ready()

    @commands.Cog.listener()
    async def on_ready(self):
        if VVIP_ROLE_ID is None:
            return

        guild = self.bot.get_guild(GUILD_ID)
        if guild:
            log(f"[VVIP] Guild: {guild.name} ({guild.id})")
            role = guild.get_role(VVIP_ROLE_ID)
            if role:
                log(f"[VVIP] VVIP role resolved: {role.name!r} ({role.id})")
            else:
                log(f"[VVIP] WARNING: VVIP_ROLE_ID {VVIP_ROLE_ID} not found in guild.")
            try:
                await guild.chunk()
                log(f"[VVIP] Member cache loaded ({guild.member_count} members).")
            except (discord.HTTPException, asyncio.TimeoutError) as e:
                log(f"[VVIP] guild.chunk() on_ready: {e}")
        else:
            log(f"[VVIP] WARNING: Guild {GUILD_ID} not found.")

        if not self.daily_vvip_task.is_running():
            self.daily_vvip_task.start()

    @commands.Cog.listener()
    async def on_member_update(self, before: discord.Member, after: discord.Member):
        if VVIP_ROLE_ID is None:
            return

        if before.guild.id != GUILD_ID:
            return

        role = after.guild.get_role(VVIP_ROLE_ID)
        if role is None:
            return

        had = role in before.roles
        has = role in after.roles
        if had == has:
            return

        uid = lookup_uid_by_discord(str(after.id))
        if not uid:
            log(f"[VVIP] Role change for unlinked member {after.id}; no links/ doc update.")
            return

        if has:
            set_vvip_in_links(uid, 1, "role assigned")
        else:
            set_vvip_in_links(uid, 0, "role removed")


if __name__ == "__main__":
    import master_bot

    master_bot.main()

# master_bot.py — Single process: Official Records (stats/reposts) + optional VVIP sync.
# Run: python master_bot.py
# Env: DISCORD_BOT_TOKEN, Firebase creds, optional VVIP_ROLE_ID

import asyncio
import os
import traceback

import discord
from discord.ext import commands

import shared_db
from Official_Records_Officer import OfficialRecordsCog
from VVIP_Sync_Bot import VVIPCog

DISCORD_TOKEN = os.getenv("DISCORD_BOT_TOKEN")
if not DISCORD_TOKEN:
    raise RuntimeError("Set DISCORD_BOT_TOKEN env var.")

intents = discord.Intents.default()
intents.guilds = True
intents.members = True
intents.messages = True
intents.message_content = True

bot = commands.Bot(command_prefix="!", intents=intents)


@bot.event
async def setup_hook():
    shared_db.log("[Discord] setup_hook starting...")
    await bot.add_cog(OfficialRecordsCog(bot))
    if shared_db.VVIP_ROLE_ID is not None:
        await bot.add_cog(VVIPCog(bot))
        shared_db.log("[Discord] VVIP cog loaded.")
    else:
        shared_db.log("[Discord] VVIP_ROLE_ID not set; VVIP sync disabled.")

    guild_obj = discord.Object(id=shared_db.GUILD_ID)
    synced = await bot.tree.sync(guild=guild_obj)
    shared_db.log(f"[Discord] Synced {len(synced)} commands to guild {shared_db.GUILD_ID}")


async def run_bot_with_retry():
    max_attempts = 5
    delay_seconds = 15

    for attempt in range(1, max_attempts + 1):
        try:
            shared_db.log(f"[BOOT] Discord start attempt {attempt}/{max_attempts} ...")
            await bot.start(DISCORD_TOKEN)
            return
        except discord.HTTPException as e:
            shared_db.log(f"[BOOT] Discord HTTPException on startup: {e}")
            shared_db.log(traceback.format_exc())

            if attempt == max_attempts:
                raise

            shared_db.log(f"[BOOT] Retrying in {delay_seconds} seconds...")
            await asyncio.sleep(delay_seconds)

        except Exception as e:
            shared_db.log(f"[BOOT] Fatal startup exception: {e}")
            shared_db.log(traceback.format_exc())
            raise


def main():
    shared_db.log("[BOOT] Starting master bot (Official Records + optional VVIP)...")
    try:
        asyncio.run(run_bot_with_retry())
    except KeyboardInterrupt:
        shared_db.log("[BOOT] Shutdown requested.")
    except Exception as e:
        shared_db.log(f"[BOOT] Process exiting due to exception: {e}")
        shared_db.log(traceback.format_exc())
        raise


if __name__ == "__main__":
    main()

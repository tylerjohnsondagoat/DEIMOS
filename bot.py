"""
DEIMOS - Community Spambot Killer
Discord bot that lets server members collectively mute and purge spambots.
"""

import os
import json
import asyncio
import logging
from collections import deque
from datetime import datetime, timedelta, timezone

import discord
from discord import app_commands
from discord.ext import commands, tasks
import aiomysql

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("deimos")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
CONFIRM_THRESHOLD = 3       # Total votes needed (initiator + 2 confirmers)
KILL_WINDOW_SECONDS = 240   # How long confirmers have to vote (4 min)
PURGE_MINUTES = 15          # Delete target's messages from last N minutes
MUTE_DAYS = 14              # Discord timeout duration
COOLDOWN_SECONDS = 300      # 5 min cooldown per initiator
MODCHAT_CHANNEL_ID = int(os.environ["MODCHAT_CHANNEL_ID"])  # Mod report channel (set in Railway env)

# Comma-separated user IDs allowed to use dev commands (set DEV_USER_IDS in Railway env, e.g. "123,456")
DEV_USER_IDS: set[int] = {
    int(uid) for uid in os.environ.get("DEV_USER_IDS", "").split(",") if uid.strip().isdigit()
}

# How much of the target's flagged content to forward to modchat before purging.
# "first" = only the earliest message in the purge window (current behavior).
# To switch to forwarding ALL collected messages, change this to "all".
# See execute_kill() where FORWARD_MODE is read.
FORWARD_MODE = "first"
MAX_FORWARDED_MESSAGES = 20  # Hard cap when FORWARD_MODE = "all" to avoid flooding modchat

# --- Auto-trap: hard-rate carpet-bomb detection -----------------------------
# A user who posts into TRAP_CHANNELS distinct channels within TRAP_WINDOW_SECONDS
# is auto-muted + purged (no human can hit 5 different channels in 12s). Counts
# threads and voice-channel text chats, not just top-level text channels.
TRAP_CHANNELS = 5
TRAP_WINDOW_SECONDS = 12
# Armed by default. Set DEIMOS_AUTOTRAP_ENABLED=0 in Railway env to disable instantly.
AUTOTRAP_ENABLED = os.environ.get("DEIMOS_AUTOTRAP_ENABLED", "1").strip().lower() not in (
    "0", "false", "no", "off", "",
)


def defang_mentions(text: str) -> str:
    """Neutralize mass-ping tokens so forwarding spambot content into modchat can't ping the room.

    Wraps @everyone / @here in backticks (Discord renders them as inline code, which never pings).
    Belt-and-suspenders only: every modchat.send() that carries this also passes
    allowed_mentions=discord.AllowedMentions.none(), so even a missed token can't notify.
    """
    if not text:
        return text
    return text.replace("@everyone", "`@everyone`").replace("@here", "`@here`")

# ---------------------------------------------------------------------------
# Bot setup
# ---------------------------------------------------------------------------
intents = discord.Intents.default()
intents.message_content = True
intents.members = True
intents.presences = True

bot = commands.Bot(command_prefix="!", intents=intents)

# In-memory state
# A "kill" is now logical: one (guild, target) vote that can span multiple channels.
# Its kill_id is the message_id of its FIRST panel (also the DEIMOS_kills DB row key).
pending_kills: dict[int, dict] = {}   # kill_id -> kill data
panel_index: dict[int, int] = {}      # any panel message_id -> kill_id (reverse lookup for button clicks)
cooldowns: dict[int, datetime] = {}   # user_id -> last kill time
processed_kills: set[int] = set()     # kill_ids already resolved (dedup)

# Auto-trap state
spam_windows: dict[int, deque] = {}   # user_id -> deque[(created_at, channel_id)]
trapped_users: set[int] = set()       # user_ids already auto-trapped (debounce)


def iter_message_channels(guild: discord.Guild):
    """Every channel that can hold messages: text channels, voice-channel text
    chats, and threads (active). Used for both /kill purges and auto-trap purges."""
    seen: set[int] = set()
    for ch in guild.text_channels:
        seen.add(ch.id)
        yield ch
    for ch in guild.voice_channels:  # VoiceChannel supports .history() for text-in-voice
        if ch.id not in seen:
            seen.add(ch.id)
            yield ch
    for th in guild.threads:         # active threads (includes forum posts)
        if th.id not in seen:
            seen.add(th.id)
            yield th

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------
db_pool: aiomysql.Pool = None


async def init_db():
    global db_pool
    db_pool = await aiomysql.create_pool(
        host=os.environ["MYSQL_HOST"],
        port=int(os.environ.get("MYSQL_PORT", 3306)),
        user=os.environ["MYSQL_USER"],
        password=os.environ["MYSQL_PASSWORD"],
        db=os.environ["MYSQL_DATABASE"],
        autocommit=True,
        minsize=1,
        maxsize=5,
    )
    logger.info("Database pool initialized")


async def db_execute(query: str, args=None):
    async with db_pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(query, args)
            return cur


async def db_fetchone(query: str, args=None):
    async with db_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(query, args)
            return await cur.fetchone()


async def db_fetchall(query: str, args=None):
    async with db_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(query, args)
            return await cur.fetchall()


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

async def insert_kill(guild_id, target_id, target_name, initiator_id, initiator_name, message_id, channel_id):
    await db_execute(
        """INSERT INTO DEIMOS_kills
           (guild_id, target_user_id, target_username, initiated_by, initiated_by_username,
            confirmed_by, status, message_id, channel_id)
           VALUES (%s, %s, %s, %s, %s, %s, 'pending', %s, %s)""",
        (guild_id, target_id, target_name, initiator_id, initiator_name,
         json.dumps([initiator_id]), message_id, channel_id),
    )


async def confirm_kill(message_id, confirmed_by_list):
    await db_execute(
        """UPDATE DEIMOS_kills SET status='confirmed', confirmed_by=%s, resolved_at=NOW()
           WHERE message_id=%s AND status='pending'""",
        (json.dumps(confirmed_by_list), message_id),
    )


async def expire_kill(message_id):
    await db_execute(
        "UPDATE DEIMOS_kills SET status='expired', resolved_at=NOW() WHERE message_id=%s AND status='pending'",
        (message_id,),
    )


async def mark_false_positive(guild_id, target_id):
    """Mark the most recent confirmed kill of this user as false positive. Returns confirmed_by list or None."""
    row = await db_fetchone(
        """SELECT id, confirmed_by FROM DEIMOS_kills
           WHERE guild_id=%s AND target_user_id=%s AND status='confirmed'
           ORDER BY resolved_at DESC LIMIT 1""",
        (guild_id, target_id),
    )
    if not row:
        return None
    await db_execute(
        "UPDATE DEIMOS_kills SET status='false_positive' WHERE id=%s",
        (row["id"],),
    )
    return json.loads(row["confirmed_by"]) if row["confirmed_by"] else []


async def add_score(guild_id, user_id, username, correct=0, false_pos=0):
    await db_execute(
        """INSERT INTO DEIMOS_scores (guild_id, user_id, username, correct_kills, false_positives)
           VALUES (%s, %s, %s, %s, %s)
           ON DUPLICATE KEY UPDATE
             username=%s,
             correct_kills=correct_kills+%s,
             false_positives=false_positives+%s""",
        (guild_id, user_id, username, correct, false_pos,
         username, correct, false_pos),
    )


async def get_leaderboard(guild_id, limit=10):
    return await db_fetchall(
        """SELECT user_id, username, correct_kills, false_positives,
                  (correct_kills - false_positives) AS net_score
           FROM DEIMOS_scores
           WHERE guild_id=%s
           ORDER BY net_score DESC, correct_kills DESC
           LIMIT %s""",
        (guild_id, limit),
    )


# ---------------------------------------------------------------------------
# Vote panel: embed + button (shared by every channel a kill spans)
# ---------------------------------------------------------------------------

def build_vote_embed(kill_data: dict) -> discord.Embed:
    """Build the live KILL VOTE embed at the kill's current vote count."""
    total = 1 + len(kill_data["confirmers"])  # initiator + confirmers
    remaining = max(0, CONFIRM_THRESHOLD - total)
    embed = discord.Embed(
        title="KILL VOTE",
        description=f"**Target:** <@{kill_data['target_id']}>\n"
                    f"**Flagged by:** <@{kill_data['initiator_id']}>\n\n"
                    f"**{remaining} more confirmation{'s' if remaining != 1 else ''} needed** "
                    f"within {KILL_WINDOW_SECONDS}s.\n\n"
                    f"Votes count from **any channel**: use `/kill` on the same user "
                    f"or press the button below.\n\n"
                    f"If confirmed: **{MUTE_DAYS}-day mute** + purge last {PURGE_MINUTES} min of messages.",
        color=discord.Color.orange(),
        timestamp=discord.utils.utcnow(),
    )
    embed.set_footer(text=f"Expires in {KILL_WINDOW_SECONDS}s")
    return embed


async def refresh_all_panels(kill_data: dict):
    """Re-render every channel panel for this kill at the current vote count."""
    embed = build_vote_embed(kill_data)
    count = len(kill_data["confirmers"])
    for panel in kill_data["panels"]:
        try:
            msg = await panel["channel"].fetch_message(panel["message_id"])
            await msg.edit(embed=embed, view=ConfirmKillView(count))
        except Exception:
            logger.debug("Could not refresh panel %s", panel["message_id"])


class ConfirmKillButton(discord.ui.Button):
    def __init__(self, confirmer_count: int = 0):
        super().__init__(
            style=discord.ButtonStyle.danger,
            label=f"Confirm Kill ({confirmer_count}/{CONFIRM_THRESHOLD - 1})",
            custom_id="deimos_confirm",
        )

    async def callback(self, interaction: discord.Interaction):
        kill_id = panel_index.get(interaction.message.id)
        kill_data = pending_kills.get(kill_id) if kill_id is not None else None

        if not kill_data:
            await interaction.response.send_message("This kill has already expired or been resolved.", ephemeral=True)
            return

        if kill_id in processed_kills:
            await interaction.response.send_message("This kill has already been confirmed.", ephemeral=True)
            return

        user_id = interaction.user.id

        # Can't confirm your own kill
        if user_id == kill_data["initiator_id"]:
            await interaction.response.send_message("You can't confirm your own kill.", ephemeral=True)
            return

        # Can't confirm if you're the target
        if user_id == kill_data["target_id"]:
            await interaction.response.send_message("Nice try.", ephemeral=True)
            return

        # Can't vote twice
        if user_id in kill_data["confirmers"]:
            await interaction.response.send_message("You already confirmed this kill.", ephemeral=True)
            return

        # Can't be a bot
        if interaction.user.bot:
            return

        kill_data["confirmers"].add(user_id)
        kill_data["confirmer_names"][user_id] = interaction.user.display_name
        total = 1 + len(kill_data["confirmers"])  # initiator + confirmers

        if total < CONFIRM_THRESHOLD:
            # Sync every channel's panel to the new count.
            await interaction.response.defer()
            await refresh_all_panels(kill_data)
            return

        # --- THRESHOLD MET: EXECUTE KILL ---
        processed_kills.add(kill_id)
        await interaction.response.defer()
        await execute_kill(interaction, kill_data)


class ConfirmKillView(discord.ui.View):
    def __init__(self, confirmer_count: int = 0):
        super().__init__(timeout=None)
        self.add_item(ConfirmKillButton(confirmer_count))


# ---------------------------------------------------------------------------
# Kill execution
# ---------------------------------------------------------------------------

async def execute_kill(interaction: discord.Interaction, kill_data: dict):
    guild = interaction.guild
    target_id = kill_data["target_id"]
    msg_id = kill_data["message_id"]

    # Build full participant list
    all_participants = [kill_data["initiator_id"]] + list(kill_data["confirmers"])
    all_names = {kill_data["initiator_id"]: kill_data["initiator_name"]}
    all_names.update(kill_data["confirmer_names"])

    try:
        member = guild.get_member(target_id) or await guild.fetch_member(target_id)
    except discord.NotFound:
        member = None

    muted = False
    purged = 0

    collected_messages: list[discord.Message] = []

    if member:
        # Mute (timeout) for 14 days
        try:
            until = discord.utils.utcnow() + timedelta(days=MUTE_DAYS)
            await member.timeout(until, reason=f"DEIMOS: Kill confirmed by {len(all_participants)} Enforcers")
            muted = True
        except discord.Forbidden:
            logger.warning("Cannot timeout %s - insufficient permissions", target_id)
        except Exception:
            logger.error("Failed to timeout %s", target_id, exc_info=True)

        # Collect messages from last 15 minutes BEFORE deleting them
        cutoff = discord.utils.utcnow() - timedelta(minutes=PURGE_MINUTES)
        for channel in iter_message_channels(guild):
            try:
                async for msg in channel.history(limit=100, after=cutoff):
                    if msg.author.id == target_id:
                        collected_messages.append(msg)
            except (discord.Forbidden, discord.HTTPException):
                continue

        # Delete the collected messages
        for msg in collected_messages:
            try:
                await msg.delete()
                purged += 1
            except (discord.Forbidden, discord.HTTPException, discord.NotFound):
                continue

    # Update DB (DB row is keyed on the kill's first panel = its kill_id)
    await confirm_kill(msg_id, all_participants)

    # Award scores
    for uid in all_participants:
        name = all_names.get(uid, "Unknown")
        await add_score(guild.id, uid, name, correct=1, false_pos=0)

    # Remove from pending + clear panel lookups
    pending_kills.pop(msg_id, None)
    for panel in kill_data["panels"]:
        panel_index.pop(panel["message_id"], None)

    # Update every channel's panel
    confirmer_mentions = " ".join(f"<@{uid}>" for uid in kill_data["confirmers"])
    result_embed = discord.Embed(
        title="KILL CONFIRMED",
        description=f"**Target:** <@{target_id}>\n"
                    f"**Initiated by:** <@{kill_data['initiator_id']}>\n"
                    f"**Confirmed by:** {confirmer_mentions}\n\n"
                    f"**Muted:** {'14 days' if muted else 'Failed (check perms)'}\n"
                    f"**Messages purged:** {purged}",
        color=discord.Color.red(),
        timestamp=discord.utils.utcnow(),
    )
    result_embed.set_footer(text="Use /unkill to reverse if this was a mistake")

    for panel in kill_data["panels"]:
        try:
            msg = await panel["channel"].fetch_message(panel["message_id"])
            await msg.edit(embed=result_embed, view=None)
        except Exception:
            logger.debug("Failed to edit kill panel %s", panel["message_id"])

    # Post report to modchat
    try:
        modchat = bot.get_channel(MODCHAT_CHANNEL_ID)
        if modchat:
            report = discord.Embed(
                title="KILL CONFIRMED - Mod Review",
                description=(
                    f"**Target:** <@{target_id}> (`{target_id}`)\n"
                    f"**Initiated by:** <@{kill_data['initiator_id']}>\n"
                    f"**Confirmed by:** {confirmer_mentions}\n\n"
                    f"**Muted:** {'14 days' if muted else 'FAILED (check perms)'}\n"
                    f"**Messages purged:** {purged}\n\n"
                    f"If this was a real spambot, **ban them**.\n"
                    f"If this was a mistake, use `/unkill @user` to reverse "
                    f"(add `mute:` to also timeout the voters)."
                ),
                color=discord.Color.red(),
                timestamp=discord.utils.utcnow(),
            )
            await modchat.send(embed=report)

            # Forward the flagged content for review.
            # FORWARD_MODE = "first" -> only the earliest message in the window.
            # FORWARD_MODE = "all"   -> every collected message, capped at MAX_FORWARDED_MESSAGES.
            if collected_messages:
                ordered = sorted(collected_messages, key=lambda m: m.created_at)
                if FORWARD_MODE == "all":
                    to_forward = ordered[:MAX_FORWARDED_MESSAGES]
                    header = (
                        f"**Flagged content from <@{target_id}>** "
                        f"({len(ordered)} message{'s' if len(ordered) != 1 else ''}):"
                    )
                else:  # "first" (default)
                    to_forward = ordered[:1]
                    header = (
                        f"**Flagged content from <@{target_id}>** "
                        f"(earliest of {len(ordered)} message{'s' if len(ordered) != 1 else ''}):"
                    )

                await modchat.send(header, allowed_mentions=discord.AllowedMentions.none())
                for msg in to_forward:
                    forwarded = False
                    try:
                        await msg.forward(modchat)
                        forwarded = True
                    except (AttributeError, discord.HTTPException, discord.Forbidden):
                        pass
                    if not forwarded:
                        # Fallback: quote the content manually. This is the real ping risk:
                        # the spambot's raw text lands in a normal message, so an @everyone in
                        # it would ping all of modchat. defang_mentions() backticks it; the
                        # allowed_mentions=none() below is the hard guarantee nothing pings.
                        content = defang_mentions(msg.content or "[no text]")
                        attachments = " ".join(a.url for a in msg.attachments)
                        body = f"> From {msg.channel.mention}:\n> {content[:1800]}"
                        if attachments:
                            body += f"\n{attachments}"
                        try:
                            await modchat.send(body, allowed_mentions=discord.AllowedMentions.none())
                        except discord.HTTPException:
                            pass
                if FORWARD_MODE == "all" and len(ordered) > MAX_FORWARDED_MESSAGES:
                    await modchat.send(f"...+{len(ordered) - MAX_FORWARDED_MESSAGES} more not shown")
    except Exception:
        logger.error("Failed to post kill report to modchat", exc_info=True)

    logger.info("[guild:%s] Kill confirmed on %s by %s (%d messages purged)",
                guild.id, target_id, all_participants, purged)


# ---------------------------------------------------------------------------
# Auto-trap: hard-rate carpet-bomb detection
# ---------------------------------------------------------------------------

class TrapApproveButton(discord.ui.Button):
    def __init__(self, target_id: int):
        super().__init__(
            style=discord.ButtonStyle.danger,
            label="Approve (confirm spam)",
            custom_id=f"deimos_trap_approve:{target_id}",
        )
        self.target_id = target_id

    async def callback(self, interaction: discord.Interaction):
        if not interaction.user.guild_permissions.manage_messages:
            await interaction.response.send_message("Moderators only.", ephemeral=True)
            return
        embed = interaction.message.embeds[0] if interaction.message.embeds else discord.Embed()
        embed.color = discord.Color.dark_red()
        embed.add_field(
            name="Reviewed",
            value=(
                f"Confirmed as spam by {interaction.user.mention}.\n"
                f"**Reminder: the auto-trap only MUTED + purged. You still need to "
                f"BAN <@{self.target_id}> to remove them for good.**"
            ),
            inline=False,
        )
        await interaction.response.edit_message(embed=embed, view=None)


class TrapUnmuteButton(discord.ui.Button):
    def __init__(self, target_id: int):
        super().__init__(
            style=discord.ButtonStyle.success,
            label="Unmute (false positive)",
            custom_id=f"deimos_trap_unmute:{target_id}",
        )
        self.target_id = target_id

    async def callback(self, interaction: discord.Interaction):
        if not interaction.user.guild_permissions.manage_messages:
            await interaction.response.send_message("Moderators only.", ephemeral=True)
            return
        guild = interaction.guild
        # Remove the timeout
        try:
            member = guild.get_member(self.target_id) or await guild.fetch_member(self.target_id)
            await member.timeout(None, reason=f"DEIMOS Auto-Trap reversed by {interaction.user.display_name}")
        except Exception:
            logger.debug("Auto-trap unmute: could not clear timeout on %s", self.target_id)
        # Mark the trap as a false positive and let the user be re-evaluated
        await mark_false_positive(guild.id, self.target_id)
        trapped_users.discard(self.target_id)

        embed = interaction.message.embeds[0] if interaction.message.embeds else discord.Embed()
        embed.color = discord.Color.green()
        embed.add_field(
            name="Reviewed",
            value=f"Reversed by {interaction.user.mention} (false positive). User unmuted.",
            inline=False,
        )
        await interaction.response.edit_message(embed=embed, view=None)


class TrapReviewView(discord.ui.View):
    def __init__(self, target_id: int):
        super().__init__(timeout=None)
        self.add_item(TrapApproveButton(target_id))
        self.add_item(TrapUnmuteButton(target_id))


async def trap_spammer(guild: discord.Guild, member: discord.Member, channel_ids: set[int]):
    """Auto-mute + purge a carpet-bomber, then post a mod-review report with buttons."""
    target_id = member.id

    # 1. Mute (14-day timeout)
    muted = False
    try:
        until = discord.utils.utcnow() + timedelta(days=MUTE_DAYS)
        await member.timeout(until, reason=f"DEIMOS Auto-Trap: carpet-bomb across {len(channel_ids)} channels")
        muted = True
    except discord.Forbidden:
        logger.warning("Auto-trap cannot timeout %s - insufficient permissions", target_id)
    except Exception:
        logger.error("Auto-trap failed to timeout %s", target_id, exc_info=True)

    # 2. Collect + purge their last 15 min across every message-bearing channel
    collected: list[discord.Message] = []
    cutoff = discord.utils.utcnow() - timedelta(minutes=PURGE_MINUTES)
    for ch in iter_message_channels(guild):
        try:
            async for msg in ch.history(limit=100, after=cutoff):
                if msg.author.id == target_id:
                    collected.append(msg)
        except (discord.Forbidden, discord.HTTPException):
            continue
    purged = 0
    for msg in collected:
        try:
            await msg.delete()
            purged += 1
        except (discord.Forbidden, discord.HTTPException, discord.NotFound):
            continue

    logger.info("[guild:%s] AUTO-TRAP on %s (%s): %d channels, muted=%s, purged=%d",
                guild.id, target_id, member.display_name, len(channel_ids), muted, purged)

    # 3. Mod-review report to modchat with Approve / Unmute buttons
    modchat = bot.get_channel(MODCHAT_CHANNEL_ID)
    if not modchat:
        logger.error("Auto-trap: modchat channel %s not found; trap applied without report", MODCHAT_CHANNEL_ID)
        return

    channel_mentions = " ".join(f"<#{cid}>" for cid in channel_ids)
    report = discord.Embed(
        title="AUTO-TRAP: Carpet-bomb detected",
        description=(
            f"**Target:** {member.mention} (`{target_id}`)\n"
            f"**Pattern:** {len(channel_ids)} channels within {TRAP_WINDOW_SECONDS}s "
            f"(faster than any human)\n"
            f"**Channels:** {channel_mentions}\n\n"
            f"**Muted:** {'14 days' if muted else 'FAILED (check perms)'}\n"
            f"**Messages purged:** {purged}\n\n"
            f"Review below. **Approve** confirms spam (you must still ban them); "
            f"**Unmute** reverses a false positive."
        ),
        color=discord.Color.orange(),
        timestamp=discord.utils.utcnow(),
    )
    try:
        report_msg = await modchat.send(embed=report, view=TrapReviewView(target_id))
    except Exception:
        logger.error("Auto-trap: failed to post mod report", exc_info=True)
        return

    # Record in DB as a confirmed auto-trap (keyed on the report message)
    try:
        await insert_kill(guild.id, target_id, member.display_name,
                          bot.user.id, "DEIMOS Auto-Trap", report_msg.id, MODCHAT_CHANNEL_ID)
        await confirm_kill(report_msg.id, [bot.user.id])
    except Exception:
        logger.error("Auto-trap: failed to record kill row", exc_info=True)

    # Forward the earliest flagged message for context
    if collected:
        ordered = sorted(collected, key=lambda m: m.created_at)
        await modchat.send(
            f"**Flagged content from <@{target_id}>** (earliest of {len(ordered)} message"
            f"{'s' if len(ordered) != 1 else ''}):",
            allowed_mentions=discord.AllowedMentions.none(),
        )
        first = ordered[0]
        forwarded = False
        try:
            await first.forward(modchat)
            forwarded = True
        except (AttributeError, discord.HTTPException, discord.Forbidden):
            pass
        if not forwarded:
            content = defang_mentions(first.content or "[no text]")
            attachments = " ".join(a.url for a in first.attachments)
            body = f"> From {first.channel.mention}:\n> {content[:1800]}"
            if attachments:
                body += f"\n{attachments}"
            try:
                await modchat.send(body, allowed_mentions=discord.AllowedMentions.none())
            except discord.HTTPException:
                pass


@bot.event
async def on_message(message: discord.Message):
    if not AUTOTRAP_ENABLED:
        return
    if message.guild is None:
        return

    author = message.author
    if author.bot or message.webhook_id is not None:
        return
    if author.id in trapped_users:
        return

    # Exempt mods/admins (the only humans who might run a legit cross-poster)
    perms = getattr(author, "guild_permissions", None)
    if perms and (perms.manage_messages or perms.administrator):
        return

    uid = author.id
    now = message.created_at  # tz-aware UTC, creation time
    dq = spam_windows.setdefault(uid, deque())
    dq.append((now, message.channel.id))

    cutoff = now - timedelta(seconds=TRAP_WINDOW_SECONDS)
    while dq and dq[0][0] < cutoff:
        dq.popleft()

    distinct = {cid for _, cid in dq}
    if len(distinct) >= TRAP_CHANNELS:
        trapped_users.add(uid)
        spam_windows.pop(uid, None)
        try:
            await trap_spammer(message.guild, author, distinct)
        except Exception:
            logger.error("Auto-trap handler failed for %s", uid, exc_info=True)


# ---------------------------------------------------------------------------
# Slash commands
# ---------------------------------------------------------------------------

@bot.tree.command(name="kill", description="Flag a suspected spambot for community confirmation")
@app_commands.describe(target="The suspected spambot")
async def kill_command(interaction: discord.Interaction, target: discord.Member):
    guild = interaction.guild
    user = interaction.user

    # Safety checks
    if target.bot:
        await interaction.response.send_message("Can't kill bots.", ephemeral=True)
        return

    if target.id == user.id:
        await interaction.response.send_message("Can't kill yourself.", ephemeral=True)
        return

    if target.guild_permissions.manage_messages or target.guild_permissions.administrator:
        await interaction.response.send_message("Can't kill moderators or admins.", ephemeral=True)
        return

    # Already an active kill on this (guild, target)? Join it instead of starting a new one.
    for kill_data in pending_kills.values():
        if kill_data["target_id"] == target.id and kill_data["guild_id"] == guild.id:
            await join_existing_kill(interaction, kill_data, user)
            return

    # --- NEW KILL ---

    # Cooldown check (only gates STARTING a kill; joining an existing one is free)
    last_kill = cooldowns.get(user.id)
    if last_kill:
        elapsed = (datetime.now(timezone.utc) - last_kill).total_seconds()
        if elapsed < COOLDOWN_SECONDS:
            remaining = int(COOLDOWN_SECONDS - elapsed)
            await interaction.response.send_message(
                f"Cooldown active. Try again in {remaining}s.", ephemeral=True
            )
            return

    cooldowns[user.id] = datetime.now(timezone.utc)

    kill_data = {
        "guild_id": guild.id,
        "target_id": target.id,
        "target_name": target.display_name,
        "initiator_id": user.id,
        "initiator_name": user.display_name,
        "confirmers": set(),
        "confirmer_names": {},
        "message_id": None,   # set after send; doubles as kill_id + DB row key
        "panels": [],         # [{"channel": ch, "message_id": mid}, ...]
        "expires_at": datetime.now(timezone.utc) + timedelta(seconds=KILL_WINDOW_SECONDS),
    }

    embed = build_vote_embed(kill_data)
    await interaction.response.send_message(embed=embed, view=ConfirmKillView(0))
    msg = await interaction.original_response()

    kill_data["message_id"] = msg.id
    kill_data["panels"].append({"channel": interaction.channel, "message_id": msg.id})
    pending_kills[msg.id] = kill_data
    panel_index[msg.id] = msg.id

    # Insert into DB
    await insert_kill(guild.id, target.id, target.display_name,
                      user.id, user.display_name, msg.id, interaction.channel_id)

    logger.info("[guild:%s] Kill vote started on %s (%s) by %s (%s)",
                guild.id, target.id, target.display_name, user.id, user.display_name)


async def join_existing_kill(interaction: discord.Interaction, kill_data: dict, user: discord.Member):
    """A /kill on a target that already has a live vote: count the invoker's vote (if new)
    and make sure this channel has a live panel."""
    user_id = user.id
    kill_id = kill_data["message_id"]
    already_voted = user_id == kill_data["initiator_id"] or user_id in kill_data["confirmers"]
    has_panel_here = any(p["channel"].id == interaction.channel_id for p in kill_data["panels"])

    # Count this /kill as a confirmation vote (max flexibility: every /kill is a vote).
    if not already_voted:
        kill_data["confirmers"].add(user_id)
        kill_data["confirmer_names"][user_id] = user.display_name

    total = 1 + len(kill_data["confirmers"])

    if not has_panel_here:
        # Surface a live panel in this channel.
        embed = build_vote_embed(kill_data)
        await interaction.response.send_message(embed=embed, view=ConfirmKillView(len(kill_data["confirmers"])))
        new_msg = await interaction.original_response()
        kill_data["panels"].append({"channel": interaction.channel, "message_id": new_msg.id})
        panel_index[new_msg.id] = kill_id
    else:
        note = "You've already voted on this kill." if already_voted else "Your vote was added."
        await interaction.response.send_message(
            f"{note} The live vote is posted in this channel.", ephemeral=True
        )

    if total >= CONFIRM_THRESHOLD and kill_id not in processed_kills:
        processed_kills.add(kill_id)
        await execute_kill(interaction, kill_data)
    else:
        await refresh_all_panels(kill_data)


@bot.tree.command(name="deimos", description="How DEIMOS works - Enforcer guide")
async def help_command(interaction: discord.Interaction):
    embed = discord.Embed(
        title="DEIMOS - Enforcer System",
        description=(
            "DEIMOS lets server Enforcers collectively neutralize spambots. "
            "No single person can act alone - it takes a group to confirm a kill."
        ),
        color=discord.Color.dark_red(),
    )
    embed.add_field(
        name="How it works",
        value=(
            "1. Spot a spambot? Use `/kill @user` to flag them.\n"
            "2. A **Kill Vote** appears with a confirmation button.\n"
            "3. **2 other server members** must confirm within 4 minutes. "
            "Votes count from **any channel**: pressing the button OR running "
            "`/kill` on the same user both add a vote, so you don't have to "
            "round everyone into one channel.\n"
            "4. If confirmed: the target is **muted for 14 days** and their "
            "messages from the last 15 minutes are **purged**."
        ),
        inline=False,
    )
    embed.add_field(
        name="Rules",
        value=(
            "- You can't kill bots, mods, admins, or yourself\n"
            "- You can't confirm your own kill vote\n"
            "- One active vote per target at a time\n"
            "- 5-minute cooldown between kill votes"
        ),
        inline=False,
    )
    embed.add_field(
        name="Scoring",
        value=(
            "Every confirmed kill earns **+1** for all Enforcers who participated. "
            "If a moderator reverses a kill (`/unkill`), it counts as **-1** and a false positive. "
            "Check the standings with `/killboard`."
        ),
        inline=False,
    )
    embed.add_field(
        name="Mod Review",
        value=(
            "When a kill passes, the target's flagged message is **forwarded to a moderator-only "
            "channel** for review before it gets purged. Mods can confirm legit kills, ban true "
            "spambots, or reverse false positives."
        ),
        inline=False,
    )
    embed.add_field(
        name="Auto-Trap",
        value=(
            f"DEIMOS auto-mutes anyone who carpet-bombs **{TRAP_CHANNELS}+ channels in "
            f"{TRAP_WINDOW_SECONDS} seconds** (a rate no human can hit). Their messages are "
            "purged and a report with **Approve / Unmute** buttons is sent to the mod channel. "
            "Mods, admins, and bots are never trapped."
        ),
        inline=False,
    )
    embed.add_field(
        name="Abuse Warning",
        value=(
            "**Do not abuse `/kill`.** If you are found to be flagging real users, coordinating "
            "false votes, or otherwise misusing the system, you may be **muted, banned, or receive "
            "a similar server infraction**. When a kill is reversed as a false positive, moderators "
            "can also timeout everyone who voted for it."
        ),
        inline=False,
    )
    embed.add_field(
        name="Moderator Commands",
        value=(
            "`/unkill @user` - Reverse a kill (unmute + mark false positive)\n"
            "`/unkill @user mute:<duration>` - Same, but also timeout the voters "
            "(`1h`-`24h` or `1d`-`28d`)"
        ),
        inline=False,
    )
    embed.set_footer(text="Every server member is an Enforcer. Stay vigilant - and don't abuse it.")
    await interaction.response.send_message(embed=embed, ephemeral=True)


@bot.tree.command(name="killboard", description="Enforcer leaderboard - top spambot killers")
async def killboard_command(interaction: discord.Interaction):
    rows = await get_leaderboard(interaction.guild_id)

    if not rows:
        await interaction.response.send_message("No kills recorded yet.", ephemeral=True)
        return

    lines = []
    medals = {0: "\N{TROPHY}", 1: "\N{SECOND PLACE MEDAL}", 2: "\N{THIRD PLACE MEDAL}"}
    for i, row in enumerate(rows):
        prefix = medals.get(i, f"`{i+1}.`")
        net = row["net_score"]
        fp = row["false_positives"]
        fp_str = f" ({fp} false)" if fp else ""
        lines.append(f"{prefix} <@{row['user_id']}> - **{net}** kills{fp_str}")

    embed = discord.Embed(
        title="ENFORCER KILLBOARD",
        description="\n".join(lines),
        color=discord.Color.gold(),
        timestamp=discord.utils.utcnow(),
    )
    embed.set_footer(text="Net score = confirmed kills - false positives")
    await interaction.response.send_message(embed=embed)


def parse_mute_duration(value: str) -> timedelta | None:
    """Parse '1h'..'24h' or '1d'..'28d' into a timedelta. Returns None if invalid."""
    if not value:
        return None
    s = value.strip().lower()
    if len(s) < 2:
        return None
    unit = s[-1]
    num_part = s[:-1]
    if not num_part.isdigit():
        return None
    n = int(num_part)
    if unit == "h" and 1 <= n <= 24:
        return timedelta(hours=n)
    if unit == "d" and 1 <= n <= 28:
        return timedelta(days=n)
    return None


async def mute_duration_autocomplete(
    interaction: discord.Interaction, current: str
) -> list[app_commands.Choice[str]]:
    options: list[str] = [f"{h}h" for h in range(1, 25)] + [f"{d}d" for d in range(1, 29)]
    cur = current.strip().lower()
    if cur:
        filtered = [o for o in options if o.startswith(cur)]
    else:
        filtered = options
    return [app_commands.Choice(name=o, value=o) for o in filtered[:25]]


@bot.tree.command(name="unkill", description="[Mod] Reverse a kill - unmute user and mark as false positive")
@app_commands.default_permissions(manage_messages=True)
@app_commands.describe(
    target="The user to unmute",
    mute="Optional: timeout the voters who confirmed this kill (1h-24h or 1d-28d)",
)
@app_commands.autocomplete(mute=mute_duration_autocomplete)
async def unkill_command(
    interaction: discord.Interaction,
    target: discord.Member,
    mute: str | None = None,
):
    # Mod check
    if not interaction.user.guild_permissions.manage_messages:
        await interaction.response.send_message("Moderators only.", ephemeral=True)
        return

    # Validate mute duration if provided
    voter_mute_delta: timedelta | None = None
    if mute:
        voter_mute_delta = parse_mute_duration(mute)
        if voter_mute_delta is None:
            await interaction.response.send_message(
                f"Invalid duration `{mute}`. Use 1h-24h or 1d-28d (e.g. `6h`, `3d`, `28d`).",
                ephemeral=True,
            )
            return

    # Find and mark false positive
    participants = await mark_false_positive(interaction.guild_id, target.id)
    if participants is None:
        await interaction.response.send_message(
            f"No confirmed kill found for {target.mention}.", ephemeral=True
        )
        return

    # Remove timeout on the original target
    try:
        await target.timeout(None, reason=f"DEIMOS: Kill reversed by {interaction.user.display_name}")
    except discord.Forbidden:
        pass

    # Decrement scores for all participants
    for uid in participants:
        await add_score(interaction.guild_id, uid, "Unknown", correct=-1, false_pos=1)

    # Optionally mute the voters
    voters_muted: list[int] = []
    voters_failed: list[int] = []
    if voter_mute_delta is not None:
        until = discord.utils.utcnow() + voter_mute_delta
        reason = (
            f"DEIMOS: voted on false-positive kill of {target.display_name}, "
            f"reversed by {interaction.user.display_name}"
        )
        for uid in participants:
            try:
                voter = interaction.guild.get_member(uid) or await interaction.guild.fetch_member(uid)
            except (discord.NotFound, discord.HTTPException):
                voters_failed.append(uid)
                continue
            try:
                await voter.timeout(until, reason=reason)
                voters_muted.append(uid)
            except discord.Forbidden:
                voters_failed.append(uid)
            except Exception:
                logger.error("Failed to mute voter %s", uid, exc_info=True)
                voters_failed.append(uid)

    description = (
        f"**{target.mention}** has been unmuted.\n"
        f"Marked as false positive - scores adjusted for {len(participants)} Enforcers."
    )
    if voter_mute_delta is not None:
        description += f"\n\n**Voter punishment:** {mute} timeout"
        if voters_muted:
            description += f"\nMuted: {' '.join(f'<@{u}>' for u in voters_muted)}"
        if voters_failed:
            description += f"\nFailed: {' '.join(f'<@{u}>' for u in voters_failed)}"

    embed = discord.Embed(
        title="KILL REVERSED",
        description=description,
        color=discord.Color.green(),
        timestamp=discord.utils.utcnow(),
    )
    await interaction.response.send_message(embed=embed)

    logger.info(
        "[guild:%s] Kill reversed on %s by %s (voter_mute=%s, muted=%d, failed=%d)",
        interaction.guild_id, target.id, interaction.user.id,
        mute or "none", len(voters_muted), len(voters_failed),
    )


# ---------------------------------------------------------------------------
# Dev commands
# ---------------------------------------------------------------------------

def is_owner():
    def predicate(interaction: discord.Interaction) -> bool:
        return interaction.user.id in DEV_USER_IDS
    return app_commands.check(predicate)


@bot.tree.command(name="pulse", description="[Dev] Check if DEIMOS is alive")
@is_owner()
async def pulse_command(interaction: discord.Interaction):
    uptime = discord.utils.utcnow() - bot.uptime
    hours, remainder = divmod(int(uptime.total_seconds()), 3600)
    minutes, seconds = divmod(remainder, 60)

    db_ok = False
    if db_pool:
        try:
            await db_fetchone("SELECT 1 AS ping")
            db_ok = True
        except Exception:
            pass

    pending = len(pending_kills)
    processed = len(processed_kills)

    embed = discord.Embed(
        title="DEIMOS PULSE",
        description=(
            f"**Status:** Online\n"
            f"**Uptime:** {hours}h {minutes}m {seconds}s\n"
            f"**Latency:** {round(bot.latency * 1000)}ms\n"
            f"**Database:** {'Connected' if db_ok else 'DOWN'}\n"
            f"**Pending kills:** {pending}\n"
            f"**Processed (session):** {processed}\n"
            f"**Auto-trap:** {'ARMED' if AUTOTRAP_ENABLED else 'OFF'} "
            f"({TRAP_CHANNELS} ch / {TRAP_WINDOW_SECONDS}s, {len(trapped_users)} trapped)"
        ),
        color=discord.Color.green() if db_ok else discord.Color.red(),
        timestamp=discord.utils.utcnow(),
    )
    await interaction.response.send_message(embed=embed, ephemeral=True)


@bot.tree.command(name="vision", description="[Dev] What DEIMOS can see in this server")
@is_owner()
async def vision_command(interaction: discord.Interaction):
    guild = interaction.guild

    # Channels we can read
    readable = []
    unreadable = []
    for ch in guild.text_channels:
        perms = ch.permissions_for(guild.me)
        if perms.read_messages and perms.send_messages:
            readable.append(ch.mention)
        else:
            unreadable.append(f"{ch.mention} ({'no read' if not perms.read_messages else 'no send'})")

    # Key permissions
    me = guild.me
    perms = me.guild_permissions
    perm_lines = [
        f"Administrator: {'YES' if perms.administrator else 'NO'}",
        f"Moderate Members: {'YES' if perms.moderate_members else 'NO'}",
        f"Manage Messages: {'YES' if perms.manage_messages else 'NO'}",
        f"Read Message History: {'YES' if perms.read_message_history else 'NO'}",
    ]

    embed = discord.Embed(
        title="DEIMOS VISION",
        color=discord.Color.blue(),
        timestamp=discord.utils.utcnow(),
    )
    embed.add_field(
        name=f"Readable Channels ({len(readable)})",
        value="\n".join(readable[:20]) + (f"\n...+{len(readable)-20} more" if len(readable) > 20 else "") if readable else "None",
        inline=False,
    )
    if unreadable:
        embed.add_field(
            name=f"Blocked Channels ({len(unreadable)})",
            value="\n".join(unreadable[:10]) + (f"\n...+{len(unreadable)-10} more" if len(unreadable) > 10 else ""),
            inline=False,
        )
    embed.add_field(
        name="Permissions",
        value="```\n" + "\n".join(perm_lines) + "\n```",
        inline=False,
    )
    embed.add_field(
        name="Server Info",
        value=f"Members: {guild.member_count}\nMy top role: {me.top_role.mention if me.top_role else 'None'}",
        inline=False,
    )
    await interaction.response.send_message(embed=embed, ephemeral=True)


@bot.tree.command(name="scan", description="[Dev] Preview what messages DEIMOS would purge for a user")
@is_owner()
@app_commands.describe(target="The user to scan")
async def scan_command(interaction: discord.Interaction, target: discord.Member):
    await interaction.response.defer(ephemeral=True)

    cutoff = discord.utils.utcnow() - timedelta(minutes=PURGE_MINUTES)
    found = []

    for channel in iter_message_channels(interaction.guild):
        try:
            async for msg in channel.history(limit=100, after=cutoff):
                if msg.author.id == target.id:
                    content = msg.content[:80] + ("..." if len(msg.content) > 80 else "")
                    ts = int(msg.created_at.timestamp())
                    found.append(f"{channel.mention} <t:{ts}:R> - {content or '[embed/attachment]'}")
        except (discord.Forbidden, discord.HTTPException):
            continue

    if not found:
        await interaction.followup.send(
            f"No messages from {target.mention} in the last {PURGE_MINUTES} minutes across any visible channel.",
            ephemeral=True,
        )
        return

    # Chunk into embed fields if lots of messages
    description = "\n".join(found[:30])
    if len(found) > 30:
        description += f"\n\n...+{len(found) - 30} more"

    embed = discord.Embed(
        title=f"SCAN: {target.display_name}",
        description=description,
        color=discord.Color.blue(),
        timestamp=discord.utils.utcnow(),
    )
    embed.set_footer(text=f"{len(found)} messages would be purged across {interaction.guild.name}")
    await interaction.followup.send(embed=embed, ephemeral=True)


@bot.tree.command(name="stats", description="[Dev] DEIMOS kill stats for this server")
@is_owner()
async def stats_command(interaction: discord.Interaction):
    guild_id = interaction.guild_id
    total = await db_fetchone("SELECT COUNT(*) AS c FROM DEIMOS_kills WHERE guild_id=%s", (guild_id,))
    confirmed = await db_fetchone("SELECT COUNT(*) AS c FROM DEIMOS_kills WHERE guild_id=%s AND status='confirmed'", (guild_id,))
    expired = await db_fetchone("SELECT COUNT(*) AS c FROM DEIMOS_kills WHERE guild_id=%s AND status='expired'", (guild_id,))
    false_pos = await db_fetchone("SELECT COUNT(*) AS c FROM DEIMOS_kills WHERE guild_id=%s AND status='false_positive'", (guild_id,))
    enforcers = await db_fetchone("SELECT COUNT(*) AS c FROM DEIMOS_scores WHERE guild_id=%s AND correct_kills > 0", (guild_id,))

    embed = discord.Embed(
        title="DEIMOS STATS",
        description=(
            f"**Total kill votes:** {total['c']}\n"
            f"**Confirmed kills:** {confirmed['c']}\n"
            f"**Expired (no action):** {expired['c']}\n"
            f"**False positives:** {false_pos['c']}\n"
            f"**Active Enforcers:** {enforcers['c']}"
        ),
        color=discord.Color.dark_red(),
        timestamp=discord.utils.utcnow(),
    )
    await interaction.response.send_message(embed=embed, ephemeral=True)


# ---------------------------------------------------------------------------
# Background task: expire pending kills
# ---------------------------------------------------------------------------

@tasks.loop(seconds=5)
async def expire_pending_kills():
    now = datetime.now(timezone.utc)
    expired = []

    for kill_id, kill_data in list(pending_kills.items()):
        if kill_id in processed_kills:
            continue
        if now >= kill_data["expires_at"]:
            expired.append(kill_id)

    for kill_id in expired:
        kill_data = pending_kills.pop(kill_id, None)
        if not kill_data:
            continue

        processed_kills.add(kill_id)
        await expire_kill(kill_id)

        # Edit every channel's panel to show expired, then drop its lookup
        embed = discord.Embed(
            title="KILL EXPIRED",
            description=f"**Target:** <@{kill_data['target_id']}>\n"
                        f"Not enough confirmations within {KILL_WINDOW_SECONDS}s.",
            color=discord.Color.dark_grey(),
            timestamp=discord.utils.utcnow(),
        )
        for panel in kill_data["panels"]:
            try:
                msg = await panel["channel"].fetch_message(panel["message_id"])
                await msg.edit(embed=embed, view=None)
            except Exception:
                logger.debug("Could not edit expired kill panel %s", panel["message_id"])
            panel_index.pop(panel["message_id"], None)

        logger.info("[guild:%s] Kill vote expired for %s", kill_data["guild_id"], kill_data["target_id"])


@expire_pending_kills.before_loop
async def before_expire_loop():
    await bot.wait_until_ready()


# ---------------------------------------------------------------------------
# Bot lifecycle
# ---------------------------------------------------------------------------

@bot.event
async def on_command_error(ctx, error):
    if isinstance(error, commands.CommandNotFound):
        return
    logger.error("Prefix command error", exc_info=error)


@bot.tree.error
async def on_app_command_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    if isinstance(error, app_commands.TransformerError):
        msg = f"Couldn't find that user. Pick them from the autocomplete list or @mention them directly."
        try:
            if interaction.response.is_done():
                await interaction.followup.send(msg, ephemeral=True)
            else:
                await interaction.response.send_message(msg, ephemeral=True)
        except Exception:
            pass
        return
    if isinstance(error, app_commands.CheckFailure):
        try:
            if not interaction.response.is_done():
                await interaction.response.send_message("You don't have permission to use this command.", ephemeral=True)
        except Exception:
            pass
        return
    logger.error("App command error", exc_info=error)


@bot.event
async def on_ready():
    await init_db()
    bot.uptime = discord.utils.utcnow()
    await bot.change_presence(
        status=discord.Status.online,
        activity=discord.Activity(type=discord.ActivityType.watching, name="for spambots"),
    )
    logger.info("DEIMOS online as %s (%s)", bot.user.name, bot.user.id)
    logger.info("Connected to %d guild(s)", len(bot.guilds))

    # Sync commands
    try:
        synced = await bot.tree.sync()
        logger.info("Synced %d slash commands", len(synced))
    except Exception:
        logger.error("Failed to sync commands", exc_info=True)

    # Start background task
    if not expire_pending_kills.is_running():
        expire_pending_kills.start()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    token = os.environ.get("DISCORD_TOKEN")
    if not token:
        logger.error("DISCORD_TOKEN not set")
        return
    bot.run(token, log_handler=None)


if __name__ == "__main__":
    main()

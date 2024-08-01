use std::{borrow::Cow, collections::HashMap, fmt::Display};

use futures::{join, stream::iter, StreamExt};
use log::{debug, info, warn};

use rand::Rng;
use serenity::{
    all::{ChannelType, EditMember, GuildMemberUpdateEvent, Role, RoleId},
    async_trait,
    client::Cache,
    model::{
        gateway::Activity,
        prelude::{
            ActivityType, ApplicationId, ChannelId, Guild, GuildId, Member, Presence, UserId,
        },
        user::User,
        voice::VoiceState,
    },
    prelude::*,
};

use sled::Db;

use crate::db::{
    get_name, has_overridden_name, make_name_batch, name_overrides_db_tree_name, DbKey,
};

const LEAGUE_OF_LEGENDS_APPLICATION_ID: Option<ApplicationId> =
    Some(ApplicationId::new(401518684763586560));
const BOT_USER_ID: UserId = UserId::new(1125768259841577021);

fn current_champion_from_activities<'a, I: IntoIterator<Item = &'a Activity>>(
    activities: I,
) -> Option<&'a str> {
    activities
        .into_iter()
        .inspect(|activity| debug!("Checking activity {activity:?}"))
        .flat_map(|activity: &Activity| {
            let is_valid_activity = activity.kind == ActivityType::Playing
                && activity.application_id == LEAGUE_OF_LEGENDS_APPLICATION_ID;
            is_valid_activity.then_some(activity.assets.as_ref()?.large_text.as_ref()?)
        })
        .next()
        .map(String::as_str)
}
struct Handler {
    db: Db,
}

async fn set_nicks<'a, S: Into<String> + Display, I: IntoIterator<Item = (UserId, S)>>(
    ctx: &Context,
    guild_id: GuildId,
    nicks: I,
) {
    iter(nicks.into_iter())
        .for_each_concurrent(10, |(user_id, nick)| async move {
            info!("Setting nickname to {nick} for {user_id}");
            if let Err(e) = guild_id
                .edit_member(&ctx.http, user_id, EditMember::new().nickname(nick))
                .await
            {
                warn!("Failed to set nickname for {user_id}: {e:?}");
            } else {
                info!("Successfully set nickname for {user_id}");
            }
        })
        .await;
}
async fn channel_members(
    cache: &Cache,
    guild_id: GuildId,
    channel_id: ChannelId,
) -> Option<Vec<Member>> {
    cache
        .guild(guild_id)?
        .channels
        .get(&channel_id)?
        .members(cache)
        .inspect_err(|e| {
            warn!("Failed to get members for channel {channel_id:?} in guild {guild_id:?} {e}")
        })
        .ok()
}

#[async_trait]
impl EventHandler for Handler {
    async fn guild_create(&self, ctx: Context, guild: Guild, _is_new: Option<bool>) {
        info!("Guild create for {} ({})", guild.name, guild.id);
        let names = self.db.open_tree(DbKey::from(guild.id)).unwrap();
        let name_overrides = self
            .db
            .open_tree(name_overrides_db_tree_name(guild.id))
            .unwrap();
        names
            .apply_batch(make_name_batch(
                guild
                    .members
                    .values()
                    .filter(|member| !has_overridden_name(member, &name_overrides)),
            ))
            .unwrap();
        iter(
            guild
                .channels
                .values()
                .filter(|c| c.kind == ChannelType::Voice),
        )
        .for_each_concurrent(10, |channel| {
            info!(
                "Examining channel {} ({}) in {} ({})",
                channel.name, channel.id, guild.name, guild.id
            );
            self.sync_nicks(&ctx, guild.id, channel.id)
        })
        .await;
    }

    async fn presence_update(&self, ctx: Context, presence: Presence) {
        async fn find_channel_containing_user(
            presence: Presence,
            cache: &Cache,
        ) -> Option<ChannelId> {
            cache
                .guild(presence.guild_id?)?
                .channels
                .values()
                .filter(|channel| channel.kind == ChannelType::Voice)
                .filter_map(|channel| {
                    channel
                        .members(cache)
                        .inspect_err(|e| {
                            warn!(
                                "Failed to get members during presence update for channel {} {e}",
                                channel.name
                            )
                        })
                        .ok()?
                        .into_iter()
                        .any(|member| member.user.id == presence.user.id)
                        .then_some(channel.id)
                })
                .next()
        }
        if let Some(guild_id) = presence.guild_id {
            if let Some(channel_id) = find_channel_containing_user(presence, &ctx.cache).await {
                self.sync_nicks(&ctx, guild_id, channel_id).await;
            }
        }
    }

    async fn voice_state_update(
        &self,
        ctx: Context,
        old_state: Option<VoiceState>,
        new_state: VoiceState,
    ) {
        let new_state_future = self.process_voice_state_update(&ctx, &new_state);
        let old_state_future = async {
            if let Some(voice_state) = old_state {
                let restore_leaving_user_name_future = async {
                    if let Some(ref member) = voice_state.member {
                        let names = self.db.open_tree(DbKey::from(member.guild_id)).unwrap();
                        let nick_to_restore = get_name(&names, DbKey::from(member.user.id))
                            .unwrap_or(member.user.name.clone());
                        info!(
                            "Restoring nickname {nick_to_restore} to {} ({})",
                            member.user.name, member.user.id
                        );
                        if let Err(e) = member
                            .guild_id
                            .edit_member(
                                &ctx.http,
                                member.user.id,
                                EditMember::new().nickname(nick_to_restore.to_string()),
                            )
                            .await
                        {
                            warn!(
                                "Failed to restore user name {nick_to_restore} to {} ({}): {e:?}",
                                member.user.name, member.user.id
                            );
                        }
                    }
                };
                join!(
                    restore_leaving_user_name_future,
                    self.process_voice_state_update(&ctx, &voice_state),
                );
            }
        };
        join!(new_state_future, old_state_future);
    }

    async fn guild_member_update(
        &self,
        _ctx: Context,
        _old_if_available: Option<Member>,
        new: Option<Member>,
        _event: GuildMemberUpdateEvent,
    ) {
        if let Some(new) = new {
            let name_overrides = self
                .db
                .open_tree(name_overrides_db_tree_name(new.guild_id))
                .unwrap();
            if !has_overridden_name(&new, &name_overrides) {
                let user_id_key = DbKey::from(new.user.id);
                name_overrides.remove(user_id_key).unwrap();
                let names = self.db.open_tree(DbKey::from(new.guild_id)).unwrap();
                names
                    .apply_batch(make_name_batch(std::iter::once((
                        user_id_key,
                        new.display_name(),
                    ))))
                    .unwrap();
            }
        }
    }
    async fn guild_member_addition(&self, _ctx: Context, new_member: Member) {
        self.db
            .open_tree(DbKey::from(new_member.guild_id))
            .unwrap()
            .insert(DbKey::from(new_member.user.id), new_member.display_name())
            .unwrap();
    }
    async fn guild_member_removal(
        &self,
        _ctx: Context,
        guild_id: GuildId,
        user: User,
        _member_data_if_available: Option<Member>,
    ) {
        let key = DbKey::from(user.id);
        self.db
            .open_tree(name_overrides_db_tree_name(guild_id))
            .unwrap()
            .remove(key)
            .unwrap();
        self.db
            .open_tree(DbKey::from(guild_id))
            .unwrap()
            .remove(key)
            .unwrap();
    }
}

fn max_role_position<'a, I: IntoIterator<Item = &'a RoleId>>(
    roles: &HashMap<RoleId, Role>,
    role_ids: I,
) -> u16 {
    if let Some(min_role_position) = role_ids
        .into_iter()
        .flat_map(|role_id| roles.get(role_id))
        .map(|role| role.position)
        .max()
    {
        min_role_position
    } else {
        warn!("No roles found");
        0
    }
}

impl Handler {
    async fn process_voice_state_update(&self, ctx: &Context, voice_state: &VoiceState) {
        if let Some(guild_id) = voice_state.guild_id {
            if let Some(channel_id) = voice_state.channel_id {
                self.sync_nicks(ctx, guild_id, channel_id).await;
            }
        }
    }
    async fn sync_nicks(&self, ctx: &Context, guild_id: GuildId, channel_id: ChannelId) {
        info!("Syncing nicknames for channel {channel_id} in guild {guild_id}");
        let members = channel_members(&ctx.cache, guild_id, channel_id)
            .await
            .unwrap_or(vec![]);
        let (names, new_nicks) = if let Some(guild) = ctx.cache.guild(guild_id) {
            // Find the bot's role's max position. Discord only permits us to rename people with "lower" roles than us.
            // It's okay to recompute this every time since it can get dragged around while we're still running
            let bot_max_role_position = max_role_position(
                &guild.roles,
                &guild.members.get(&BOT_USER_ID).unwrap().roles,
            );
            let mut renamable_members: Vec<_> = members
                .iter()
                .filter(|member| {
                    let position = max_role_position(&guild.roles, &member.roles);
                    info!(
                        "User {} ({}). Max role pos {position} (vs bot: {bot_max_role_position}).",
                        member.user.name, member.user.id
                    );
                    position < bot_max_role_position
                })
                .collect();

            let mut r = rand::thread_rng();

            // First assign champion names to members at random since champion names are the first choice.
            let mut new_nicks: Vec<_> = members
                .iter()
                .flat_map(|member| {
                    Some((
                        member,
                        current_champion_from_activities(
                            &guild.presences.get(&member.user.id)?.activities,
                        )?,
                    ))
                })
                .map_while(|(member, champion_name)| {
                    if renamable_members.is_empty() {
                        None
                    } else {
                        let n = r.gen_range(0..renamable_members.len());
                        let n = if renamable_members[n].user.id == member.user.id {
                            (n + 1) % renamable_members.len()
                        } else {
                            n
                        };
                        Some((
                            renamable_members.swap_remove(n).user.id,
                            Cow::Owned(champion_name.to_string()),
                        ))
                    }
                })
                .collect();
            let names = self.db.open_tree(DbKey::from(guild_id)).unwrap();
            // Fall back to reassigning their old nickname or current user name.
            new_nicks.extend(renamable_members.into_iter().map(|member| {
                let nick = if let Some(nick) = get_name(&names, DbKey::from(member.user.id)) {
                    Cow::Owned(nick)
                } else {
                    Cow::Borrowed(member.user.name.as_str())
                };
                (member.user.id, nick)
            }));
            (names, new_nicks)
        } else {
            warn!("Failed to sync nicknames for guild {guild_id} because the guild wasn't found in the cache");
            return;
        };
        // First set to the old nicks so that if we crash, the old nick will stick.
        let old_nicks: Vec<_> = members
            .iter()
            .flat_map(|member| {
                Some((
                    member.user.id,
                    get_name(&names, DbKey::from(member.user.id))?,
                ))
            })
            .collect();
        info!("Setting old nicknames so they're saved if we encounter an error.");
        set_nicks(ctx, guild_id, old_nicks).await;
        let name_overrides = self
            .db
            .open_tree(name_overrides_db_tree_name(guild_id))
            .unwrap();
        // Clear and set the overrides. We want to record the overrides before we actually make the change just in case we crash in the middle.
        name_overrides.clear().unwrap();
        name_overrides
            .apply_batch(make_name_batch(new_nicks.iter()))
            .unwrap();
        info!("Setting new nicknames");
        set_nicks(ctx, guild_id, new_nicks).await;
    }
}

pub async fn run(token: String, db: Db) {
    let intents = GatewayIntents::GUILD_PRESENCES
        | GatewayIntents::GUILD_VOICE_STATES
        | GatewayIntents::GUILDS
        | GatewayIntents::GUILD_MEMBERS;

    let mut client = Client::builder(token, intents)
        .event_handler(Handler { db })
        .await
        .expect("Error creating client");

    if let Err(why) = client.start().await {
        println!("Client error: {:?}", why);
    }
}

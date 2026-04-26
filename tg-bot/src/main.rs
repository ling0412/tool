use chrono_tz::Asia::Shanghai;
use sqlx::{sqlite::SqlitePoolOptions, SqlitePool, Row};
use std::{collections::HashMap, sync::Arc};
use teloxide::{
    prelude::*,
    types::{InlineKeyboardButton, InlineKeyboardMarkup, ParseMode},
    utils::html,
    dispatching::dialogue::InMemStorage,
};
use tokio::sync::{Mutex, Semaphore};
use tokio_cron_scheduler::{Job, JobScheduler};
use tokio_retry::{strategy::{jitter, ExponentialBackoff}, Retry};
use uuid::Uuid;

// --- 类型与状态定义 ---

#[derive(Clone, Default)]
pub enum State {
    #[default]
    Idle,
    ReceiveTitle,
    ReceiveDescription { title: String },
    ReceiveTime { title: String, desc: String },
}

#[derive(Clone)]
struct TaskMeta {
    name: String,
    chat_id: i64,
    sched_id: Uuid,
}

type TaskMap = Arc<Mutex<HashMap<Uuid, TaskMeta>>>;

#[derive(Clone)]
struct BotState {
    pool: SqlitePool,
    sched: Arc<JobScheduler>,
    tasks: TaskMap,
    pending_sem: Arc<Semaphore>,
}

#[derive(teloxide::macros::BotCommands, Clone)]
#[command(rename_rule = "lowercase")]
enum Command {
    Register,
    List,
}

// --- 主程序 ---

#[tokio::main]
async fn main() {
    pretty_env_logger::init();
    
    // 建立连接
    let pool = SqlitePoolOptions::new()
        .max_connections(5)
        .connect("sqlite:tasks.db?mode=rwc").await.expect("数据库连接失败");

    // DDL 检查
    sqlx::query("CREATE TABLE IF NOT EXISTS tasks (uuid TEXT PRIMARY KEY, name TEXT, description TEXT, cron TEXT, chat_id INTEGER)")
        .execute(&pool).await.expect("初始化 tasks 表失败");
    sqlx::query("CREATE TABLE IF NOT EXISTS pending_messages (id INTEGER PRIMARY KEY, chat_id INTEGER, content TEXT)")
        .execute(&pool).await.expect("初始化 pending_messages 表失败");

    let sched = Arc::new(JobScheduler::new().await.unwrap());
    let bot_state = BotState {
        pool,
        sched: sched.clone(),
        tasks: Arc::new(Mutex::new(HashMap::new())),
        pending_sem: Arc::new(Semaphore::new(1)),
    };

    let bot = Bot::from_env();
    let _ = sched.start().await;
    
    // 恢复任务与补发机制
    restore_tasks(&bot, &bot_state).await;
    tokio::spawn(process_pending(bot.clone(), bot_state.clone()));

    let handler = Update::filter_message()
        .enter_dialogue::<Message, InMemStorage<State>, State>()
        .branch(dptree::entry().filter_command::<Command>().endpoint(cmd_handler))
        .branch(dptree::case![State::ReceiveTitle].endpoint(receive_title))
        .branch(dptree::case![State::ReceiveDescription { title }].endpoint(receive_desc))
        .branch(dptree::case![State::ReceiveTime { title, desc }].endpoint(receive_time));

    let callbacks = Update::filter_callback_query().endpoint(handle_callback);

    Dispatcher::builder(bot, dptree::entry().branch(handler).branch(callbacks))
        .dependencies(dptree::deps![bot_state, InMemStorage::<State>::new()])
        .enable_ctrlc_handler()
        .build()
        .dispatch()
        .await;
}

// --- 指令与对话处理器 ---

async fn cmd_handler(
    bot: Bot, 
    msg: Message, 
    cmd: Command, 
    dialogue: Dialogue<State, InMemStorage<State>>, 
    state: BotState
) -> ResponseResult<()> {
    match cmd {
        Command::Register => {
            bot.send_message(msg.chat.id, "请输入任务标题:").await?;
            let _ = dialogue.update(State::ReceiveTitle).await;
        }
        Command::List => {
            let tasks_data = {
                let tasks = state.tasks.lock().await;
                tasks.iter()
                    .filter(|(_, v)| v.chat_id == msg.chat.id.0)
                    .map(|(k, v)| (k.to_string(), v.name.clone()))
                    .collect::<Vec<_>>()
            };
            
            if tasks_data.is_empty() {
                bot.send_message(msg.chat.id, "当前没有活跃任务").await?;
            } else {
                let btns = tasks_data.into_iter()
                    .map(|(uuid, name)| vec![InlineKeyboardButton::callback(format!("🗑️ 删除: {}", name), uuid)])
                    .collect::<Vec<_>>();
                
                bot.send_message(msg.chat.id, "<b>活跃任务列表</b>\n点击按钮即可删除任务:")
                    .parse_mode(ParseMode::Html)
                    .reply_markup(InlineKeyboardMarkup::new(btns))
                    .await?;
            }
        }
    }
    Ok(())
}

async fn receive_title(bot: Bot, dialogue: Dialogue<State, InMemStorage<State>>, msg: Message) -> ResponseResult<()> {
    if let Some(t) = msg.text().map(str::trim).filter(|t| !t.is_empty()) {
        bot.send_message(msg.chat.id, "请输入描述内容:").await?;
        let _ = dialogue.update(State::ReceiveDescription { title: t.to_string() }).await;
    } else {
        bot.send_message(msg.chat.id, "标题不能为空:").await?;
    }
    Ok(())
}

async fn receive_desc(bot: Bot, dialogue: Dialogue<State, InMemStorage<State>>, title: String, msg: Message) -> ResponseResult<()> {
    let desc = msg.text().map(str::trim).unwrap_or("");
    bot.send_message(msg.chat.id, "请输入 Cron 表达式\n格式: <code>秒 分 时 日 月 周</code>\n例如: <code>1/30 * * * * *</code> (每30秒一次)")
        .parse_mode(ParseMode::Html)
        .await?;
    let _ = dialogue.update(State::ReceiveTime { title, desc: desc.to_string() }).await;
    Ok(())
}

async fn receive_time(bot: Bot, dialogue: Dialogue<State, InMemStorage<State>>, (title, desc): (String, String), msg: Message, state: BotState) -> ResponseResult<()> {
    let cron_str = match msg.text().map(str::trim).filter(|c| !c.is_empty()) {
        Some(c) => c.to_string(),
        None => {
            bot.send_message(msg.chat.id, "Cron 不能为空:").await?;
            return Ok(());
        }
    };

    let db_uuid = Uuid::new_v4();
    let chat_id = msg.chat.id;

    // 预验证与任务构建
    let (b, p, t, d) = (bot.clone(), state.pool.clone(), title.clone(), desc.clone());
    let job = match Job::new_async_tz(&cron_str, Shanghai, move |_, _| {
        let (bc, pc, tn, dn) = (b.clone(), p.clone(), t.clone(), d.clone());
        Box::pin(async move { reliable_send(bc, chat_id, tn, dn, pc).await })
    }) {
        Ok(j) => j,
        Err(_) => { 
            bot.send_message(chat_id, "Cron 格式无效，请检查格式 (秒 分 时 日 月 周):").await?; 
            return Ok(()); 
        }
    };

    let sched_id = job.guid();
    
    // 数据库插入
    let db_res = sqlx::query("INSERT INTO tasks (uuid, name, description, cron, chat_id) VALUES (?,?,?,?,?)")
        .bind(db_uuid.to_string()).bind(&title).bind(&desc).bind(&cron_str).bind(chat_id.0)
        .execute(&state.pool).await;

    if db_res.is_ok() {
        if state.sched.add(job).await.is_ok() {
            state.tasks.lock().await.insert(db_uuid, TaskMeta { name: title, chat_id: chat_id.0, sched_id });
            bot.send_message(chat_id, "✅ 任务设置成功").await?;
            let _ = dialogue.exit().await;
        } else {
            sqlx::query("DELETE FROM tasks WHERE uuid = ?").bind(db_uuid.to_string()).execute(&state.pool).await.ok();
            bot.send_message(chat_id, "调度器添加失败，请重发 Cron 表达式:").await?;
        }
    } else {
        bot.send_message(chat_id, "数据库错误，请重试").await?;
    }
    Ok(())
}

// --- 回调与删除逻辑 ---

async fn handle_callback(bot: Bot, q: CallbackQuery, state: BotState) -> ResponseResult<()> {
    let Some(data) = q.data else { return Ok(()) };
    let Ok(db_uuid) = Uuid::parse_str(&data) else { return Ok(()) };

    // 1. 只读检查权限，避免持锁跨 await
    let auth_check = {
        let tasks = state.tasks.lock().await;
        tasks.get(&db_uuid).map(|m| m.chat_id)
    };

    match auth_check {
        None => {
            bot.answer_callback_query(q.id).text("任务已不存在").await?;
            if let Some(m) = q.message { bot.delete_message(m.chat().id, m.id()).await.ok(); }
            return Ok(());
        }
        Some(owner) if owner != q.from.id.0 as i64 => {
            bot.answer_callback_query(q.id).text("无权操作").await?;
            return Ok(());
        }
        Some(_) => {}
    }

    // 2. 原子化删除
    let removed = state.tasks.lock().await.remove(&db_uuid);

    if let Some(meta) = removed {
        let sched_res = state.sched.remove(&meta.sched_id).await;
        let db_res = sqlx::query("DELETE FROM tasks WHERE uuid = ?").bind(&data).execute(&state.pool).await;

        if sched_res.is_err() || db_res.is_err() {
            state.tasks.lock().await.insert(db_uuid, meta); // 回滚
            bot.answer_callback_query(q.id).text("系统错误，删除失败").await?;
        } else {
            bot.answer_callback_query(q.id).text("已成功删除").await?;
            if let Some(m) = q.message { bot.delete_message(m.chat().id, m.id()).await.ok(); }
        }
    } else {
        bot.answer_callback_query(q.id).text("任务已由其他进程处理").await?;
    }
    Ok(())
}

// --- 核心辅助函数 ---

async fn reliable_send(bot: Bot, chat_id: ChatId, title: String, desc: String, pool: SqlitePool) {
    // 方案 A：严格转义，确保消息一定能发送成功，不再依赖用户输入的 HTML 安全性
    let text = format!("🔔 <b>{}</b>\n\n{}", html::escape(&title), html::escape(&desc));
    
    let row_id = sqlx::query("INSERT INTO pending_messages (chat_id, content) VALUES (?,?)")
        .bind(chat_id.0).bind(&text).execute(&pool).await
        .map(|r| r.last_insert_rowid()).unwrap_or(0);

    let strategy = ExponentialBackoff::from_millis(1000).map(jitter).take(3);
    
    let retry_res = Retry::spawn(strategy, || async {
        bot.send_message(chat_id, &text)
            .parse_mode(ParseMode::Html)
            .await
    }).await;

    if retry_res.is_ok() && row_id != 0 {
        sqlx::query("DELETE FROM pending_messages WHERE id = ?").bind(row_id).execute(&pool).await.ok();
    }
}

async fn process_pending(bot: Bot, state: BotState) {
    let Ok(_permit) = state.pending_sem.try_acquire() else { return; };
    
    let rows = sqlx::query("SELECT id, chat_id, content FROM pending_messages").fetch_all(&state.pool).await.unwrap_or_default();
    for row in rows {
        let id: i64 = row.get("id");
        let cid: i64 = row.get("chat_id");
        let content: String = row.get("content");
        if bot.send_message(ChatId(cid), format!("{}\n\n<i>(补发消息)</i>", content))
            .parse_mode(ParseMode::Html).await.is_ok() 
        {
            sqlx::query("DELETE FROM pending_messages WHERE id = ?").bind(id).execute(&state.pool).await.ok();
        }
    }
}

async fn restore_tasks(bot: &Bot, state: &BotState) {
    let rows = sqlx::query("SELECT uuid, name, description, cron, chat_id FROM tasks").fetch_all(&state.pool).await.unwrap_or_default();
    let mut restored = Vec::new();

    for r in rows {
        let u_str: String = r.get("uuid");
        let n: String = r.get("name");
        let d: String = r.get("description");
        let c: String = r.get("cron");
        let cid: i64 = r.get("chat_id");
        
        let (bc, pc, tn, dn) = (bot.clone(), state.pool.clone(), n.clone(), d.clone());
        let job_res = Job::new_async_tz(&c, Shanghai, move |_, _| {
            let (bc2, pc2, tn2, dn2) = (bc.clone(), pc.clone(), tn.clone(), dn.clone());
            Box::pin(async move { reliable_send(bc2, ChatId(cid), tn2, dn2, pc2).await })
        });

        if let Ok(job) = job_res {
            let sid = job.guid();
            if state.sched.add(job).await.is_ok() {
                if let Ok(u) = Uuid::parse_str(&u_str) {
                    restored.push((u, TaskMeta { name: n, chat_id: cid, sched_id: sid }));
                }
            }
        }
    }

    let mut map = state.tasks.lock().await;
    for (u, m) in restored {
        map.insert(u, m);
    }
}

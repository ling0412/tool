use anyhow::{bail, Context, Result};
use camino::{Utf8Path, Utf8PathBuf};
use clap::Parser;
use dialoguer::{theme::ColorfulTheme, Confirm};
use filetime::{set_symlink_file_times, FileTime};
use globset::{Glob, GlobSetBuilder};
use jwalk::WalkDir;
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::os::unix::fs::{symlink, MetadataExt};
use std::time::SystemTime;
use std::{env, fs, io};
use xxhash_rust::xxh3::xxh3_64;

#[derive(Parser, Debug)]
#[command(author, version, about = "rlink: 安全级多源聚合同步工具")]
struct Args {
    #[arg(short, long)]
    task: Option<String>,

    #[arg(short, long, default_value = "rlink.toml")]
    config: Utf8PathBuf,

    #[arg(short = 'n', long)]
    dry_run: bool,

    #[arg(long, alias = "tree")]
    force_tree: bool,

    /// 自动确认。注：触发高危异常比例（如 >80% 清理）时，该标志失效。
    #[arg(short = 'y', long)]
    yes: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct HighResTime {
    secs: i64,
    nanos: u32,
}

impl HighResTime {
    fn from_meta(meta: &fs::Metadata) -> Self {
        let st = meta.modified().unwrap_or(SystemTime::UNIX_EPOCH);
        let duration = st.duration_since(SystemTime::UNIX_EPOCH).unwrap_or_default();
        Self {
            secs: duration.as_secs() as i64,
            nanos: duration.subsec_nanos(),
        }
    }
    fn to_filetime(self) -> FileTime {
        FileTime::from_unix_time(self.secs, self.nanos)
    }
}

#[derive(Deserialize, Debug, Clone, Copy, PartialEq)]
#[serde(rename_all = "lowercase")]
enum LinkMode { Symlink, Hardlink }

#[derive(Deserialize, Debug)]
struct Config {
    #[serde(rename = "tasks")]
    tasks: Vec<TaskConfig>,
}

#[derive(Deserialize, Debug)]
struct TaskConfig {
    name: String,
    target_root: Utf8PathBuf,
    #[serde(default = "default_link_mode")]
    link_mode: LinkMode,
    sources: Vec<SourceConfig>,
    #[serde(default = "default_safety_threshold")]
    safety_threshold: usize,
}

#[derive(Deserialize, Debug)]
struct SourceConfig {
    path: Utf8PathBuf,
    patterns: Vec<String>,
    #[serde(default)]
    ignore_patterns: Vec<String>,
    #[serde(default = "default_tree")]
    tree: bool,
}

fn default_link_mode() -> LinkMode { LinkMode::Symlink }
fn default_tree() -> bool { true }
fn default_safety_threshold() -> usize { 50 }

#[derive(Debug)]
struct RemoteRecord {
    src_path: Utf8PathBuf,
    filename: String,
    rel_dir: Utf8PathBuf,
    mtime: HighResTime,
}

#[derive(Debug, Clone)]
struct LocalLink {
    path: Utf8PathBuf,
    mtime: HighResTime,
}

fn clean_path(path: &Utf8Path) -> Utf8PathBuf {
    assert!(path.is_absolute(), "路径必须为绝对路径: {}", path);
    let mut result = Utf8PathBuf::new();
    for component in path.components() {
        match component {
            camino::Utf8Component::CurDir => {}
            camino::Utf8Component::ParentDir => { result.pop(); }
            c => result.push(c.as_str()),
        }
    }
    result
}

fn resolve_link_target(link_path: &Utf8Path, dest: &Utf8Path) -> Utf8PathBuf {
    let absolute = if dest.is_absolute() {
        dest.to_path_buf()
    } else {
        link_path.parent().expect("链接文件必须有父目录").join(dest)
    };
    clean_path(&absolute)
}

fn run_task(task: &TaskConfig, args: &Args) -> Result<()> {
    tracing::info!(">>> 任务开始: [{}]", task.name);

    let target_root_abs = if args.dry_run {
        let cwd = env::current_dir().context("无法获取当前工作目录")?;
        let cwd_utf8 = Utf8PathBuf::try_from(cwd).context("工作目录包含非 UTF-8 字符")?;
        clean_path(&cwd_utf8.join(&task.target_root))
    } else {
        fs::create_dir_all(&task.target_root)?;
        task.target_root.canonicalize_utf8().context("目标路径规范化失败")?
    };
    
    // 获取目标设备号，dry_run 下如果目录不存在则降级为 0
    let target_dev = fs::metadata(&target_root_abs).map(|m| m.dev()).unwrap_or(0);

    let mut resolved_sources = Vec::new();
    for src in &task.sources {
        let src_abs = src.path.canonicalize_utf8()
            .with_context(|| format!("源路径解析失败 (挂载点可能未就绪): {:?}", src.path))?;
        
        // 仅在非 dry_run 且目标设备已知时执行硬链接跨设备强制校验
        if !args.dry_run && task.link_mode == LinkMode::Hardlink && target_dev != 0 {
            let src_dev = fs::metadata(&src_abs)?.dev();
            if src_dev != target_dev {
                bail!("硬链接不能跨设备: {} (dev={}) -> {} (dev={})", src_abs, src_dev, target_root_abs, target_dev);
            }
        }
        resolved_sources.push((src, src_abs));
    }

    // Phase 1: 扫描目标
    let mut local_map: HashMap<Utf8PathBuf, Vec<LocalLink>> = HashMap::new();
    let mut used_local_paths = HashSet::new();
    let mut inode_to_local: HashMap<(u64, u64), Vec<LocalLink>> = HashMap::new();

    if args.dry_run && !target_root_abs.exists() {
        tracing::info!("目标根目录尚不存在，预览模式将所有条目视为新增: {:?}", target_root_abs);
    }

    for entry in WalkDir::new(&target_root_abs).skip_hidden(true).follow_links(false) {
        let entry = entry?;
        let path = Utf8PathBuf::from_path_buf(entry.path().to_path_buf()).ok().context("路径包含非法字符")?;
        
        if path == target_root_abs || !path.starts_with(&target_root_abs) { continue; }

        let meta = match fs::symlink_metadata(&path) {
            Ok(m) => m,
            Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
            Err(e) => { tracing::warn!("跳过无法访问的条目 {:?}: {}", path, e); continue; }
        };
        used_local_paths.insert(path.clone());

        let is_managed = match task.link_mode {
            LinkMode::Symlink => meta.file_type().is_symlink(),
            LinkMode::Hardlink => meta.is_file() && !meta.file_type().is_symlink(),
        };

        if is_managed {
            let link_data = LocalLink { path: path.clone(), mtime: HighResTime::from_meta(&meta) };
            if task.link_mode == LinkMode::Symlink {
                if let Ok(dest) = fs::read_link(&path) {
                    if let Ok(dest_utf8) = Utf8PathBuf::from_path_buf(dest) {
                        let abs_dest = resolve_link_target(&path, &dest_utf8);
                        if resolved_sources.iter().any(|(_, src_abs)| abs_dest.starts_with(src_abs)) {
                            local_map.entry(abs_dest).or_default().push(link_data);
                        } else {
                            tracing::warn!("跳过外部链接: {:?} -> {:?}", path, abs_dest);
                        }
                    }
                }
            } else {
                inode_to_local.entry((meta.dev(), meta.ino())).or_default().push(link_data);
            }
        }
    }

    let initial_managed_count = used_local_paths.len();

    // Phase 2: 扫描源
    let mut remote_files = Vec::new();
    let mut seen_src = HashSet::new();

    for (src_cfg, src_abs) in resolved_sources {
        let mut gb = GlobSetBuilder::new();
        for p in &src_cfg.patterns { gb.add(Glob::new(p)?); }
        let glob_set = gb.build()?;
        let mut ib = GlobSetBuilder::new();
        for p in &src_cfg.ignore_patterns { ib.add(Glob::new(p)?); }
        let ignore_set = ib.build()?;

        for entry in WalkDir::new(&src_abs).skip_hidden(true) {
            let entry = entry?;
            if !entry.file_type().is_file() || entry.file_type().is_symlink() { continue; }

            let full_path = Utf8PathBuf::from_path_buf(entry.path().to_path_buf()).ok().context("源路径非法")?;
            
            // 重新引入重复源路径过滤，并增加日志预警
            if !seen_src.insert(full_path.clone()) {
                tracing::warn!("源路径被多个配置重叠覆盖，后者忽略: {}", full_path);
                continue; 
            }

            let rel_path = full_path.strip_prefix(&src_abs)?;
            if glob_set.is_match(rel_path.as_str()) && !ignore_set.is_match(rel_path.as_str()) {
                let meta = entry.metadata().with_context(|| format!("读取源文件元数据失败: {}", full_path))?;
                let use_tree = args.force_tree || src_cfg.tree;
                let record = RemoteRecord {
                    src_path: full_path.clone(),
                    filename: full_path.file_name().unwrap_or("unknown").to_string(),
                    rel_dir: if use_tree { rel_path.parent().map(|p| p.to_path_buf()).unwrap_or_default() } else { Utf8PathBuf::new() },
                    mtime: HighResTime::from_meta(&meta),
                };

                if task.link_mode == LinkMode::Hardlink {
                    if let Some(links) = inode_to_local.get(&(meta.dev(), meta.ino())) {
                        local_map.insert(full_path.clone(), links.clone());
                    }
                }
                remote_files.push(record);
            }
        }
    }

    // Phase 3: 同步
    let (mut created, mut updated) = (0, 0);
    let mut processed_src = HashSet::new();

    for remote in remote_files {
        processed_src.insert(remote.src_path.clone());
        if let Some(links) = local_map.get(&remote.src_path) {
            for link in links {
                if task.link_mode == LinkMode::Symlink && link.mtime != remote.mtime {
                    if !args.dry_run {
                        let ft = remote.mtime.to_filetime();
                        set_symlink_file_times(&link.path, ft, ft)?;
                    }
                    updated += 1;
                }
            }
        } else {
            let target_parent = target_root_abs.join(&remote.rel_dir);
            let mut final_name = remote.filename.clone();
            let mut target_path = target_parent.join(&final_name);

            let mut attempt = 0;
            while used_local_paths.contains(&target_path) {
                attempt += 1;
                let hash = xxh3_64(format!("{}{}", remote.src_path, attempt).as_bytes());
                final_name = format!("{}.{:016x}{}", 
                    remote.src_path.file_stem().unwrap_or("unknown"), 
                    hash, 
                    remote.src_path.extension().map(|e| format!(".{}", e)).unwrap_or_default());
                target_path = target_parent.join(&final_name);
                if attempt > 10 { bail!("无法解决路径命名冲突: {:?}", target_path); }
            }

            if args.dry_run {
                created += 1;
                used_local_paths.insert(target_path);
            } else {
                fs::create_dir_all(&target_parent)?;
                let res = match task.link_mode {
                    LinkMode::Symlink => symlink(&remote.src_path, &target_path),
                    LinkMode::Hardlink => fs::hard_link(&remote.src_path, &target_path),
                };

                match res {
                    Ok(_) => {
                        if task.link_mode == LinkMode::Symlink {
                            let ft = remote.mtime.to_filetime();
                            let _ = set_symlink_file_times(&target_path, ft, ft);
                        }
                        created += 1;
                        used_local_paths.insert(target_path);
                    }
                    Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {
                        tracing::error!("TOCTOU 冲突：路径已被占用 {:?}", target_path);
                    }
                    Err(e) => return Err(e).with_context(|| format!("创建链接失败: {:?}", target_path)),
                }
            }
        }
    }

    // Phase 4: 清理
    let to_prune: Vec<_> = local_map.iter()
        .filter(|(src, _)| !processed_src.contains(*src))
        .flat_map(|(_, links)| links)
        .collect();

    let mut pruned = 0;
    if !to_prune.is_empty() {
        let is_suspicious = initial_managed_count > 10 && (to_prune.len() as f64 / initial_managed_count as f64) > 0.8;
        let triggers_threshold = to_prune.len() > task.safety_threshold;

        let should_proceed = if !args.dry_run && (triggers_threshold || is_suspicious) {
            if args.yes && !is_suspicious {
                tracing::warn!("任务 [{}]: 清理数 ({}) 触发阈值，通过 -y 自动放行", task.name, to_prune.len());
                true 
            } else {
                let warn_msg = if is_suspicious { "【高危提示】删除比例超过 80%，请确认源挂载是否正常！" } 
                               else { "清理数量超过预设阈值，确认继续？" };
                Confirm::with_theme(&ColorfulTheme::default())
                    .with_prompt(format!("任务 [{}]: {} (共 {} 条)", task.name, warn_msg, to_prune.len()))
                    .default(false).interact()?
            }
        } else { true };

        if should_proceed {
            for link in to_prune {
                if args.dry_run { 
                    pruned += 1; 
                } else {
                    match fs::remove_file(&link.path) {
                        Ok(_) => pruned += 1,
                        Err(e) => tracing::warn!("清理失败 {:?}: {}", link.path, e),
                    }
                }
            }
        } else {
            tracing::warn!("任务 [{}] 清理阶段被手动跳过", task.name);
        }
    }

    tracing::info!("任务 [{}] 结束: 新增 {}, 更新 {}, 清理 {}", task.name, created, updated, pruned);
    Ok(())
}

fn main() {
    tracing_subscriber::fmt().with_max_level(tracing::Level::INFO).init();
    let args = Args::parse();

    let config_res: Result<Config> = (|| {
        let content = fs::read_to_string(&args.config)?;
        Ok(toml::from_str(&content)?)
    })();

    let config = match config_res {
        Ok(c) => c,
        Err(e) => { tracing::error!("配置文件加载失败: {:?}", e); std::process::exit(1); }
    };

    let tasks_to_run: Vec<_> = config.tasks.iter()
        .filter(|t| args.task.as_ref().map_or(true, |n| n == &t.name))
        .collect();

    if let Some(name) = &args.task {
        if tasks_to_run.is_empty() {
            tracing::error!("未找到匹配的任务: '{}'", name);
            std::process::exit(1);
        }
    }

    let mut has_error = false;
    for task in tasks_to_run {
        if let Err(e) = run_task(task, &args) {
            tracing::error!("任务 [{}] 运行中止: {:?}", task.name, e);
            has_error = true;
        }
    }

    if has_error { std::process::exit(1); }
}

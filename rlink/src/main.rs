use anyhow::{Context, Result};
use camino::Utf8PathBuf;
use clap::Parser;
use filetime::{set_symlink_file_times, FileTime};
use globset::{Glob, GlobSetBuilder};
use jwalk::WalkDir;
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::os::unix::fs::{symlink, MetadataExt};
use std::time::{Instant, SystemTime};
use xxhash_rust::xxh3::xxh3_64;

#[derive(Parser, Debug)]
#[command(author, version, about = "rlink: 高可靠多源聚合同步工具")]
struct Args {
    #[arg(default_value = "sync")]
    command: String,

    /// 指定执行的任务名称
    #[arg(short, long, value_name = "TASK_NAME")]
    task: Option<String>,

    /// 配置文件路径
    #[arg(short, long, default_value = "rlink.toml")]
    config: Utf8PathBuf,

    /// 演练模式
    #[arg(short = 'n', long)]
    dry_run: bool,
}

#[derive(Deserialize, Debug, Clone, Copy, PartialEq)]
#[serde(rename_all = "lowercase")]
enum LinkMode { 
    Symlink, 
    Hardlink 
}

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
}

#[derive(Deserialize, Debug)]
struct SourceConfig {
    path: Utf8PathBuf,
    patterns: Vec<String>,
    #[serde(default)]
    ignore_patterns: Vec<String>,
}

fn default_link_mode() -> LinkMode { 
    LinkMode::Symlink 
}

#[derive(Debug)]
struct RemoteRecord {
    filename: String,
    rel_dir: Utf8PathBuf,
    mtime: i64,
    inode: u64,
    dev: u64,
}

#[derive(Debug, Clone)]
struct LocalLink {
    path: Utf8PathBuf,
    mtime: i64,
}

fn get_mtime_secs(meta: &fs::Metadata) -> i64 {
    meta.modified()
        .unwrap_or(SystemTime::UNIX_EPOCH)
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_target(false)
        .init();

    let args = Args::parse();
    
    if args.command != "sync" {
        anyhow::bail!("Unsupported command: '{}'.", args.command);
    }

    let content = fs::read_to_string(&args.config).context("读取配置文件失败")?;
    let config: Config = toml::from_str(&content).context("解析配置失败")?;

    // 验证任务间的 target_root 是否重叠，防止跨任务误删
    let mut roots: Vec<_> = config.tasks.iter().map(|t| (&t.name, &t.target_root)).collect();
    roots.sort_by_key(|(_, path)| path.as_str().len());
    for i in 0..roots.len() {
        for j in i + 1..roots.len() {
            if roots[j].1.starts_with(roots[i].1) {
                anyhow::bail!("致命错误：任务 [{}] 与 [{}] 的目标路径重叠，可能导致误删。", roots[i].0, roots[j].0);
            }
        }
    }

    let tasks_to_run: Vec<&TaskConfig> = if let Some(ref name) = args.task {
        let filtered: Vec<_> = config.tasks.iter().filter(|t| &t.name == name).collect();
        if filtered.is_empty() { anyhow::bail!("未找到任务: '{}'", name); }
        filtered
    } else {
        config.tasks.iter().collect()
    };

    if args.dry_run {
        tracing::info!("--- [DRY RUN MODE ACTIVE] ---");
    }

    let start = Instant::now();
    for task in tasks_to_run {
        tracing::info!(">>> 开始执行任务: [{}]", task.name);
        if let Err(e) = run_task(task, &args) {
            tracing::error!("任务 [{}] 中断: {:?}", task.name, e);
        }
    }

    tracing::info!("所有任务处理完毕，总耗时: {:.2?}", start.elapsed());
    Ok(())
}

fn run_task(task: &TaskConfig, args: &Args) -> Result<()> {
    if !args.dry_run {
        fs::create_dir_all(&task.target_root).context("无法创建任务根目录")?;
    }

    // --- Phase 1: 扫描本地状态 ---
    let mut local_map: HashMap<Utf8PathBuf, Vec<LocalLink>> = HashMap::new();
    let mut used_local_paths = HashSet::new();
    let mut inode_to_local: HashMap<(u64, u64), Vec<LocalLink>> = HashMap::new();

    for entry in WalkDir::new(&task.target_root).skip_hidden(true) {
        let entry = entry?;
        let path = Utf8PathBuf::from(entry.path().to_string_lossy().into_owned());
        let meta = fs::symlink_metadata(&path)?;
        
        let is_target = match task.link_mode {
            LinkMode::Symlink => meta.file_type().is_symlink(),
            LinkMode::Hardlink => meta.is_file() && !meta.file_type().is_symlink(),
        };

        if is_target {
            used_local_paths.insert(path.clone());
            let mtime = get_mtime_secs(&meta);
            let link_data = LocalLink { path: path.clone(), mtime };

            if task.link_mode == LinkMode::Symlink {
                if let Ok(dest) = fs::read_link(&path) {
                    local_map.entry(Utf8PathBuf::from(dest.to_string_lossy().into_owned())).or_default().push(link_data);
                }
            } else {
                inode_to_local.entry((meta.dev(), meta.ino())).or_default().push(link_data);
            }
        }
    }

    // --- Phase 2: 聚合扫描远程源 ---
    let mut remote_files: HashMap<Utf8PathBuf, RemoteRecord> = HashMap::new();
    for src_cfg in &task.sources {
        let mut glob_builder = GlobSetBuilder::new();
        for p in &src_cfg.patterns { glob_builder.add(Glob::new(p)?); }
        let glob_set = glob_builder.build()?;

        let mut ignore_builder = GlobSetBuilder::new();
        for p in &src_cfg.ignore_patterns { ignore_builder.add(Glob::new(p)?); }
        let ignore_set = ignore_builder.build()?;

        for entry in WalkDir::new(&src_cfg.path).skip_hidden(true) {
            let entry = entry?;
            if !entry.file_type().is_file() || entry.file_type().is_symlink() { continue; }

            let full_path = Utf8PathBuf::from(entry.path().to_string_lossy().into_owned());
            let rel_path = match full_path.strip_prefix(&src_cfg.path) {
                Ok(p) => p,
                Err(_) => continue,
            };

            let rel_str = rel_path.as_str();
            if glob_set.is_match(rel_str) && !ignore_set.is_match(rel_str) {
                if let Ok(meta) = entry.metadata() {
                    let record = RemoteRecord {
                        filename: full_path.file_name().unwrap_or("unknown").to_string(),
                        rel_dir: rel_path.parent().map(|p| p.to_path_buf()).unwrap_or_default(),
                        mtime: get_mtime_secs(&meta),
                        inode: meta.ino(),
                        dev: meta.dev(),
                    };
                    if task.link_mode == LinkMode::Hardlink {
                        if let Some(links) = inode_to_local.get(&(record.dev, record.inode)) {
                            local_map.insert(full_path.clone(), links.clone());
                        }
                    }
                    remote_files.insert(full_path, record);
                }
            }
        }
    }

    // --- Phase 3: 同步阶段 ---
    let (mut created, mut updated) = (0, 0);
    for (src_path, remote_rec) in &remote_files {
        if let Some(links) = local_map.get(src_path) {
            for link in links {
                if task.link_mode == LinkMode::Symlink && link.mtime != remote_rec.mtime {
                    if args.dry_run {
                        tracing::info!("[DRY RUN] 校准时间戳: {:?}", link.path);
                        updated += 1;
                    } else {
                        let ft = FileTime::from_unix_time(remote_rec.mtime, 0);
                        if let Err(e) = set_symlink_file_times(&link.path, ft, ft) {
                            tracing::warn!("无法同步时间戳 {:?}: {}", link.path, e);
                        } else {
                            updated += 1;
                        }
                    }
                }
            }
        } else {
            let target_parent = task.target_root.join(&remote_rec.rel_dir);
            let mut final_name = remote_rec.filename.clone();
            let mut target_link = target_parent.join(&final_name);

            if used_local_paths.contains(&target_link) {
                let hash = xxh3_64(src_path.as_str().as_bytes());
                final_name = format!("{}.{:08x}{}", 
                    src_path.file_stem().unwrap_or("unknown"), hash as u32,
                    src_path.extension().map(|e| format!(".{}", e)).unwrap_or_default());
                target_link = target_parent.join(&final_name);
                
                // 二次冲突检测
                if used_local_paths.contains(&target_link) {
                    tracing::error!("确定的路径冲突，跳过: {:?}", target_link);
                    continue;
                }
            }

            if args.dry_run {
                tracing::info!("[DRY RUN] 创建链接: {:?} -> {:?}", target_link, src_path);
                created += 1;
            } else {
                if let Err(e) = fs::create_dir_all(&target_parent) {
                    tracing::error!("创建父目录失败 {:?}: {}", target_parent, e);
                    continue;
                }

                let res = match task.link_mode {
                    LinkMode::Symlink => symlink(src_path, &target_link),
                    LinkMode::Hardlink => fs::hard_link(src_path, &target_link),
                };
                
                match res {
                    Ok(_) => {
                        created += 1;
                        if task.link_mode == LinkMode::Symlink {
                            let ft = FileTime::from_unix_time(remote_rec.mtime, 0);
                            if let Err(e) = set_symlink_file_times(&target_link, ft, ft) {
                                tracing::warn!("新建链接时间戳校准失败 {:?}: {}", target_link, e);
                            }
                        }
                        used_local_paths.insert(target_link);
                    }
                    Err(e) => tracing::error!("创建链接失败 {:?}: {}", target_link, e),
                }
            }
        }
    }

    // --- Phase 4: 清理阶段 ---
    let mut pruned = 0;
    let dead_links: Vec<_> = local_map.iter()
        .filter(|(src, _)| !remote_files.contains_key(*src))
        .flat_map(|(_, links)| links.iter().map(|l| l.path.clone()))
        .collect();

    for path in dead_links {
        if args.dry_run {
            tracing::info!("[DRY RUN] 移除失效项: {:?}", path);
            pruned += 1;
        } else {
            match fs::remove_file(&path) {
                Ok(_) => pruned += 1,
                Err(e) => tracing::warn!("清理失败 {:?}: {}", path, e),
            }
        }
    }

    tracing::info!("统计: 新增 {}, 更新 {}, 清理 {}", created, updated, pruned);
    Ok(())
}

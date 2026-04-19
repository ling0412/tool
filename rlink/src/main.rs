use anyhow::{bail, Context, Result};
use camino::{Utf8Path, Utf8PathBuf};
use clap::Parser;
use filetime::{set_symlink_file_times, FileTime};
use globset::{Glob, GlobSetBuilder};
use jwalk::WalkDir;
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::{fs, io};
use std::os::unix::fs::{symlink, MetadataExt};
use std::time::SystemTime;
use xxhash_rust::xxh3::xxh3_64;

#[derive(Parser, Debug)]
#[command(author, version, about = "rlink: 高可靠多源聚合同步工具")]
struct Args {
    /// 指定运行的任务名 (可选，不指定则运行所有)
    #[arg(short, long)]
    task: Option<String>,

    /// 配置文件路径
    #[arg(short, long, default_value = "rlink.toml")]
    config: Utf8PathBuf,

    /// 演练模式 (不实际执行 IO 写入)
    #[arg(short = 'n', long)] // 这里显式指定为 'n'
    dry_run: bool,

    /// 临时强制所有任务启用层级结构 (忽略配置文件中的 tree = false)
    #[arg(long, alias = "tree")]
    force_tree: bool,
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

#[derive(Debug)]
struct RemoteRecord {
    src_path: Utf8PathBuf,
    filename: String,
    rel_dir: Utf8PathBuf,
    mtime: i64,
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

/// 词法规范化路径：输入必须为绝对路径
fn clean_path(path: &Utf8Path) -> Utf8PathBuf {
    debug_assert!(path.is_absolute(), "clean_path 必须处理绝对路径: {}", path);
    let mut result = Utf8PathBuf::new();
    for component in path.components() {
        match component {
            camino::Utf8Component::CurDir => {}
            camino::Utf8Component::ParentDir => {
                result.pop();
            }
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

fn main() -> Result<()> {
    tracing_subscriber::fmt().with_max_level(tracing::Level::INFO).init();
    let args = Args::parse();

    let content = fs::read_to_string(&args.config).context("读取配置文件失败")?;
    let config: Config = toml::from_str(&content).context("解析配置失败")?;

    let tasks_to_run: Vec<_> = config.tasks.iter()
        .filter(|t| args.task.as_ref().map_or(true, |n| n == &t.name))
        .collect();

    if let Some(name) = &args.task {
        if tasks_to_run.is_empty() { bail!("未找到任务: '{}'", name); }
    }

    for task in tasks_to_run {
        if let Err(e) = run_task(task, &args) {
            tracing::error!("任务 [{}] 异常终止: {:?}", task.name, e);
        }
    }
    Ok(())
}

fn run_task(task: &TaskConfig, args: &Args) -> Result<()> {
    tracing::info!(">>> 任务开始: [{}]", task.name);

    if !args.dry_run { fs::create_dir_all(&task.target_root)?; }
    let target_root_abs = task.target_root.canonicalize_utf8()
        .with_context(|| format!("目标根目录不可访问: {}", task.target_root))?;
    let target_dev = fs::metadata(&target_root_abs)?.dev();

    let mut resolved_sources = Vec::new();
    for src in &task.sources {
        let src_abs = src.path.canonicalize_utf8().with_context(|| format!("源路径无效: {}", src.path))?;
        if target_root_abs.starts_with(&src_abs) {
            tracing::warn!("警告: 目标目录位于源 {} 内部，可能引发递归或重复扫描", src_abs);
        }
        if task.link_mode == LinkMode::Hardlink && fs::metadata(&src_abs)?.dev() != target_dev {
            bail!("硬链接不能跨设备: {} -> {}", src_abs, target_root_abs);
        }
        resolved_sources.push((src, src_abs));
    }

    // Phase 1: 扫描目标
    let mut local_map: HashMap<Utf8PathBuf, Vec<LocalLink>> = HashMap::new();
    let mut used_local_paths = HashSet::new();
    let mut inode_to_local: HashMap<(u64, u64), Vec<LocalLink>> = HashMap::new();

    for entry in WalkDir::new(&target_root_abs).skip_hidden(true) {
        let entry = entry?;
        let path = match Utf8PathBuf::from_path_buf(entry.path().to_path_buf()) {
            Ok(p) => p,
            Err(_) => continue,
        };
        if path == target_root_abs { continue; }
        
        let meta = match fs::symlink_metadata(&path) {
            Ok(m) => m,
            Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
            Err(e) => { tracing::warn!("无法获取目标元数据 {:?}: {}", path, e); continue; }
        };
        used_local_paths.insert(path.clone());

        let is_managed = match task.link_mode {
            LinkMode::Symlink => meta.file_type().is_symlink(),
            LinkMode::Hardlink => meta.is_file() && !meta.file_type().is_symlink(),
        };

        if is_managed {
            let link_data = LocalLink { path: path.clone(), mtime: get_mtime_secs(&meta) };
            if task.link_mode == LinkMode::Symlink {
                if let Ok(dest) = fs::read_link(&path) {
                    if let Ok(dest_utf8) = Utf8PathBuf::from_path_buf(dest) {
                        local_map.entry(resolve_link_target(&path, &dest_utf8)).or_default().push(link_data);
                    }
                }
            } else {
                inode_to_local.entry((meta.dev(), meta.ino())).or_default().push(link_data);
            }
        }
    }

    // Phase 2: 扫描源
    let mut remote_files = Vec::new();
    let mut seen_src = HashSet::new();

    for (src_cfg, src_abs) in resolved_sources {
        if src_cfg.patterns.is_empty() {
            tracing::warn!("源 {} 未配置 patterns，已跳过", src_abs);
            continue;
        }

        let mut gb = GlobSetBuilder::new();
        for p in &src_cfg.patterns { gb.add(Glob::new(p)?); }
        let glob_set = gb.build()?;
        let mut ib = GlobSetBuilder::new();
        for p in &src_cfg.ignore_patterns { ib.add(Glob::new(p)?); }
        let ignore_set = ib.build()?;

        for entry in WalkDir::new(&src_abs).skip_hidden(true) {
            let entry = entry?;
            if !entry.file_type().is_file() || entry.file_type().is_symlink() { continue; }

            let full_path = match Utf8PathBuf::from_path_buf(entry.path().to_path_buf()) {
                Ok(p) => p,
                Err(_) => continue,
            };

            // 注意：jwalk 默认不跟随链接且 skip_hidden=true，
            // 基于规范化的 src_abs 拼接出的 full_path 在不跟随链接的情况下是唯一的。
            if !seen_src.insert(full_path.clone()) { continue; }

            let rel_path = full_path.strip_prefix(&src_abs)?;
            if glob_set.is_match(rel_path.as_str()) && !ignore_set.is_match(rel_path.as_str()) {
                let meta = match entry.metadata() {
                    Ok(m) => m,
                    Err(e) => { tracing::warn!("源文件元数据读取失败 {:?}: {}", full_path, e); continue; }
                };
                
                let use_tree = args.force_tree || src_cfg.tree;
                let record = RemoteRecord {
                    src_path: full_path.clone(),
                    filename: full_path.file_name().unwrap_or("unknown").to_string(),
                    rel_dir: if use_tree { rel_path.parent().map(|p| p.to_path_buf()).unwrap_or_default() } else { Utf8PathBuf::new() },
                    mtime: get_mtime_secs(&meta),
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
                    if args.dry_run {
                        updated += 1;
                    } else {
                        let ft = FileTime::from_unix_time(remote.mtime, 0);
                        match set_symlink_file_times(&link.path, ft, ft) {
                            Ok(_) => updated += 1,
                            Err(e) => tracing::warn!("无法更新时间戳 {:?}: {}", link.path, e),
                        }
                    }
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
                final_name = format!("{}.{:08x}{}", 
                    remote.src_path.file_stem().unwrap_or("unknown"), 
                    hash as u32, 
                    remote.src_path.extension().map(|e| format!(".{}", e)).unwrap_or_default());
                target_path = target_parent.join(&final_name);
                if attempt > 10 { bail!("无法解决路径冲突: {:?}", target_path); }
            }

            if args.dry_run {
                tracing::info!("[DRY RUN] 建立链接: {:?} -> {:?}", target_path, remote.src_path);
                created += 1;
                used_local_paths.insert(target_path); // 仅在本次运行内占位，防止后续重复
            } else {
                if let Err(e) = fs::create_dir_all(&target_parent) {
                    tracing::warn!("父目录创建失败 {:?}: {}", target_parent, e);
                    continue;
                }
                
                let res = match task.link_mode {
                    LinkMode::Symlink => symlink(&remote.src_path, &target_path),
                    LinkMode::Hardlink => fs::hard_link(&remote.src_path, &target_path),
                };

                match res {
                    Ok(_) => {
                        if task.link_mode == LinkMode::Symlink {
                            let ft = FileTime::from_unix_time(remote.mtime, 0);
                            let _ = set_symlink_file_times(&target_path, ft, ft);
                        }
                        created += 1;
                        used_local_paths.insert(target_path);
                    }
                    Err(e) => tracing::warn!("链接创建失败 {:?}: {}", target_path, e),
                }
            }
        }
    }

    // Phase 4: 清理
    let mut pruned = 0;
    for (src_path, links) in local_map {
        if !processed_src.contains(&src_path) {
            for link in links {
                if args.dry_run {
                    pruned += 1;
                } else {
                    match fs::remove_file(&link.path) {
                        Ok(_) => pruned += 1,
                        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                        Err(e) => tracing::warn!("清理失效链接失败 {:?}: {}", link.path, e),
                    }
                }
            }
        }
    }

    let status = if args.dry_run { "预计" } else { "实际" };
    tracing::info!("任务 [{}] 统计 ({}): 新增 {}, 时间戳更新 {}, 清理 {}", task.name, status, created, updated, pruned);
    Ok(())
}

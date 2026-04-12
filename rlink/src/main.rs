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
#[command(author, version, about = "rlink: 链接同步工具")]
struct Args {
    #[arg(num_args = 1, default_value = "sync")]
    command: String,
    #[arg(short, long, default_value = "rlink.toml")]
    config: Utf8PathBuf,
    #[arg(short = 'n', long)]
    dry_run: bool,
}

#[derive(Deserialize, Debug, Clone, Copy, PartialEq)]
#[serde(rename_all = "lowercase")]
enum LinkMode { Symlink, Hardlink }

#[derive(Deserialize, Debug)]
struct Config {
    target_root: Utf8PathBuf,
    #[serde(default = "default_link_mode")]
    link_mode: LinkMode,
    sources: Vec<SourceConfig>,
}

fn default_link_mode() -> LinkMode { LinkMode::Symlink }

#[derive(Deserialize, Debug)]
struct SourceConfig {
    path: Utf8PathBuf,
    patterns: Vec<String>,
}

#[derive(Debug)]
struct RemoteRecord {
    filename: String,
    rel_dir: Utf8PathBuf,
    mtime: i64,
    _size: u64, // 预留字段，前缀下划线消除未使用告警
    inode: u64,
    dev: u64,
}

#[derive(Debug, Clone)]
struct LocalLink {
    path: Utf8PathBuf,
    mtime: i64,
}

fn get_mtime_secs(meta: &fs::Metadata) -> i64 {
    meta.modified().unwrap_or(SystemTime::UNIX_EPOCH)
        .duration_since(SystemTime::UNIX_EPOCH).unwrap_or_default().as_secs() as i64
}

fn main() -> Result<()> {
    tracing_subscriber::fmt().with_max_level(tracing::Level::INFO).with_target(false).init();
    let args = Args::parse();
    if args.command != "sync" { anyhow::bail!("不支持的命令: '{}'。", args.command); }
    if args.dry_run { tracing::info!("--- [DRY RUN MODE ACTIVE] ---"); }

    let start = Instant::now();
    run_sync(&args)?;
    tracing::info!("任务完毕，耗时: {:.2?}", start.elapsed());
    Ok(())
}

fn run_sync(args: &Args) -> Result<()> {
    let content = fs::read_to_string(&args.config).context("读取配置文件失败")?;
    let config: Config = toml::from_str(&content)?;
    
    if !args.dry_run { fs::create_dir_all(&config.target_root)?; }

    let mut glob_sets = Vec::new();
    for src in &config.sources {
        let mut builder = GlobSetBuilder::new();
        for pat in &src.patterns { builder.add(Glob::new(pat)?); }
        glob_sets.push(builder.build()?);
    }

    // --- Phase 1: 扫描本地 ---
    tracing::info!("阶段 1: 扫描本地状态 (模式: {:?})...", config.link_mode);
    let mut local_map: HashMap<Utf8PathBuf, Vec<LocalLink>> = HashMap::new();
    let mut used_local_paths = HashSet::new();
    let mut inode_to_local: HashMap<(u64, u64), Vec<LocalLink>> = HashMap::new();

    for entry in WalkDir::new(&config.target_root).skip_hidden(true) {
        let entry = entry?;
        let path = Utf8PathBuf::from(entry.path().to_string_lossy().into_owned());
        
        // 访问链接本身的元数据
        let meta = fs::symlink_metadata(&path)?;
        let is_target = match config.link_mode {
            LinkMode::Symlink => meta.file_type().is_symlink(),
            LinkMode::Hardlink => meta.is_file() && !meta.file_type().is_symlink(),
        };

        if is_target {
            used_local_paths.insert(path.clone());
            let mtime = get_mtime_secs(&meta);
            let link_data = LocalLink { path: path.clone(), mtime };

            if config.link_mode == LinkMode::Symlink {
                if let Ok(target) = fs::read_link(&path) {
                    let source_path = Utf8PathBuf::from(target.to_string_lossy().into_owned());
                    local_map.entry(source_path).or_default().push(link_data);
                }
            } else {
                inode_to_local.entry((meta.dev(), meta.ino())).or_default().push(link_data);
            }
        }
    }

    // --- Phase 2: 扫描远程 ---
    tracing::info!("阶段 2: 扫描远程存储池...");
    let mut remote_files: HashMap<Utf8PathBuf, RemoteRecord> = HashMap::new();

    for (idx, src_cfg) in config.sources.iter().enumerate() {
        let globset = &glob_sets[idx];
        let root = &src_cfg.path;

        for entry in WalkDir::new(root).skip_hidden(true) {
            let entry = entry?;
            if !entry.file_type().is_file() || entry.file_type().is_symlink() { continue; }

            let full_path = Utf8PathBuf::from(entry.path().to_string_lossy().into_owned());
            let rel_path = match full_path.strip_prefix(root) { Ok(p) => p, Err(_) => continue };

            if globset.is_match(rel_path.as_str()) {
                if let Ok(meta) = entry.metadata() {
                    let rec = RemoteRecord {
                        filename: full_path.file_name().unwrap_or("unknown").to_string(),
                        rel_dir: rel_path.parent().map(|p| p.to_path_buf()).unwrap_or_default(),
                        mtime: get_mtime_secs(&meta),
                        _size: meta.len(),
                        inode: meta.ino(),
                        dev: meta.dev(),
                    };

                    if config.link_mode == LinkMode::Hardlink {
                        // 使用设备号与 Inode 确定文件身份
                        if let Some(links) = inode_to_local.get(&(rec.dev, rec.inode)) {
                            local_map.insert(full_path.clone(), links.clone());
                        }
                    }
                    remote_files.insert(full_path, rec);
                }
            }
        }
    }

    // --- Phase 3: 同步状态 ---
    tracing::info!("阶段 3: 执行同步...");
    let mut created = 0;
    let mut updated = 0;

    for (src_path, remote_rec) in &remote_files {
        if let Some(links) = local_map.get(src_path) {
            for link in links {
                if config.link_mode == LinkMode::Symlink && link.mtime != remote_rec.mtime {
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
            let target_parent_dir = config.target_root.join(&remote_rec.rel_dir);
            let mut final_name = remote_rec.filename.clone();
            let mut target_link = target_parent_dir.join(&final_name);

            // 路径冲突防御逻辑
            if used_local_paths.contains(&target_link) {
                let hash = xxh3_64(src_path.as_str().as_bytes());
                final_name = format!("{}.{:08x}{}", 
                    src_path.file_stem().unwrap_or("unknown"), hash as u32,
                    src_path.extension().map(|e| format!(".{}", e)).unwrap_or_default());
                target_link = target_parent_dir.join(&final_name);
                
                if used_local_paths.contains(&target_link) {
                    tracing::error!("冲突路径已存在，跳过: {:?}", target_link);
                    continue;
                }
            }

            if args.dry_run {
                created += 1;
                tracing::info!("[DRY RUN] 创建链接: {:?} -> {:?}", target_link, src_path);
            } else {
                // 目录创建及拦截
                if let Err(e) = fs::create_dir_all(&target_parent_dir) {
                    tracing::error!("创建目录失败 {:?}: {}", target_parent_dir, e);
                    continue; 
                }

                let res = match config.link_mode {
                    LinkMode::Symlink => symlink(src_path, &target_link),
                    LinkMode::Hardlink => fs::hard_link(src_path, &target_link),
                };
                
                match res {
                    Ok(_) => {
                        created += 1;
                        if config.link_mode == LinkMode::Symlink {
                            let ft = FileTime::from_unix_time(remote_rec.mtime, 0);
                            if let Err(e) = set_symlink_file_times(&target_link, ft, ft) {
                                tracing::warn!("无法同步新链接的时间戳 {:?}: {}", target_link, e);
                            }
                        }
                        used_local_paths.insert(target_link);
                    }
                    Err(e) => tracing::error!("操作失败 {:?}: {}", target_link, e),
                }
            }
        }
    }

    // --- Phase 4: 清理 ---
    tracing::info!("阶段 4: 清理失效项目...");
    let mut pruned = 0;
    let dead_links: Vec<_> = local_map.iter()
        .filter(|(src, _)| !remote_files.contains_key(*src))
        .flat_map(|(_, links)| links.iter().map(|l| l.path.clone()))
        .collect();

    for path in dead_links {
        if args.dry_run {
            pruned += 1;
            tracing::info!("[DRY RUN] 移除失效链接: {:?}", path);
        } else {
            match fs::remove_file(&path) {
                Ok(_) => pruned += 1,
                Err(e) => tracing::warn!("移除失败 {:?}: {}", path, e),
            }
        }
    }

    tracing::info!("统计: 新增 {}, 更新 {}, 清理 {}", created, updated, pruned);
    Ok(())
}

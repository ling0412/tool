//! Inode and Reflink Finder (inofd)
// 查找指定文件的所有硬链接（基于 Inode 号），并可选地查找 Btrfs 共享数据块的 Reflink 副本。

use clap::Parser;
use jwalk::WalkDir;
use std::path::{Path, PathBuf};
use std::os::unix::fs::MetadataExt;
use std::io::{self, Write};
use std::time::Instant;
use std::collections::HashSet;
use std::fs::File;

// 引入 Btrfs 检查所需的库
use nix::sys::statfs;
// 需要引入 FiemapExtent 和 Fiemap
use fiemap::{FiemapExtent, Fiemap};


/// 查找指定文件的所有硬链接（基于 Inode 号），并可选地查找 Btrfs 共享数据块的 Reflink 副本。
#[derive(Parser, Debug)]
#[clap(author, version, about = "基于 Inode 查找硬链接或 Btrfs Reflink 共享文件。", long_about = None)]
struct Args {
    /// 目标文件路径
    target: PathBuf,

    /// 搜索硬链接和 Reflink 的起始路径
    search_path: PathBuf,
    
    /// 禁用 Btrfs Reflink 共享块文件搜索模式。
    #[clap(short = 'r', long)]
    disable_reflink: bool,

    /// 强制执行硬链接搜索，即使目标文件的链接数（nlink）为 1 时也执行。
    #[clap(short = 'f', long)]
    force_hardlink: bool,

    /// 排除隐藏文件和目录（以 '.' 开头）进行搜索。默认包含隐藏文件。
    #[clap(short = 'i', long)]
    skip_hidden: bool,
}

// ============== 实用函数：Btrfs 检查 ==============

/// 检查路径是否位于 Btrfs 文件系统上
fn is_on_btrfs(path: &Path) -> io::Result<bool> {
    match statfs::statfs(path) {
        Ok(stat) => Ok(stat.filesystem_type() == statfs::BTRFS_SUPER_MAGIC),
        Err(e) => Err(io::Error::new(io::ErrorKind::Other, format!("无法获取文件系统信息: {}", e))),
    }
}

// ============== 核心逻辑函数：硬链接查找 (高性能) ==============

/// 获取目标文件的 Inode 号、Device ID、链接数 nlink 和文件大小 size
fn get_target_inode_info(path: &Path) -> io::Result<(u64, u64, u64, u64)> {
    let metadata = path.metadata()?;
    
    Ok((metadata.ino(), metadata.dev(), metadata.nlink(), metadata.len())) 
}

/// 遍历搜索路径，查找匹配 Inode 号的文件（JWalk 并行，设备 ID 剪枝）
fn find_hard_links(search_path: &Path, target_inode: u64, target_dev: u64, skip_hidden: bool) -> io::Result<HashSet<PathBuf>> {
    
    let walker = WalkDir::new(search_path)
        .sort(false)
        .skip_hidden(skip_hidden)
        .follow_links(false)
        .max_depth(std::usize::MAX)
        .process_read_dir(move |_depth, _path, _read_dir_state, children| {
            // 文件系统剪枝：只保留 Device ID 匹配的条目
            children.retain(|entry_result| {
                if let Ok(entry) = entry_result {
                    if let Ok(metadata) = entry.metadata() {
                        metadata.dev() == target_dev
                    } else {
                        false
                    }
                } else {
                    false
                }
            });
        });

    // JWalk 迭代器在后台并行读取目录
    let links: HashSet<PathBuf> = walker
        .into_iter()
        .filter_map(|entry_result| {
            let entry = match entry_result {
                Ok(e) => e,
                Err(e) => {
                    // 记录 JWalk 遍历中遇到的错误（例如权限错误），然后跳过该条目
                    eprintln!("[JWalk 警告] 遍历错误：{}", e);
                    return None
                },
            };
            
            let path = entry.path();
            
            let metadata = match entry.metadata() {
                Ok(m) => m,
                Err(_) => return None,
            };

            // 核心检查：Inode 和 Device ID 都匹配，且非目录
            if !metadata.is_dir() && metadata.ino() == target_inode && metadata.dev() == target_dev {
                Some(path)
            } else {
                None
            }
        })
        .collect();
    
    Ok(links)
}

// FIEMAP_EXTENT_FLAG_INLINE constant (value from Linux kernel headers: 0x00000004)
const FIEMAP_EXTENT_FLAG_INLINE: u32 = 0x00000004;

// ============== 核心逻辑函数：Reflink 共享查找 (fiemap) ==============

/// 提取文件的 Fiemap Extent 列表，并过滤掉 inline data。
fn get_extents(path: &Path) -> io::Result<Vec<FiemapExtent>> {
    let file = File::open(path)?;
    
    // 过滤掉 inline data Extent。
    let physical_extents = Fiemap::new(file)
        .collect::<Result<Vec<FiemapExtent>, io::Error>>()?
        .into_iter()
        .filter(|e| e.fe_flags.bits() & FIEMAP_EXTENT_FLAG_INLINE == 0) 
        .collect();
    
    Ok(physical_extents)
}

/// 比较两个文件的 Extent 列表是否完全相同 (100% 共享)
fn same_extents(extents1: &[FiemapExtent], extents2: &[FiemapExtent]) -> bool {
    extents1.iter().zip(extents2.iter()).all(|(extent1, extent2)| {
        (extent1.fe_physical == extent2.fe_physical) && (extent1.fe_length == extent2.fe_length)
    })
}

/// 遍历搜索路径，查找与目标文件 Extent 列表完全相同的 Reflink 副本
fn find_reflinked_files_by_extents(
    search_path: &Path, 
    target_extents: Vec<FiemapExtent>, 
    target_dev: u64, 
    target_inode: u64, 
    target_size: u64, 
    skip_hidden: bool
) -> io::Result<HashSet<PathBuf>> {
    
    let target_extent_count = target_extents.len();
    
    let walker = WalkDir::new(search_path)
        .sort(false)
        .skip_hidden(skip_hidden) 
        .follow_links(false)
        .max_depth(std::usize::MAX)
        .process_read_dir(move |_depth, _path, _read_dir_state, children| {
            // 文件系统剪枝：只保留 Device ID 匹配的条目
            children.retain(|entry_result| {
                if let Ok(entry) = entry_result {
                    if let Ok(metadata) = entry.metadata() {
                        metadata.dev() == target_dev
                    } else {
                        false
                    }
                } else {
                    false
                }
            });
        });

    let reflinked_files: HashSet<PathBuf> = walker
        .into_iter()
        .filter_map(|entry_result| {
            let entry = match entry_result {
                Ok(e) => e,
                Err(e) => {
                    // 记录 JWalk 遍历中遇到的错误
                    eprintln!("[JWalk 警告] 遍历错误：{}", e);
                    return None
                },
            };
            
            let path = entry.path().to_path_buf();
            
            let metadata = match entry.metadata() {
                Ok(m) => m,
                Err(_) => return None,
            };
            
            // 预剪枝：排除目录、目标文件本身、和大小不一致的文件
            if metadata.is_dir() || metadata.ino() == target_inode || metadata.len() != target_size {
                return None;
            }
            
            // 核心逻辑：获取当前文件的 Extent，并与目标 Extent 比较
            match get_extents(&path) {
                Ok(extents) => {
                    // 性能优化：快速失败机制 - Extent 数量不一致则跳过深度比较
                    if extents.len() != target_extent_count {
                        return None;
                    }
                                            
                    if same_extents(&extents, &target_extents) {
                        // Extent 列表完全一致，表示完全共享数据块 (Reflinked)
                        Some(path)
                    } else {
                        None
                    }
                }
                Err(e) => {
                    // 忽略常见的瞬时/不支持错误，以清理输出
                    if e.kind() == io::ErrorKind::NotFound || e.kind() == io::ErrorKind::PermissionDenied || 
                       e.raw_os_error() == Some(6) || e.raw_os_error() == Some(95) || e.raw_os_error() == Some(25) {
                        // 忽略：(6: ENXIO, 95: EOPNOTSUPP, 25: ENOTTY) 
                    } else {
                        eprintln!("[警告] 无法获取文件 {} 的 Extent: {}", path.display(), e);
                    }
                    None
                }
            }
        })
        .collect();
    
    Ok(reflinked_files)
}


// ============== 主函数 ==============

fn main() -> io::Result<()> {
    
    let args = Args::parse();
    let start_time = Instant::now();
    
    let mut total_results: HashSet<PathBuf> = HashSet::new();
    
    // --- 1. 获取目标文件信息 ---
    let (target_inode, target_dev, target_nlink, target_size) = match get_target_inode_info(&args.target) {
        Ok((i, d, n, s)) => (i, d, n, s),
        Err(e) => {
            eprintln!("[错误] 无法获取目标文件 {} 的信息：{}", args.target.display(), e);
            return Err(e);
        }
    };
    
    // 检查 Btrfs 状态
    let is_btrfs = match is_on_btrfs(&args.target) {
        Ok(b) => b,
        Err(e) => {
            eprintln!("[错误] 无法检查文件系统类型：{}", e);
            return Err(e);
        }
    };
    
    // 确定 Reflink 搜索是否执行
    let perform_reflink_search = is_btrfs && !args.disable_reflink;
    
    println!("目标文件: {} [Inode: {}, Dev: {}, Size: {} bytes]", 
        args.target.display(), target_inode, target_dev, target_size);

    
    // --- 2. Inode 硬链接查找 ---
    let perform_hardlink_search = target_nlink > 1 || args.force_hardlink;

    if perform_hardlink_search {
        print!("  -> 硬链接查找: 搜索 {}... ", args.search_path.display());
        io::stdout().flush().unwrap();
        
        let mut hard_links_set = find_hard_links(&args.search_path, target_inode, target_dev, args.skip_hidden)?;
        
        // 优化: 移除自身路径，避免昂贵的 canonicalize
        hard_links_set.remove(&args.target); 
        total_results.extend(hard_links_set);
        
        if target_nlink <= 1 {
            eprintln!("\n[提示] 目标文件 nlink=1，因 -f 强制执行查找。");
        }
    } else {
        println!("  -> 硬链接查找: nlink=1，跳过。使用 -f 强制执行。");
    }

    
    // --- 3. Btrfs 共享查找 ---
    let mut btrfs_shared_count = 0;
    if perform_reflink_search {
        
        print!("  -> Reflink 查找: 目标在 Btrfs 上。提取 Extent... ");
        io::stdout().flush().unwrap();
        
        let target_extents = match get_extents(&args.target) {
            Ok(extents) => extents,
            Err(e) => {
                eprintln!("\n[Reflink 查找失败] 无法获取目标文件 Extent: {}", e);
                return Err(e);
            }
        };
        
        // 优化点 1: Inline Data 警告处理
        if target_extents.is_empty() && target_size > 0 {
            println!("跳过。");
            eprintln!("[警告] 目标文件 ({}) 是 Inline Data。Reflink 检查不可靠，已跳过。", args.target.display());
        } else {
            print!("完成 (Extents: {})。开始并行比较... ", target_extents.len());
            io::stdout().flush().unwrap();

            match find_reflinked_files_by_extents(&args.search_path, target_extents, target_dev, target_inode, target_size, args.skip_hidden) {
                Ok(btrfs_results) => {
                    btrfs_shared_count = btrfs_results.len(); 
                    total_results.extend(btrfs_results);
                }
                Err(e) => {
                    eprintln!("\n[Reflink 查找失败] {}", e);
                }
            }
        }
    } else {
        if is_btrfs {
             println!("  -> Reflink 查找: 目标在 Btrfs 上，已通过 -r 显式禁用。");
        } else {
            println!("  -> Reflink 查找: 目标不在 Btrfs 上，已自动禁用。");
        }
    }
    
    // --- 4. 输出结果 ---
    let final_count = total_results.len();
    let elapsed = start_time.elapsed();
    
    // 硬链接计数来自 total_results 中 Inode 匹配的项
    let hard_links_found = total_results.iter()
        .filter_map(|p| p.metadata().ok())
        .filter(|m| m.ino() == target_inode)
        .count();
    
    println!("\n--- 查找结果摘要 (耗时: {:.2?}) ---", elapsed);
    // Reflink 计数来自专用搜索结果，它已经排除了硬链接
    println!("总共找到 {} 个副本 (硬链接: {}, Reflink: {})", 
        final_count, 
        hard_links_found, 
        btrfs_shared_count);
    
    if final_count > 0 {
        println!("--- 详细列表 ---");
        // 将结果转换为 Vec 并排序，以便输出顺序稳定
        let mut sorted_results: Vec<PathBuf> = total_results.into_iter().collect();
        sorted_results.sort();
        
        for link in sorted_results.iter() {
            let metadata = match link.metadata() {
                Ok(m) => m,
                Err(_) => {
                    println!("[已消失] {}", link.display());
                    continue;
                }
            };

            // 确定状态：HARDLINK (Inode 匹配) 或 REFLINK (非 Inode 匹配，但被 Reflink 搜索找到)
            let status = if metadata.ino() == target_inode {
                "HARDLINK"
            } else if perform_reflink_search {
                // 如果启用了 Reflink 搜索，且此文件不是硬链接，则它是 Reflink 结果集中的一个
                "REFLINK"
            } else {
                "?" 
            };
            
            println!("[{}] {}", status, link.display());
        }
    } else {
        println!("\n[提示] 未找到其他链接或共享文件。");
    }

    Ok(())
}

use clap::Parser;
use jwalk::WalkDir; 
use std::path::{Path, PathBuf};
use std::os::unix::fs::MetadataExt;
use std::io::{self, Write};
use std::time::Instant;
use num_cpus;
use rayon;

/// 查找指定文件的所有硬链接（基于 Inode 号）。
#[derive(Parser, Debug)]
#[clap(author, version, about = "基于 Inode 查找硬链接。", long_about = None)]
struct Args {
    /// 目标文件名
    filename: PathBuf,

    /// 搜索硬链接的起始路径
    search_path: PathBuf,
}

// ============== 核心逻辑函数 ==============

/// 获取目标文件的 Inode 号和 Device ID
fn get_target_inode_info(path: &Path) -> io::Result<(u64, u64)> {
    let metadata = path.metadata()?;
    
    let inode = metadata.ino();
    let dev = metadata.dev();
    
    let nlink = metadata.nlink();
    if nlink <= 1 {
        eprintln!("[警告] 目标文件 {} 的链接数（nlink）为 {}，可能不存在其他硬链接。",
                  path.display(), nlink);
    }
    
    Ok((inode, dev))
}

/// 遍历搜索路径，查找匹配 Inode 号的文件（JWalk 并行）
fn find_hard_links(search_path: &Path, target_inode: u64, target_dev: u64) -> io::Result<Vec<PathBuf>> {
    
    let walker = WalkDir::new(search_path)
        .sort(false) 
        .skip_hidden(false) 
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
    let links: Vec<PathBuf> = walker
        .into_iter()
        .filter_map(|entry_result| {
            let entry = match entry_result {
                Ok(e) => e,
                Err(_) => return None,
            };
            
            let path = entry.path();

            let metadata = match entry.metadata() {
                Ok(m) => m,
                Err(_) => return None, 
            };

            if metadata.is_dir() {
                return None;
            }

            // 核心检查：Inode 和 Device ID 都匹配
            if metadata.ino() == target_inode && metadata.dev() == target_dev {
                Some(path)
            } else {
                None
            }
        })
        .collect();
    
    Ok(links)
}

// ============== 主函数 ==============

fn main() -> io::Result<()> {
    // 初始化 Rayon 线程池
    let num_threads = num_cpus::get();
    let _ = rayon::ThreadPoolBuilder::new().num_threads(num_threads).build_global();

    let args = Args::parse();
    
    // --- 第一步：获取目标文件的 Inode 信息 ---
    let (target_inode, target_dev) = match get_target_inode_info(&args.filename) {
        Ok(info) => {
            println!("目标文件: {} (Inode: {}, Device: {})", args.filename.display(), info.0, info.1);
            info
        },
        Err(e) => {
            eprintln!("[错误] 无法获取目标文件 {} 的信息：{}", args.filename.display(), e);
            return Err(e);
        }
    };
    
    // --- 第二步：查找硬链接并计时 ---
    print!("开始搜索: 在 {} 中查找 Inode {} (并行线程数: {})... ", 
           args.search_path.display(), target_inode, num_threads);
    io::stdout().flush().unwrap();
    
    let start_time = Instant::now();
    let hard_links = find_hard_links(&args.search_path, target_inode, target_dev)?;
    let elapsed = start_time.elapsed();
    
    // --- 第三步：输出结果 ---
    println!("完成 (耗时: {:.2?})", elapsed);
    println!("\n--- 找到 {} 个硬链接 ---", hard_links.len());
    
    for (i, link) in hard_links.iter().enumerate() {
        println!("{}. {}", i + 1, link.display());
    }
    
    if hard_links.len() <= 1 {
        println!("\n[提示] 在指定的搜索路径中，未找到其他硬链接。");
    }

    Ok(())
}


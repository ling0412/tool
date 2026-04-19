use clap::Parser;
use std::fs::{File, OpenOptions};
use std::os::unix::io::AsRawFd;
use std::{mem, slice, collections::BTreeMap};
use nix::errno::Errno;

#[derive(Parser, Debug)]
#[command(author, version, about = "Btrfs 物理布局分析工具 - 兼容压缩/Zoned/CMR")]
struct Args {
    #[arg(short, long)]
    file: String,
    
    #[arg(short, long)]
    device: Option<String>,
    
    /// 展开所有分片细节
    #[arg(long)]
    full: bool,

    /// 断层点及首尾前后各显示几行
    #[arg(long, default_value_t = 2)]
    context: usize,
}

// 遵循内核 Linux/fiemap.h 的标准定义
const FS_IOC_FIEMAP: u64 = 0xC020_660B;
const BLKGETZONESZ: u64 = 0x8004_1284;
const FIEMAP_FLAG_SYNC: u32 = 0x01;

const FIEMAP_EXTENT_LAST:      u32 = 0x0000_0001; // 最后一个 extent，用于终止循环
const FIEMAP_EXTENT_UNKNOWN:   u32 = 0x0000_0002; // 物理地址未知
const FIEMAP_EXTENT_DELALLOC:  u32 = 0x0000_0004; // 延迟分配（仍在内存中）
const FIEMAP_EXTENT_ENCODED:   u32 = 0x0000_0008; // 压缩/加密（物理地址无效）
const FIEMAP_EXTENT_UNWRITTEN: u32 = 0x0000_0800; // 预分配但未写入

#[repr(C)]
pub struct Fiemap {
    pub fm_start: u64, pub fm_length: u64, pub fm_flags: u32,
    pub fm_mapped_extents: u32, pub fm_extent_count: u32, pub fm_reserved: u32,
}

#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct FiemapExtent {
    pub fe_logical: u64, pub fe_physical: u64, pub fe_length: u64,
    pub fe_reserved64: [u64; 2], pub fe_flags: u32, pub fe_reserved32: [u32; 3],
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let (is_zoned, slice_size) = detect_hardware_caps(&args.device);

    let extents = get_all_extents(&args.file)?;
    if extents.is_empty() { return Ok(()); }
    let total_bytes: u64 = extents.iter().map(|ex| ex.fe_length).sum();

    let distribution = calculate_distribution(&extents, slice_size);

    print_balanced_heatmap(&distribution, slice_size, is_zoned, args.full, args.context);
    
    println!("\n[ {} ] 物理指标摘要", args.file);
    println!("--------------------------------------------------");
    println!("文件规格:    {:<12} | 分片总数: {}", format_size(total_bytes), extents.len());
    println!("平均分片大小: {:<12}", format_size(total_bytes / extents.len() as u64));

    if is_zoned {
        print_smr_stats(&extents, &distribution, total_bytes, slice_size);
    } else {
        print_cmr_stats(&extents, total_bytes);
    }

    Ok(())
}

// 物理性判定逻辑：排除非物理位置的分片
fn is_physical_extent(flags: u32) -> bool {
    flags & (FIEMAP_EXTENT_UNKNOWN 
           | FIEMAP_EXTENT_DELALLOC 
           | FIEMAP_EXTENT_ENCODED 
           | FIEMAP_EXTENT_UNWRITTEN) == 0
}

fn detect_hardware_caps(device: &Option<String>) -> (bool, u64) {
    if let Some(dev_path) = device {
        if let Ok(file) = OpenOptions::new().read(true).open(dev_path) {
            let mut zone_sectors: u32 = 0;
            let ret = unsafe { libc::ioctl(file.as_raw_fd(), BLKGETZONESZ as _, &mut zone_sectors) };
            if ret == 0 && zone_sectors > 0 {
                return (true, zone_sectors as u64 * 512);
            }
        }
    }
    (false, 1024 * 1024 * 1024) // CMR 默认按 1GB 步进显示
}

fn calculate_distribution(extents: &[FiemapExtent], slice_size: u64) -> BTreeMap<u64, u64> {
    let mut dist = BTreeMap::new();
    for ex in extents {
        if !is_physical_extent(ex.fe_flags) { continue; }
        let mut start = ex.fe_physical;
        let mut len = ex.fe_length;
        while len > 0 {
            let id = start / slice_size;
            let offset = start % slice_size;
            let can_take = (slice_size - offset).min(len);
            *dist.entry(id).or_insert(0) += can_take;
            start += can_take;
            len -= can_take;
        }
    }
    dist
}

fn print_balanced_heatmap(
    dist: &BTreeMap<u64, u64>,
    slice_size: u64,
    is_zoned: bool,
    full: bool,
    context: usize,
) {
    let ids: Vec<u64> = dist.keys().cloned().collect();
    let total = ids.len();
    if total == 0 { return; }

    // 预计算显示掩码
    let mut show = vec![false; total];
    
    // 首尾始终显示
    for i in 0..context.min(total) { show[i] = true; }
    for i in total.saturating_sub(context)..total { show[i] = true; }

    // 标记断层点（不连续点）及其上下文
    for i in 0..total {
        let gap_before = i > 0 && ids[i] != ids[i - 1] + 1;
        let gap_after  = i + 1 < total && ids[i + 1] != ids[i] + 1;
        
        if gap_before || gap_after {
            let lo = i.saturating_sub(context);
            let hi = (i + context + 1).min(total);
            for j in lo..hi { if j < total { show[j] = true; } }
        }
    }

    let label = if is_zoned { "Zone ID" } else { "LBA Range" };
    println!("\n\x1b[1;32m[ 物理分布图 - {} ]\x1b[0m", if is_zoned { "Zoned SMR" } else { "CMR/LBA" });
    println!("{:<12} {:<24} {}", label, "占用量", "填充率");

    let mut i = 0;
    while i < total {
        if full || show[i] {
            print_zone_row(&ids, dist, i, slice_size, is_zoned);
            i += 1;
        } else {
            let fold_start = i;
            while i < total && !show[i] && !full { i += 1; }
            let fold_count = i - fold_start;
            let first_id = ids[fold_start];
            let last_id  = ids[i - 1];

            if is_zoned {
                println!("  ...... [ {} 个连续 Zone ({:06}~{:06}) 折叠 ] ......", fold_count, first_id, last_id);
            } else {
                println!("  ...... [ {} 个连续区间 ({}GB~{}GB) 折叠 ] ......", fold_count, first_id, last_id);
            }
        }
    }
}

fn print_zone_row(ids: &[u64], dist: &BTreeMap<u64, u64>, i: usize, slice_size: u64, is_zoned: bool) {
    let id = ids[i];
    let used = dist[&id];
    let ratio = (used as f64 / slice_size as f64 * 100.0).min(100.0);
    
    let filled = ((ratio / 5.0) as usize).min(20);
    let bar = "█".repeat(filled) + &"░".repeat(20 - filled);
    
    let id_str = if is_zoned { format!("{:06}", id) } else { format!("{:>4} GB", id) };
    println!("{:<12} [{}] {:>5.1}%  ({})", id_str, bar, ratio, format_size(used));
}

fn print_smr_stats(extents: &[FiemapExtent], dist: &BTreeMap<u64, u64>, total_bytes: u64, zone_size: u64) {
    let ids: Vec<u64> = dist.keys().cloned().collect();
    
    // 计算 Runs（物理连续段数）
    let runs = if ids.is_empty() { 0u64 } else {
        ids.windows(2).filter(|w| w[1] != w[0] + 1).count() as u64 + 1
    };

    let zone_count = ids.len() as u64;
    let frag_ratio = if zone_count > 1 { (runs - 1) as f64 / (zone_count - 1) as f64 } else { 0.0 };
    let ideal_zones = (total_bytes + zone_size - 1) / zone_size;
    let encoded_count = extents.iter().filter(|e| !is_physical_extent(e.fe_flags)).count();

    println!("\n\x1b[1;33m[ Zoned 物理参数评估 ]\x1b[0m");
    println!("--------------------------------------------------");
    println!("物理区间段 (Runs):  {}", runs);
    println!("占用 Zone 总数:     {}", zone_count);
    println!("非物理(压缩/内联):  {}", encoded_count);
    println!("理论最优 Zone 数:   {}", ideal_zones);
    println!("物理空间浪费:       {} Zone", zone_count.saturating_sub(ideal_zones.min(zone_count)));
    println!("物理碎片率 (Frag):  {:.2}%", frag_ratio * 100.0);
    println!("--------------------------------------------------");
}

fn print_cmr_stats(extents: &[FiemapExtent], total_bytes: u64) {
    let physical: Vec<&FiemapExtent> = extents.iter()
        .filter(|e| is_physical_extent(e.fe_flags))
        .collect();

    if physical.is_empty() {
        println!("\n\x1b[1;35m[ CMR/LBA 寻道参数评估 ]\x1b[0m");
        println!("警告: 所有分片均为编码/压缩类型，无物理地址。");
        return;
    }

    let first_p = physical.first().unwrap().fe_physical;
    let last_p = physical.last().unwrap().fe_physical + physical.last().unwrap().fe_length;
    let span = last_p.saturating_sub(first_p);
    let overhead = span as f64 / total_bytes as f64;
    let encoded_count = extents.len() - physical.len();

    println!("\n\x1b[1;35m[ CMR/LBA 寻道参数评估 ]\x1b[0m");
    println!("--------------------------------------------------");
    println!("物理有效分片:       {} / {} (非物理: {})", physical.len(), extents.len(), encoded_count);
    println!("物理跨距 (Span):    {}", format_size(span));
    println!("跨距比 (Overhead):  {:.3}x", overhead);
    println!("--------------------------------------------------");
}

fn get_all_extents(path: &str) -> Result<Vec<FiemapExtent>, Box<dyn std::error::Error>> {
    let file = File::open(path)?;
    let fd = file.as_raw_fd();
    let mut all_extents = Vec::new();
    let mut fm_start = 0u64;
    let batch = 256;

    loop {
        let buf_size = mem::size_of::<Fiemap>() + batch * mem::size_of::<FiemapExtent>();
        let mut buffer = vec![0u8; buf_size];
        let fiemap = unsafe { &mut *(buffer.as_mut_ptr() as *mut Fiemap) };
        fiemap.fm_start = fm_start;
        fiemap.fm_length = u64::MAX - fm_start;
        fiemap.fm_extent_count = batch as u32;
        fiemap.fm_flags = FIEMAP_FLAG_SYNC;

        let ret = unsafe { libc::ioctl(fd, FS_IOC_FIEMAP as _, fiemap as *mut _) };
        if ret < 0 { return Err(Errno::last().into()); }

        let got = fiemap.fm_mapped_extents as usize;
        if got == 0 { break; }

        let ptr = unsafe { buffer.as_ptr().add(mem::size_of::<Fiemap>()) as *const FiemapExtent };
        all_extents.extend_from_slice(unsafe { slice::from_raw_parts(ptr, got) });
        
        let last = &all_extents[all_extents.len() - 1];
        // 正确使用 LAST 标志位作为终止条件
        if (last.fe_flags & FIEMAP_EXTENT_LAST) != 0 { break; }
        fm_start = last.fe_logical + last.fe_length;
    }
    Ok(all_extents)
}

fn format_size(bytes: u64) -> String {
    if bytes < 1024 { return format!("{} B", bytes); }
    let kib = bytes as f64 / 1024.0;
    if kib < 1024.0 { return format!("{:.2} KB", kib); }
    let mib = kib / 1024.0;
    if mib < 1024.0 { return format!("{:.2} MB", mib); }
    let gib = mib / 1024.0;
    format!("{:.2} GB", gib)
}

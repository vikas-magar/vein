use error::Result;
#[cfg(target_os = "linux")]
use error::VeinError;
#[cfg(target_os = "linux")]
use std::fs;
use tracing::info;

/// Network interface request flags.
#[repr(C)]
pub struct IfreqFlags {
    /// Interface name.
    pub ifr_name: [u8; 16],
    /// Flags.
    pub ifr_flags: i16,
}

/// Defines the lifecycle and initialization of a Vein Engine node.
pub trait SystemNode {
    /// Returns the logical name of this node.
    fn node_name(&self) -> &str;

    /// Sets up system-level dependencies (pseudo-filesystems, network interfaces).
    #[cfg(target_os = "linux")]
    fn setup_system(&self) -> Result<()> {
        let _ = fs::create_dir_all("/sys/fs/bpf");
        let _ = fs::create_dir_all("/proc");
        let _ = fs::create_dir_all("/dev");

        unsafe {
            let none = c"none".as_ptr();
            libc::mount(
                none,
                c"/sys".as_ptr(),
                c"sysfs".as_ptr(),
                0,
                std::ptr::null(),
            );
            libc::mount(
                none,
                c"/sys/fs/bpf".as_ptr(),
                c"bpf".as_ptr(),
                0,
                std::ptr::null(),
            );
            libc::mount(
                none,
                c"/proc".as_ptr(),
                c"proc".as_ptr(),
                0,
                std::ptr::null(),
            );
            libc::mount(
                none,
                c"/dev".as_ptr(),
                c"devtmpfs".as_ptr(),
                0,
                std::ptr::null(),
            );

            // Allow time for devtmpfs population if needed
            for _ in 0..100 {
                let fd = libc::open(c"/dev/console".as_ptr(), libc::O_RDWR);
                if fd >= 0 {
                    libc::dup2(fd, 0);
                    libc::dup2(fd, 1);
                    libc::dup2(fd, 2);
                    break;
                }
                let ts = libc::timespec {
                    tv_sec: 0,
                    tv_nsec: 10_000_000,
                };
                libc::nanosleep(&ts, std::ptr::null_mut());
            }

            let ts = libc::timespec {
                tv_sec: 1_800_000_000,
                tv_nsec: 0,
            };
            libc::clock_settime(libc::CLOCK_REALTIME, &ts);
        }

        info!(
            "[{}] Vein-OS Node booting...",
            self.node_name().to_uppercase()
        );

        unsafe {
            libc::system(c"ip link set lo up".as_ptr());
            libc::system(c"ip link set eth0 up".as_ptr());
            let sock = libc::socket(libc::AF_INET, libc::SOCK_DGRAM, 0);
            if sock >= 0 {
                let mut r: IfreqFlags = std::mem::zeroed();
                r.ifr_name[0..4].copy_from_slice(b"eth0");
                libc::ioctl(sock, 0x8913, &mut r);
                r.ifr_flags |= (libc::IFF_UP | libc::IFF_RUNNING) as i16;
                libc::ioctl(sock, 0x8914, &mut r);
                libc::close(sock);
            } else {
                return Err(VeinError::internal(
                    "Failed to open socket for AF_INET configuration",
                ));
            }
        }
        Ok(())
    }

    /// Sets up system-level dependencies for non-Linux targets (no-op).
    #[cfg(not(target_os = "linux"))]
    fn setup_system(&self) -> Result<()> {
        info!(" [DEV MODE] Skipping Linux init for {}", self.node_name());
        Ok(())
    }
}

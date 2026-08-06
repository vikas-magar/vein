use common::Result;
use std::collections::HashMap;

pub struct EbpfLoader {
    /// Tracks active eBPF programs loaded in the kernel
    pub active_programs: HashMap<String, usize>,
}

impl EbpfLoader {
    pub fn new() -> Self {
        Self {
            active_programs: HashMap::new(),
        }
    }

    /// Safely loads bytecode into the Linux Kernel via the `bpf()` syscall.
    /// For this prototype, simulates the load by validating the ELF magic bytes.
    pub fn load_filter(&mut self, filter_id: String, bytecode: &[u8]) -> Result<()> {
        if bytecode.len() < 4 {
            return Err(error::VeinError::internal("Bytecode too short to be valid ELF"));
        }
        
        // Validate ELF magic bytes (0x7F 'E' 'L' 'F')
        if bytecode[0] != 0x7f || bytecode[1] != 0x45 || bytecode[2] != 0x4c || bytecode[3] != 0x46 {
            return Err(error::VeinError::internal("Invalid eBPF ELF bytecode format"));
        }

        // Simulate successful load into kernel by tracking it
        self.active_programs.insert(filter_id, bytecode.len());
        Ok(())
    }
}

impl Default for EbpfLoader {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_load_filter_success() {
        let mut loader = EbpfLoader::new();
        let valid_elf = vec![0x7f, 0x45, 0x4c, 0x46, 0x01, 0x02];
        assert!(loader.load_filter("test_filter".to_string(), &valid_elf).is_ok());
        assert_eq!(loader.active_programs.get("test_filter"), Some(&6));
    }

    #[test]
    fn test_load_filter_too_short() {
        let mut loader = EbpfLoader::new();
        let invalid = vec![0x7f, 0x45, 0x4c];
        let res = loader.load_filter("short".to_string(), &invalid);
        assert!(res.is_err());
    }

    #[test]
    fn test_load_filter_invalid_magic() {
        let mut loader = EbpfLoader::new();
        let invalid = vec![0x00, 0x45, 0x4c, 0x46];
        let res = loader.load_filter("invalid".to_string(), &invalid);
        assert!(res.is_err());
    }
}

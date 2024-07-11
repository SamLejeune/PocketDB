use super::constants::params::ELEMENT_SIZE;

pub fn bytes_to_u32(bytes: &[u8]) -> u32 {
    let mut result = 0u32;
    for (i, &byte) in bytes.iter().enumerate() {
        result |= (byte as u32) << (8 * i);
    }
    result
}

pub fn pad_bytes(bytes: &mut Vec<u8>) {
    let padding: usize = ELEMENT_SIZE - (bytes.len() % ELEMENT_SIZE);

    if padding < ELEMENT_SIZE {
        bytes.extend(vec![0u8; padding]);
    }
}
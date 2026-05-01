use fd_wrapper::{ReadFdWrapper, WriteFdWrapper};
use std::io::{Read, Write};
use std::os::fd::RawFd;

const DISK_INPUT_FD: RawFd = 3;
const DISK_OUTPUT_FD: RawFd = 4;
const MONITOR_INPUT_FD: RawFd = 5;
const MONITOR_OUTPUT_FD: RawFd = 6;

pub fn setup_disk_io() -> (impl Read, impl Write) {
    (
        ReadFdWrapper::new(DISK_INPUT_FD),
        WriteFdWrapper::new(DISK_OUTPUT_FD),
    )
}

pub fn setup_monitor_io() -> (impl Read, impl Write) {
    (
        ReadFdWrapper::new(MONITOR_INPUT_FD),
        WriteFdWrapper::new(MONITOR_OUTPUT_FD),
    )
}

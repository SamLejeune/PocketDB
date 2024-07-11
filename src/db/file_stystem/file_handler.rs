use std::{fs::{File, OpenOptions}, io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write}};

#[derive(Debug)]
pub struct FileHandler {
    buf_reader: BufReader<File>,
    buf_writer: BufWriter<File>,
}

impl FileHandler {
    pub fn new() -> FileHandler {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open("db.db")
            .expect("Failed to open file");
        let buf_reader = BufReader::new(file.try_clone().expect("Could not clone"));
        let buf_writer = BufWriter::new(file);

        FileHandler {
            buf_reader,
            buf_writer,
        }
    }

    pub fn write(&mut self, buf: &Vec<u8>) -> Result<(), io::Error> {
        self.buf_writer.write(buf)?;
        self.buf_writer.flush()?;

        Ok(())
    }

    pub fn read(&mut self, buf: &mut Vec<u8>) -> Result<usize, io::Error> {
        let bytes_read = self.buf_reader.read(buf)?;

        Ok(bytes_read)
    }

    pub fn seek_overwrite(&mut self, offset: usize) -> Result<(), io::Error> {
        self.buf_writer.seek(SeekFrom::Start(offset as u64))?;

        Ok(())
    }

    pub fn seek_write(&mut self) -> Result<u32, io::Error> {
        let bytes_seek = self.buf_writer.seek(SeekFrom::End(0))?;

        Ok(bytes_seek as u32)
    }

    pub fn seek_reader(&mut self, offset: usize) -> Result<(), io::Error> {
        self.buf_reader.seek(SeekFrom::Start(offset as u64))?;

        Ok(())
    }

    pub fn seek_end(&mut self) -> Result<(), io::Error> {
        self.buf_reader.seek(SeekFrom::End(0))?;

        Ok(())
    }
}